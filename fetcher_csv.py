import os
import time
import pandas as pd
import numpy as np
import requests
from io import StringIO
from helper import plot_days, time_batches
from helper_db import get_db_params
from fetcher_xml import get_active_sites
from config_db import config_db
from os import listdir
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import sqlalchemy

DIR = 'solectria_raw_csv'
URL = "https://solrenview.com/cgi-bin/cgihandler.cgi"

wait_time = 0.5  # seconds
last_fetch = 0

unit_view = {'day': '0', 'week': '1', 'month': '2', '0': 'day', '1': 'week', '2': 'month'}

# returns datetime rounded to the chosen unit
# 'day' returns datetime without hours, minutes and seconds
# 'week' returns previous monday; if dt is monday, returns itself
# 'month' returns same month with 1 day
round_dt = {'day': lambda dt: datetime(dt.year, dt.month, dt.day),
            'week': lambda dt: round_dt['day'](dt) - timedelta(days=dt.weekday()),
            'month': lambda dt: datetime(dt.year, dt.month, 1)}

# return number of time units (days/weeks/months) between start and end (inclusive)
# end - end datetime at the timezone
get_units_ago = {'day': lambda start, end: (round_dt['day'](end) - round_dt['day'](start)).days,
                 'week': lambda start, end: (round_dt['week'](end) - round_dt['week'](start)).days // 7,
                 'month': lambda start, end: (end.year - start.year) * 12 + end.month - start.month}

# returns date n units before target
# end - end datetime at the timezone
get_date_ago = {'day': lambda target, n: round_dt['day'](target) - timedelta(days=n),
                'week': lambda target, n: round_dt['week'](target) - relativedelta(weeks=n),
                'month': lambda target, n: round_dt['month'](target) - relativedelta(months=n)}

sites_data = pd.read_csv('active_sites_data.csv')
sites_data.set_index('site_id', inplace=True)


# inserts dataframe (df) into the database (from params) into given table
# defaults - default values for columns that are in the database but not in dataframe
# e.g.: {'site_id': 4760} to will add a column 'site_id' and puts 4760 in all rows
# rename - specified which columns should be renamed before putting into a database
# e.g.: {"AC Power": "value"} - replaces dataframe's column 'AC Power' with databases's 'value'
def to_database(df, params, table, defaults={'site_id': 0}, rename={"AC Power": "value"}):
    con = None
    try:
        engine = sqlalchemy.create_engine("postgresql://{user}:{password}@{host}:5432/{database}".format(**params))
        con = engine.connect()
        df = df.rename(columns=rename)
        for item in defaults.items():
            df.insert(0, item[0], item[1])
        df.to_sql(table, con, if_exists='append', index_label='date')
    finally:
        if con:
            con.close()


# modifies active_sites_data.csv by adding a column to the sites available for .csv fetch
def store_sites(sites):
    for site in sites:
        fetch("0,0,1,1", site)

    # reading all files in DIR
    files = [f for f in listdir(DIR)]
    sites = [int(f[4:f.find('_')]) for f in files]
    names = [f[f.find('_') + 1:f.find('(')] for f in files]
    csv_dict = {k: v for k, v in sorted(dict(zip(sites, names)).items(), key=lambda item: item[0])}

    # reading sites metadata file
    df = pd.read_csv('active_sites_data.csv')
    df.set_index(df.columns[0], inplace=True)
    df.insert(9, 'csv_name', '')
    for site_id in csv_dict:
        if site_id in df['csv_name']:
            df['csv_name'][site_id] = csv_dict[site_id]
        else:
            df = pd.concat([df, pd.DataFrame({'csv_name': csv_dict[site_id]}, [site_id])])
    df.sort_index(inplace=True)
    df['csv_name'] = df['csv_name']  # idk why it works but without this there's only 300 csv_names committed out of 800
    df.to_csv('active_sites_data.csv')


# returns name of a .csv file given site_id and period
# site_id - int, view - str (e.g.: "0,1,0,0"), dt - datetime obj of the beginning of intended period
def get_file_name(site_id, view, dt):
    views = view.split(',')
    unit = unit_view[views[1]]
    if unit != 'month':
        of = str(get_date_ago[unit](dt, int(views[2])).date())
    else:
        of = get_date_ago[unit](dt, int(views[2])).date().strftime("%B %Y")
    return "Site" + str(site_id) + "_" + sites_data['csv_name'][site_id] + '(Inverter-Direct,' + \
           unit.capitalize() + ' of ' + of + ').csv'


# returns end datetime in given timezone
def get_timezone_time(site_id):
    timezone = sites_data['timezone'][site_id]
    return datetime.now()  # TODO: make it actually work


# converts power to energy, t in minutes
def w_to_wh(w, t):
    return w * t * 60 / 3600


# returns a dataframe of 10-minute production batches [start, end)
# start, end - datetime objects of time interval
# end - end datetime at the timezone
# time_unit - 'day', 'week', or 'month' in which data will be fetched
# 'day': 1-minute intervals, 'week': 10-minute intervals, 'month': 1-hour intervals
def get_historical_data(site_id, start, end, current, time_unit='week'):
    total = None
    names = None
    for i in range(get_units_ago[time_unit](start, current),  # iterating through view 'ago' values
                   get_units_ago[time_unit](end - timedelta(minutes=1), current) - 1, -1):
        name, data = get_inv_data(site_id, "0,{},{},1".format(unit_view[time_unit], i))
        if not total:  # if first append
            total = data
            names = name  # assuming inverters names don't change over time
            continue
        for j in range(len(total)):
            total[j] = pd.concat([total[j], data[j]])

    for i in range(len(total)):  # removing rows outside of [start, end), e.g.: [mon, tue, |start, ... |, end, sat, sun]
        total[i] = total[i][np.logical_and(str(start) <= total[i].index, total[i].index < str(end))]
    return names, total


# view - string of arguments
def fetch(view, site_id):
    filename = DIR + "/" + get_file_name(site_id, view, get_timezone_time(site_id))
    tt = time.time()
    print("Fetching: " + filename)
    if not os.path.exists(DIR):
        os.makedirs(DIR)
    if not os.path.exists(filename):
        global last_fetch
        while time.time() - last_fetch < wait_time:
            time.sleep(0.1)
        last_fetch = time.time()

        url = URL + '?view={view}&cond=site_id={site_id}'.format(view=view, site_id=site_id)
        s = str(requests.get(url).content)
        try:
            new_url = "https://solrenview.com" + s[s.index('/downloads'):s.index('.csv') + 4]
        except:
            return ""

        new_filename = DIR + '/' + new_url.split('/')[-1]  # .csv file

        if new_filename != filename:  # if predicted filename is not the actual one
            print(new_filename, filename)

        raw = requests.get(new_url).text
        with open(filename, 'w+') as f:
            f.write(raw)
    else:
        with open(filename, 'r') as f:
            raw = f.read()
    return raw


# raw - str of .csv
# returns a tuple of lists: ['inverter_name1', ...], [<dataframe>, ...]
def parse(raw):
    tt = time.time()
    raw = raw[raw.index(','):]  # remove the header ("Quad 7 - Phase 2...")
    raw = raw.replace('(', '').replace(')', '')
    # list ['', 'inv#1  - 1013021546296  (PVI 28TL)', '', '', '', '', '', ...]
    inverters_raw = raw[:raw.index('Timeframe')].strip().split(',')

    # indexes of inverters to split vertically; - 1 because timeframe is removed later
    indexes = [i - 1 for i in range(len(inverters_raw)) if inverters_raw[i] != '' and i != 1]

    csv = pd.read_csv(StringIO(raw))
    # setting timeframe as an index (instead of 0, 1, ...) and removing brackets: e.g.: '[2019-...:00]' -> '2019-...:00'
    csv.set_index(csv.columns[0], inplace=True)

    # dfs = np.split(csv, indexes, axis=1)  # ineffective, takes about 0.4s

    indexes.append(len(csv.columns))
    dfs = [] * len(indexes)
    prev_i = 0
    for i in indexes:
        dfs.append(csv[csv.columns[prev_i:i]])
        prev_i = i

    del csv, raw

    names = [] * len(dfs)
    dfs_fin = [] * len(dfs)
    for i in range(0, len(dfs)):
        rows = np.split(dfs[i], [1, 2])  # splitting into columns (0), units (1) - unused, rest of data (2)
        main_df = rows[2]
        main_df.columns = list(rows[0].iloc[0])  # setting column names [AC Energy, AC Power, ...]
        main_df.index = list(map(lambda x: x[1:-1], rows[2].index))
        main_df.fillna(0, inplace=True)
        main_df["AC Power"] = main_df["AC Power"].astype(int)
        main_df["AC Energy"] = main_df["AC Power"].astype(float)
        try:
            main_df["AC Current"] = main_df["AC Power"].astype(float)
        except KeyError:  # "AC Current" not in df
            pass
        dfs_fin.append(main_df)
        names.append(rows[0].columns[0])

    return names, dfs_fin


# returns a dictionary of inverter names and dataframes of their production
# e.g.: {'inv #1 - 141894234': <dataframe>, ...}
def get_inv_data(site_id, view):
    names, dfs = parse(fetch(view, site_id))
    return names, dfs  # TODO: save inverter names


# merges inverter dataframes by column using merge_function for each of the columns
# default: sum all the AC Energies
# len(cols) == len(merge_functions)
def merge_inverters(inv_dfs, cols=['AC Power'], merge_functions=[lambda lst: sum(lst)]):
    indexes = inv_dfs[0].index
    final_df = pd.DataFrame(index=indexes)

    for ci in range(len(cols)):
        final_df[cols[ci]] = 0
        for i in indexes:
            total = [] * len(inv_dfs)
            for df in inv_dfs:
                total.append(df[cols[ci]][i])  # for each of columns, for each of inverters, adding each of rows
            try:
                final_df[cols[ci]][i] = merge_functions[ci](total)
            except:
                pass
    return final_df


# Inverter merge optimized for only summing inverter's AC Power and converting it to Wh
def merge_inv_production(inv_dfs):
    # time interval in seconds for conversion to watts; based on indexes[1] - indexes[0]
    interval = sum((np.array(list(map(int, inv_dfs[0].index[1].split(' ')[1].split(':'))))
                    - np.array(list(map(int, inv_dfs[0].index[0].split(' ')[1].split(':')))))
                   * np.array([3600, 60, 1]))
    vals = inv_dfs[0]["AC Power"]
    for df in inv_dfs[1:]:
        vals = vals.add(df['AC Power'])
    vals = vals.mul(interval / 3600)
    return pd.DataFrame(vals)


if __name__ == '__main__':
    inv_names, inv_data = get_historical_data(4760, datetime(2019, 1, 1), datetime(2019, 2, 1), datetime.now(), 'week')
    total_data = merge_inv_production(inv_data)
    plot_days(total_data['AC Power'], 24 * 6)
    to_database(total_data, get_db_params(cloud_connect=True), "production",
                {'site_id': 4760, 'unit': 'Wh', 'measured_by': 'INVERTER'}, {"AC Power": "value"})
