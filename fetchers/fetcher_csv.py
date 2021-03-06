import os
import time
from datetime import datetime, timedelta
from io import StringIO
import pytz
from bs4 import BeautifulSoup

import numpy as np
import pandas as pd
import requests
import sqlalchemy
from dateutil.relativedelta import relativedelta
from requests.exceptions import ChunkedEncodingError

from db.helper_db import get_db_params, run_query
from fetchers.fetcher_xml import get_active_sites

DIR = 'solectria_raw_csv'
URL = "https://solrenview.com/cgi-bin/cgihandler.cgi"

wait_time = 2  # seconds
last_fetch = 0

unit_view = {'day': '0', 'week': '1', 'month': '2', '0': 'day', '1': 'week', '2': 'month'}
interval_unit = {'day': 1, 'week': 10, 'month': 60, 1: 'day', 10: 'week', 60: 'month'}

sites_data = pd.read_csv('./csv/solectria_sites.csv')
sites_data['site_id'] = sites_data['site_id'].astype(int)
sites_data.set_index('site_id', inplace=True)

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

# returns date t units before target
# end - end datetime at the timezone
get_date_ago = {'day': lambda target, n: round_dt['day'](target) - timedelta(days=n),
                'week': lambda target, n: round_dt['week'](target) - relativedelta(weeks=n),
                'month': lambda target, n: round_dt['month'](target) - relativedelta(months=n)}


# modifies active_sites_data.csv by adding a column to the solectria_sites available for .csv fetch
def store_sites(sites):
    for site in sites:
        fetch("0,0,1,1", site)
    # reading all files in DIR
    files = [f for f in os.listdir(DIR)]
    sites = [int(f[4:f.find('_')]) for f in files]
    names = [f[f.find('_') + 1:f.find('(')] for f in files]
    csv_dict = {k: v for k, v in sorted(dict(zip(sites, names)).items(), key=lambda item: item[0])}

    # reading solectria_sites metadata file
    df = pd.read_csv('active_sites_data.csv')
    df.set_index(df.columns[0], inplace=True)
    df.insert(9, 'fetch_id', '')
    for site_id in csv_dict:
        if site_id in df['fetch_id']:
            df['fetch_id'][site_id] = csv_dict[site_id]
        else:
            df = pd.concat([df, pd.DataFrame({'fetch_id': csv_dict[site_id]}, [site_id])])
    # df.sort_index(inplace=True)
    # df['fetch_id'] = df['fetch_id']  # idk why it works but without this there's only 300 fetch_ids committed out of 800
    df.to_csv('active_sites_data.csv')


# splits inverter name into its order, serial number, and model
# e.g.: # 'inv#3  - 1012841547503  (PVI 36TL)' -> (3, '1012841547503', 'PVI 36TL')
def split_inv_name(inv_name):
    order = int(inv_name[4:inv_name.index(' ')])  # 'inv#1' -> 1
    if 'inverter' not in inv_name.lower():
        manuf_id = inv_name[inv_name.index('-') + 2:inv_name.index('(')].strip()
    else:  # e.g.: 'inv#4  - Inverter #4  (1013821711010 PVI 60TL)' -> '1013821711010'
        manuf_id = inv_name[inv_name.index('(') + 1:inv_name[inv_name.index('('):].index(' ') + inv_name.index('(')]

    models = inv_name[inv_name.index('('):].replace('  ', ' ').split(' ')
    model = models[-2].replace('(', '') + " " + models[-1].replace(')',
                                                                   '')  # 'inv#3  - 1012841547503  (PVI 36TL)' -> 'PVI 36TL'
    return order, manuf_id, model


# returns name of a .csv file given site_id and period
# site_id - int, view - str (e.g.: "0,1,0,0"), dt - datetime obj of the beginning of intended period
def get_file_name(site_id, view, dt):
    views = view.split(',')
    unit = unit_view[views[1]]
    if unit != 'month':
        of = str(get_date_ago[unit](dt, int(views[2])).date())
    else:
        of = get_date_ago[unit](dt, int(views[2])).date().strftime("%B %Y")
    return "Site" + str(site_id) + "_" + sites_data['fetch_id'][site_id] + '(Inverter-Direct,' + \
           unit.capitalize() + ' of ' + of + ').csv'


# returns end datetime in given timezone
def get_timezone_time(site_id):
    try:
        return datetime.now(pytz.timezone(sites_data['timezone'][site_id]))
    except KeyError:
        return datetime.now()


# view - string of arguments
def fetch(view, site_id):
    try:
        filename = DIR + "/" + get_file_name(site_id, view, get_timezone_time(site_id))
    except (KeyError, TypeError):
        filename = "filename unknown"
    # print("Fetching file:", filename)
    if not os.path.exists(DIR):
        os.makedirs(DIR)
    if not os.path.exists(filename):
        global last_fetch
        while time.time() - last_fetch < wait_time:
            time.sleep(0.1)
        last_fetch = time.time()

        url = URL + '?view={view}&cond=site_id={site_id}'.format(view=view, site_id=site_id)
        try:
            with requests.get(url) as r:
                s = r.content
                r.close()
            try:
                s = BeautifulSoup(s, "html.parser").find_all('script')[-2].text
                new_url = "https://solrenview.com" + s[s.index('/downloads'):s.index('.csv') + 4]
            except:
                return ""

            filename = DIR + '/' + new_url.split('/')[-1]  # .csv file

            with requests.get(new_url) as r:
                raw = r.text
                r.close()

        except ChunkedEncodingError as e:
            print(e)
            time.sleep(wait_time)
            return fetch(view, site_id)  # might not be the best solution but idk how else to fix it

        with open(filename, 'w+') as f:
            f.write(raw)
    else:
        with open(filename, 'r') as f:
            raw = f.read()
    return raw


# raw - str of .csv
# returns a tuple of lists: ['inverter_name1', ...], [<dataframe>, ...]
def parse(raw):
    try:
        raw = raw[raw.index('inv') - 1:]  # remove the header ("Quad 7 - Phase 2...")
    except ValueError:
        raise RuntimeError('Impossible to parse: ' + raw + "...")
    i = raw.index('Timeframe')
    raw = raw[:i] + raw[i:].replace('(', '').replace(')', '')  # turning '(null' and 'null)' into 'null'

    # list ['', 'inv#1  - 1013021546296  (PVI 28TL)', '', '', '', '', '', ...]
    inverters_raw = raw[:raw.index('Timeframe')].strip().split(',')

    if 'Weather' in inverters_raw[-1]:
        j = raw.index('Weather')
        i = raw[j:].index(')')  # last character on line with inverters
        raw = raw[:i + j + 1] + ',,,,' + raw[i + j + 1:]  # adding comas to end of Weather, otherwise it skews the table
    # indexes of inverters to split vertically; - 1 because timeframe is removed later
    indexes = [i - 1 for i in range(len(inverters_raw)) if inverters_raw[i] != '' and i != 1]

    csv = pd.read_csv(StringIO(raw))
    # setting timeframe as a id (instead of 0, 1, ...) and removing brackets: e.g.: '[2019-...:00]' -> '2019-...:00'
    csv.set_index(csv.columns[0], inplace=True)

    # dfs = np.split(csv, indexes, axis=1)  # ineffective, takes about 0.4s

    indexes.append(len(csv.columns))
    dfs = [] * len(indexes)
    prev_i = 0
    for i in indexes:
        dfs.append(csv[csv.columns[prev_i:i]])
        prev_i = i

    del csv, raw

    dfs_fin = [] * len(dfs)
    for i in range(len(dfs)):
        rows = np.split(dfs[i], [1, 2])  # splitting into columns (0), units (1) - unused, rest of data (2)
        main_df = rows[2]
        main_df.columns = list(rows[0].iloc[0])  # setting column names [AC Energy, AC Power, ...]
        main_df.index = list(map(lambda x: x[1:-1], rows[2].index))  # '[2020-02-23 00:00:00]' -> '2020-02-23 00:00:00'
        main_df.fillna(0, inplace=True)
        if 'inv' in rows[0].columns[0]:
            main_df["AC Power"] = main_df["AC Power"].astype(int)
            main_df["AC Energy"] = main_df["AC Energy"].astype(float)
            try:
                main_df["AC Current"] = main_df["AC Current"].astype(float)
            except KeyError:  # "AC Current" not in df
                pass
        elif 'Weather' in rows[0].columns[0]:
            for col in main_df.columns:
                main_df[col] = main_df[col].astype(float)
        main_df.columns.name = rows[0].columns[0]  # setting inverter's name
        dfs_fin.append(main_df)

    return dfs_fin


# inserts dataframe (df) into the database (from db_params) into given table
# defaults - default values for columns that are in the database but not in dataframe
# e.g.: {'site_id': 4760} to will add a column 'site_id' and puts 4760 in all rows
# rename - specified which columns should be renamed before putting into a database
# e.g.: {"AC Power": "value"} - replaces dataframe's column 'AC Power' with databases's 'value'
def store(table, df, defaults={}, rename={}, drop=[], index_label=None, bulk_load=False):
    conn = None
    raw_conn = None
    try:
        engine = sqlalchemy.create_engine(
            "postgresql://{user}:{password}@{host}:5432/{database}".format(**get_db_params()))
        conn = engine.connect()
        df = df.copy()
        if len(rename) != 0:
            df.rename(columns=rename, inplace=True)
        if len(drop) != 0:
            df.drop(drop, axis=1, inplace=True)
        for item in defaults.items():
            df.insert(0, item[0], item[1])
        if bulk_load:
            output = StringIO()
            df.to_csv(output, sep='\t', header=False)
            output.getvalue()
            output.seek(0)  # jump to start of stream
            raw_conn = engine.raw_connection()
            cursor = raw_conn.cursor()
            columns = [index_label] + list(df.columns) if index_label else df.columns
            cursor.copy_from(output, table, null="", columns=columns)
            raw_conn.commit()
            cursor.close()
            raw_conn.close()
        else:
            df.to_sql(table, conn, if_exists='append', index=False if index_label is None else True,
                      index_label=index_label)
    finally:
        if conn is not None:
            conn.close()
        if raw_conn is not None:
            raw_conn.close()


# returns dataframes of inverter production production
def get_inv_production(site_id, view):
    return parse(fetch(view, site_id))


# returns dataframe of site production combined from its inverters
# start, end - datetime objects
# interval - time interval for production batches in minutes, can be 1, 10, or 60
def get_site_production(site_id, start, end, time_interval=10):
    return merge_inv_production(get_historical_data(site_id, start, end, interval_unit[time_interval]))


# returns a dataframe of 10-minute production batches [start, end)
# start, end - datetime objects of time interval
# end - end datetime at the timezone
# time_unit - 'day', 'week', or 'month' in which data will be fetched
# 'day': 1-minute intervals, 'week': 10-minute intervals, 'month': 1-hour intervals
def get_historical_data(site_id, start, end, time_unit='week'):
    current = get_timezone_time(site_id)
    total = None
    for i in range(get_units_ago[time_unit](start, current),  # iterating through view 'ago' values
                   get_units_ago[time_unit](end - timedelta(minutes=1), current) - 1, -1):
        data = get_inv_production(site_id, "0,{},{},1".format(unit_view[time_unit], i))

        if False not in [data[j][data[j].columns[0]].isnull().all() for j in range(len(data))]:  # if all values are nan
            continue

        if not total:  # if first append
            total = data
            continue
        for j in range(len(total)):
            total[j] = pd.concat([total[j], data[j]])

    if total is None:
        raise RuntimeError('Fetched data is empty')

    for i in range(len(total)):  # removing rows outside of [start, end), e.g.: [mon, tue, |start, ... |, end, sat, sun]
        total[i] = total[i][np.logical_and(str(start) <= total[i].index, total[i].index < str(end))]
        if 'AC Power' in total[i]:
            total[i] = power_to_production(total[i], 'AC Power')
    return total


# converts a dataframe column of power in W to production in Wh
def power_to_production(power, column):
    # time interval in seconds for conversion to watts; based on id[1] - id[0]
    interval = sum((np.array(list(map(int, power.index[1].split(' ')[1].split(':'))))
                    - np.array(list(map(int, power.index[0].split(' ')[1].split(':')))))
                   * np.array([3600, 60, 1]))
    power[column] = power[column].mul(int(round(interval / 3600)))
    return power


# Inverter merge for 'AC Power' column already converted to Wh
def merge_inv_production(inv_dfs):
    total = None
    for df in inv_dfs:
        if total is None:
            total = df
            continue
        total['AC Power'] = total['AC Power'].add(df['AC Power'])
    return pd.DataFrame(total)


# collects all data about a site for specified time period, including: nn production, component production, & weather
# inserts all the data into the database
# start, end - datetime objects
# interval - time interval for production batches in minutes, can be 1, 10, or 60
def collect_data(site_id, start, end, interval=10):
    t = time.time()
    try:
        inv_data = get_historical_data(site_id, start, end, interval_unit[interval])
    except RuntimeError:
        return False
    print('Time fetching & parsing:', time.time() - t)
    t = time.time()
    if 'Weather' in inv_data[-1].columns.name:
        store('weather', inv_data[-1], {'site_id': site_id},
              {"Ambient": "temperature_ambient", "Module": "temperature_module", "Irradiance": "irradiance",
               "Wind Direction": "wind_direction", 'Wind Speed': 'wind_speed'}, index_label='date', bulk_load=True)
        inv_data = inv_data[:-1]  # removing weather from inverters

    total = None
    for i in range(len(inv_data)):
        order, manuf_id, model = split_inv_name(inv_data[i].columns.name)

        # check if component already in component_details; throws exception if error with the query
        if len(run_query(
                "SELECT * from component_details WHERE manufacturers_component_id LIKE '{}'".format(manuf_id))) == 0:
            store('component_details', pd.DataFrame(
                {'component_id': order - 1, 'manufacturers_component_id': manuf_id, 'type': 'inverter',
                 'sub_type': model, 'site_id': site_id, 'data_provider': 'Solectria', 'manufacturer': 'Solectria',
                 'is_energy_producing': True}, [0]), index_label=None)

        inv_data[i].insert(0, 'component_id', manuf_id)
        if i == 0:
            total = inv_data[i]
            continue
        total = pd.concat([total, inv_data[i]])

    # TODO: check if inverter data is not empty
    store("component_production", total, {'unit': 'Wh'}, {"AC Power": "value"},
          [c for c in total.columns if c not in ['AC Power', 'component_id']],  # removing all other columns
          index_label='date', bulk_load=True)

    total_data = merge_inv_production(inv_data)
    store("production", total_data, {'site_id': site_id, 'unit': 'Wh', 'measured_by': 'INVERTER'},
          {"AC Power": "value"}, [c for c in total_data.columns if c not in ['AC Power']],
          index_label='date', bulk_load=True)

    if len(run_query("SELECT * from site WHERE site_id LIKE '{}'".format(site_id))) == 0:
        site_data = sites_data.loc[site_id].to_dict()
        site_data['status'] = 'Active'
        try:
            site_data['zip'] = int(site_data['zip'])
            if site_data['zip'] < 10000:  # if 4-digit zip
                site_data['zip'] = '0' + str(site_data['zip'])
        except ValueError:
            pass
        store('site', pd.DataFrame(site_data, [site_id]), index_label='site_id')
    print('Time inserting into the database:', time.time() - t)
    return True
