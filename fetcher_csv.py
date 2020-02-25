import os
import time
import pandas as pd
import numpy as np
import requests
from io import StringIO
from helper import plot_days, time_batches
from fetcher_xml import get_active_sites
from os import listdir
from datetime import datetime, timedelta

DIR = 'solectria_raw_csv'
URL = "https://solrenview.com/cgi-bin/cgihandler.cgi"

wait_time = 0.5  # seconds
last_fetch = 0

unit_view = {'day': '0', 'week': '1', 'month': '2', '0': 'day', '1': 'week', '2': 'month'}

sites_data = pd.read_csv('active_sites_data.csv')
sites_data.set_index('site_id', inplace=True)


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
# site_id - int, view - str (e.g., "0,1,0,0"), dt - datetime obj of the beginning of intended period
def get_file_name(site_id, view, dt):
    # TODO: calculate filenames from the past
    return "Site" + str(site_id) + "_" + sites_data['csv_name'][site_id] + '(Inverter-Direct,' + \
           unit_view[view.split(',')[1]].capitalize() + ' of ' + str(dt.date()) + ').csv'


# returns current datetime in given timezon
def get_timezone_time(timezone):
    return datetime.now()  # TODO: make it actually work


# converts power to energy, t in minutes
def w_to_wh(w, t):
    return w * t * 60 / 3600


# returns datetime of previous monday; if dt is monday, returns itself
def get_prev_monday(dt):
    return dt - timedelta(days=dt.weekday())


# returns datetime without hour, minute, second
def clean_dt(dt):
    return datetime(dt.year, dt.month, dt.day)


# return number of weeks before end date
# current - current datetime at the timezone
def get_weeks_ago(target, current):
    return (get_prev_monday(clean_dt(current)) - get_prev_monday(clean_dt(target))).days // 7


# returns a dataframe of 10-minute production batches [start, end)
# start, end - datetime objects of time interval
# current - current datetime at the timezone
def get_historic_data(site_id, start, end, current):
    total = None
    for i in range(get_weeks_ago(start, current), get_weeks_ago(end, current) - 1, -1):  # from farthest week to 0
        data = get_inv_data(site_id, "0,1,{},1".format(i))
        if not total:  # if first append
            total = data
            continue
        for j in range(len(total)):
            total[j] = pd.concat([total[j], data[j]])

    for i in range(len(total)):  # removing rows outside of [start, end), e.g.: [mon, tue, |start, ... |, end, sat, sun]
        total[i] = total[i][np.logical_and(str(start) <= total[i].index, total[i].index < str(end))]
    return total


# view - string of arguments
def fetch(view, site_id):
    filename = DIR + "/" + get_file_name(site_id, view, get_timezone_time(sites_data['timezone'][site_id]))
    url = URL + '?view={view}&cond=site_id={site_id}'.format(view=view, site_id=site_id)
    print("Fetching: " + filename)
    if not os.path.exists(DIR):
        os.makedirs(DIR)
    if not os.path.exists(filename):
        global last_fetch
        while time.time() - last_fetch < wait_time:
            time.sleep(0.1)
        last_fetch = time.time()

        s = str(requests.get(url).content)
        try:
            new_url = "https://solrenview.com" + s[s.index('/downloads'):s.index('.csv') + 4]
        except:
            return ""

        new_filename = DIR + '/' + new_url.split('/')[-1]  # .csv file

        if new_filename != filename:  # if predicted filename is not the actual one
            print(new_filename, filename)

        raw = str(requests.get(url).content)
        with open(filename, 'w+') as f:
            f.write(raw)
    else:
        with open(filename, 'r') as f:
            raw = f.read()

    return raw


# raw - str of .csv
# returns a tuple: ['inverter_name', ...], [<dataframe>, ...]
def parse(raw):
    raw = raw[raw.index(','):]  # remove the header ("Quad 7 - Phase 2...")

    # list ['', 'inv#1  - 1013021546296  (PVI 28TL)', '', '', '', '', '', ...]
    inverters_raw = raw[:raw.index('Timeframe')].strip().split(',')

    # indexes of inverters to split; - 1 because timeframe is removed later
    indexes = [i - 1 for i in range(len(inverters_raw)) if inverters_raw[i] != '' and i != 1]

    csv = pd.read_csv(StringIO(raw))
    # setting timeframe as an index (instead of 0, 1, ...) and removing brackets: e.g.: '[2019-...:00]' -> '2019-...:00'
    csv.set_index(csv.columns[0], inplace=True)
    dfs = np.split(csv, indexes, axis=1)

    names = [] * len(dfs)
    dfs_fin = [] * len(dfs)
    for i in range(0, len(dfs)):
        rows = np.split(dfs[i], [1, 2])  # splitting into columns (0), units (1) - unused, rest of data (2)
        rows[2].columns = list(rows[0].iloc[0])  # setting column names [AC Energy, AC Power, ...]
        rows[2].index = list(map(lambda x: x[1:-1], rows[2].index))
        dfs_fin.append(rows[2])
        names.append(rows[0].columns[0])
    return names, dfs_fin


# dfs - list of dataframes
# replaces nan and null with 0 and tries to convert all columns to float
def clean(dfs):
    for df in dfs:
        for col in df.columns:
            for i in range(len(df[col])):
                data = df[col][i]
                if data != data or data == "null" or data == "(null" or data == "null)" or data == "(null)":
                    df[col][i] = 0
                else:
                    try:
                        df[col][i] = float(data)
                    except ValueError:
                        pass
    return dfs


# returns a dictionary of inverter names and dataframes of their production
# e.g., {'inv #1 - 141894234': <dataframe>, ...}
def get_inv_data(site_id, view):
    return clean(parse(fetch(view, site_id))[1])  # TODO: save inverter names


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


if __name__ == '__main__':
    fetch("0,0,1,1", 37)
