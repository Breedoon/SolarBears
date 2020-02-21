import os
import time
import pandas as pd
import numpy as np
import requests
from io import StringIO
from helper import plot_days, time_batches
from datetime import datetime, timedelta

DIR = 'solectria_raw_csv'
URL = "https://solrenview.com/cgi-bin/cgihandler.cgi"

wait_time = 0.5  # seconds
last_fetch = 0

site_ids = [4760, 5582, 5077]

# TODO: store units
# a dict of all unique units found in data for a specific value, since they are not stored
# e.g.: {'AC Energy': {[kWh]], ...}, ...}
units = {}


# converts power to energy, t in minutes
def w_to_wh(w, t):
    return w * t * 60 / 3600


# converts a unit (day/week/mon#h) to a digit (0/1/2)
def unit_to_view(unit):
    if unit == 'day':
        return 0
    elif unit == 'week':
        return 1
    elif unit == 'month':
        return 2
    else:
        raise ValueError("unit can be either 'day', 'week', or 'month'")


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


def get_file(filename, url):
    if not os.path.exists(DIR):
        os.makedirs(DIR)
    if not os.path.exists(filename):
        global last_fetch
        while time.time() - last_fetch < wait_time:
            time.sleep(0.1)
        last_fetch = time.time()
        r = requests.get(url)
        raw = r.text
        with open(filename, 'w+') as f:
            f.write(raw)
    else:
        with open(filename, 'r') as f:
            raw = f.read()
    return raw


# view - string of arguments
def fetch(view, site_id):
    url = URL + '?view={view}&cond=site_id={site_id}'.format(view=view, site_id=site_id)
    s = str(requests.get(url).content)  # TODO check raw data
    try:
        new_url = "https://solrenview.com" + s[s.index('/downloads'):s.index('.csv') + 4]
    except:
        return None
    filename = DIR + '/' + new_url.split('/')[-1]  # .csv file
    print("Fetching: " + filename)
    return get_file(filename, new_url)


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
    dfs = get_historic_data(4760, datetime(2019, 12, 1), datetime(2020, 1, 1), datetime.now())
    plot_days([w_to_wh(p, 10) for p in merge_inverters(dfs)['AC Power']], 24 * 6)
