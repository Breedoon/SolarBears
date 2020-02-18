import os
import time
import pandas as pd
import numpy as np
import requests
from io import StringIO
from helper import plot_days

DIR = 'solectria_raw_csv'
URL = "https://solrenview.com/cgi-bin/cgihandler.cgi"

wait_time = 0.5  # seconds
last_fetch = 0

site_ids = [4760, 5582, 5077]

# TODO: store units
# a dict of all unique units found in data for a specific value, since they are not stored
# e.g.: {'AC Energy': {[kWh]], ...}, ...}
units = {}


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


# view - string of arguments;
def fetch(view, site_id):
    url = URL + '?view={view}&cond=site_id={site_id}'.format(view=view, site_id=site_id)
    s = str(requests.get(url).content)
    try:
        new_url = "https://solrenview.com" + s[s.index('/downloads'):s.index('.csv') + 4]
    except:
        return None
    filename = DIR + '/' + new_url.split('/')[-1]  # .csv file
    print("Fetching: " + filename)
    return get_file(filename, new_url)


# raw - str of .csv
# returns a dict: {'inverter_name': production_dataframe, ...}
def parse(raw):
    raw = raw[raw.index(','):]  # remove the header ("Quad 7 - Phase 2...")

    # list ['', 'inv#1  - 1013021546296  (PVI 28TL)', '', '', '', '', '', ...]
    inverters_raw = raw[:raw.index('Timeframe')].strip().split(',')

    # indexes of inverters to split; - 1 because timeframe is removed later
    indexes = [i - 1 for i in range(len(inverters_raw)) if inverters_raw[i] != '' and i != 1]

    csv = pd.read_csv(StringIO(raw))

    csv.set_index(csv.columns[0], inplace=True)  # setting timeframe as an index (instead of 0, 1, ...)

    dfs = np.split(csv, indexes, axis=1)

    dfs_fin = {}
    # timeframe = None
    for i in range(0, len(dfs)):
        rows = np.split(dfs[i], [1, 2])  # splitting into columns (0), units (1) - unused, rest of data (2)
        rows[2].columns = list(rows[0].iloc[0])  # setting column names [AC Energy, AC Power, ...]
        # if i == 0:
        #     rows[2].reset_index(drop=True, inplace=True)  # resetting index to 0, otherwise rows start with 2, 3, etc
        #     timeframe = rows[2]
        # else:
        #     rows[2].reset_index(drop=True, inplace=True)  # resetting index to 0, otherwise rows start with 2, 3, etc
        dfs_fin[rows[0].columns[0]] = rows[2]

    # # merge with timeframe
    # for i in dfs_fin:
    #     dfs_fin[i] = pd.concat([timeframe, dfs_fin[i]], axis=1)
    return dfs_fin


# dfs - list of dataframes
# replaces nan and null with 0
def clean(dfs):
    for inv in dfs:
        for col in dfs[inv].columns:
            for i in range(len(dfs[inv][col])):
                data = dfs[inv][col][i]
                if data != data or data == "null" or data == "(null" or data == "null)" or data == "(null)":
                    dfs[inv][col][i] = 0
                else:
                    try:
                        dfs[inv][col][i] = float(data)
                    except:
                        pass

    return dfs


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
    inv_data = clean(parse(fetch("0,2,2,1", 4760)))  # TODO: start/end dates
    inv_dfs = list(inv_data.values())
    production = merge_inverters(inv_dfs)
    plot_days(production, 24)
