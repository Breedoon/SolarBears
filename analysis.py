import time

import pandas as pd
from site_fetching.get_coordinate import get_coordinates
import geopy.distance
import numpy as np
from scipy.stats import ttest_ind
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import os

from fetchers.fetcher_csv import get_site_production, collect_data
from db.helper_db import run_query, run_queries
from misc.mapper import plot_map
from misc.helper import time_batches

CACHE_DIR = 'efficiency_cache'


# chooses a subset of solectria sites based on farthest solaredge sites
def choose_subset(radius):
    solaredge_sites = pd.read_csv('csv/solaredge_sites.csv')
    solaredge_sites = solaredge_sites[solaredge_sites['state'] == 'MA']  # removing CA sites and Boott Mills

    solectria_sites = pd.read_csv('csv/solectria_sites.csv')
    solectria_sites = solectria_sites[solectria_sites['size'].notna()]
    solectria_sites = solectria_sites[solectria_sites['fetch_id'].notna()]

    solectria_sites = solectria_sites[np.logical_and(np.logical_and(
        solectria_sites['latitude'] > min(solaredge_sites['latitude']) - radius,
        solectria_sites['latitude'] < max(solaredge_sites['latitude']) + radius), np.logical_and(
        solectria_sites['longitude'] > min(solaredge_sites['longitude']) - radius,
        solectria_sites['longitude'] < max(solaredge_sites['longitude']) + radius)
    )]

    return list(map(int, solectria_sites['site_id'].values))


# production - dataframe with columns 'value' and 'date' for production in Wh and date in format '2020-01-20 10:00:00'
# if interval == None, assumes production is sorted by 'date', and calculates interval based on first two 'date's
def df_to_efficiency(production, size, interval=None):
    # time interval in seconds for conversion to watts; based on production[1] - production[0]
    if not interval:
        interval = sum((np.array(list(map(int, str(production['date'][1]).split(' ')[1].split(':'))))
                        - np.array(list(map(int, str(production['date'][0]).split(' ')[1].split(':')))))
                       * np.array([3600, 60, 1]))
    # efficiency = W / W_max; W = Wh / h; W_max = system size * 1000 (kW to W)
    production['efficiency'] = production['value'].div(interval / 3600 * size * 1000)
    production['efficiency'].fillna(0, inplace=True)
    return production


# production - dataframe with columns 'value' and 'date' for production in Wh and date in format '2020-01-20 10:00:00'
# interval in seconds
def daily_efficiency(production, size, interval):
    efficiency = df_to_efficiency(production, size, interval)
    result = {}
    n_points = {}
    for eff, date in zip(efficiency['efficiency'], efficiency['date']):
        try:
            result[datetime(date.year, date.month, date.day)] += eff
            n_points[datetime(date.year, date.month, date.day)] += 1
        except KeyError:
            result[datetime(date.year, date.month, date.day)] = eff
            n_points[datetime(date.year, date.month, date.day)] = 1
    for dt in result:
        result[dt] = result[dt] / n_points[dt]
    return pd.DataFrame(data=list(result.values()), index=result.keys(), columns=['value'])


# returns a file name to store daily efficiencies for a given site for given time period
def get_filename(site_id, start, end):
    return CACHE_DIR + '/' + "_".join([str(site_id), str(start), str(end)]).replace(':', '=') + '.csv'


# conducts a two-tailed t-test comparing two
# 'sites_1' and 'sites_2' - lists of site_ids; 'start', 'end' - datetime objects
# interval in minutes
def average_daily_efficiency(site_ids, start, end, interval):
    site_metadatas = run_query(
        'SELECT site_id, size FROM site WHERE site_id IN ' + '(' + ("'{}', " * len(site_ids))[:-2].format(
            *site_ids) + ')', True).set_index('site_id')
    # site_metadatas = run_queries(["SELECT * FROM site WHERE site_id = '{}'".format(site_id) for site_id in site_ids])
    system_sizes = [float(site_metadatas['size'][str(id)]) for id in site_ids]

    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
    existing_files = [CACHE_DIR + '/' + f for f in os.listdir(CACHE_DIR)]
    expected_files = [get_filename(site_id, start, end) for site_id in site_ids]
    old_files = []
    efficiencies = [[]] * len(site_ids)
    new_site_ids = []
    old_site_ids = []
    for i in range(len(site_ids)):
        if expected_files[i] not in existing_files:
            new_site_ids.append(i)
        else:
            old_site_ids.append(i)
            old_files.append(expected_files[i])
    new_datas = run_queries(["SELECT * FROM production WHERE site_id = '{site_id}' AND date "
                         ">= '{start}'::timestamp and date < '{end}'::timestamp"
                        .format(site_id=site_id, start=str(start), end=str(end))
                         for site_id in [site_ids[i] for i in new_site_ids]], True)
    new_efficiencies = [daily_efficiency(data, size, interval * 60) for data, size in zip(new_datas, system_sizes)]
    old_efficiencies = [pd.read_csv(f).set_index('date') for f in old_files]
    for i in range(len(new_site_ids)):
        efficiencies[new_site_ids[i]] = new_efficiencies[i]
        new_efficiencies[i].to_csv(get_filename(site_ids[new_site_ids[i]], start, end), index_label='date')
    for i in range(len(old_site_ids)):
        efficiencies[old_site_ids[i]] = old_efficiencies[i]

    total_effs = [np.mean(eff['value']) for eff in efficiencies]  # mean efficiency for each site over the whole period

    # for i in range(len(datas)):  # checking if any of the sites doesn't cover the entire period from start to end
    #     if min(datas[i]['date']) != start or max(datas[i]['date']) != end - relativedelta(minutes=interval) or \
    #             len(datas[i]) < (end - start) / timedelta(minutes=interval):
    #         print(site_ids[i], min(datas[i]['date']), max(datas[i]['date']), len(datas[i]))

    # total_effs = np.concatenate(tuple(eff['value'].values for eff in efficiencies))  # day as a sample unit

    return total_effs


def plot_sites(solaredge_sites=None, solectria_sites=None):
    if solaredge_sites is None:
        solaredge_sites = pd.read_csv('csv/solaredge_sites.csv')
    if solectria_sites is None:
        solectria_sites = pd.read_csv('csv/solectria_sites.csv')

    solaredge_sites.insert(0, 'source', 'solaredge')
    solectria_sites.insert(0, 'source', 'solectria')
    df = pd.concat([solaredge_sites, solectria_sites], ignore_index=True)
    plot_map(df)


def solectria_to_database(solectria_ids):
    fetching_times = [] * len(solectria_ids)
    for site_id in solectria_ids:
        print('_____________________\nFetching site:', site_id)
        t = time.time()
        try:
            if not collect_data(site_id, datetime(2018, 1, 1), datetime(2020, 1, 1), 60):
                print("Fetching failed")
        except Exception as e:
            print('Error while fetching:', str(e))
        t = time.time() - t
        print("Total time:", t)
        fetching_times.append(t)
    print(solectria_ids)
    print(fetching_times)


if __name__ == '__main__':
    solaredge_ids = run_query("SELECT site_id FROM site WHERE length(site_id) > 4 AND state = 'MA'", True).values
    solectria_ids = choose_subset(0.2125)
    new_sites = set(solectria_ids) - set(map(int, run_query('SELECT site_id FROM site', True).values))
    solectria_to_database(new_sites)

    solaredge_effs = average_daily_efficiency(solaredge_ids, datetime(2019, 1, 1), datetime(2020, 1, 1), 15)
    print("Solaredge efficiencies:", solaredge_effs)
    solectria_effs = average_daily_efficiency(solectria_ids, datetime(2019, 1, 1), datetime(2020, 1, 1), 60)
    print("Solectria efficiencies:", solectria_effs)
    print("P-value:", ttest_ind(solaredge_effs, solectria_effs)[1])
