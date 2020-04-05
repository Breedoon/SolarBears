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


def random_closest():
    solectria, solaredge = choose_subset()

    site = np.random.choice(solaredge.index, 1)[0]

    # df.insert(0, item[0], item[1])
    # finding closest solectria site
    min_dist = 1e100
    min_site_id = -1
    # for se_id, se_lat, se_long in zip(solaredge['site_id'], solaredge['latitude'], solaredge['longitude']):
    #     for se_id, se_lat, se_long in zip(solectria['site_id'], solectria['latitude'], solectria['longitude']):

    # for site_id, row in solectria.iterrows():
    # dist = geopy.distance.vincenty(site_coordinates, (row['latitude'], row['longitude'])).km
    # if dist < min_dist:
    #     min_dist = dist
    #     min_site_id = site_id

    get_site_production(min_site_id, datetime(2019, 1, 1), datetime(2020, 1, 1)).to_csv('temp.csv')


def choose_subset():
    solaredge_sites = pd.read_csv('csv/solaredge_sites.csv')
    solaredge_sites = solaredge_sites[solaredge_sites['state'] == 'MA']  # removing CA sites and Boott Mills

    solectria_sites = pd.read_csv('csv/solectria_sites.csv')
    solectria_sites = solectria_sites[solectria_sites['size'].notna()]
    solectria_sites = solectria_sites[solectria_sites['fetch_id'].notna()]

    solectria_sites = solectria_sites[np.logical_and(np.logical_and(
        solectria_sites['latitude'] > min(solaredge_sites['latitude']) - 0.04,
        solectria_sites['latitude'] < max(solaredge_sites['latitude']) + 0.04), np.logical_and(
        solectria_sites['longitude'] > min(solaredge_sites['longitude']) - 0.04,
        solectria_sites['longitude'] < max(solaredge_sites['longitude']) + 0.04)
    )]

    # solaredge_sites = pd.read_csv('csv/solaredge_sites.csv')
    # solaredge_sites = solaredge_sites[solaredge_sites['site_id'].isin(solaredge_ids)]
    # solectria_sites = pd.read_csv('csv/solectria_sites.csv')
    # solectria_sites = solectria_sites[solectria_sites['site_id'].isin(solectria_ids)]
    #
    # plot_sites(solaredge_sites, solectria_sites)

    return solaredge_sites, solectria_sites


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


# conducts a two-tailed t-test comparing two
# 'sites_1' and 'sites_2' - lists of site_ids; 'start', 'end' - datetime objects
# interval in minutes
def average_daily_efficiency(site_ids, start, end, interval):
    site_metadatas = run_query(
        'SELECT site_id, size FROM site WHERE site_id IN ' + '(' + ("'{}', " * len(site_ids))[:-2].format(
            *site_ids) + ')', True).set_index('site_id')
    # site_metadatas = run_queries(["SELECT * FROM site WHERE site_id = '{}'".format(site_id) for site_id in site_ids])
    system_sizes = [float(site_metadatas['size'][str(id)]) for id in site_ids]
    datas = run_queries(["SELECT * FROM production WHERE site_id = '{site_id}' AND date "
                         ">= '{start}'::timestamp and date < '{end}'::timestamp"
                        .format(site_id=site_id, start=str(start), end=str(end))
                         for site_id in site_ids], True)
    efficiencies = [daily_efficiency(data, size, interval * 60) for data, size in zip(datas, system_sizes)]

    # for i in range(len(datas)):  # checking if any of the sites doesn't cover the entire period from start to end
    #     if min(datas[i]['date']) != start or max(datas[i]['date']) != end - relativedelta(minutes=interval) or \
    #             len(datas[i]) < (end - start) / timedelta(minutes=interval):
    #         print(site_ids[i], min(datas[i]['date']), max(datas[i]['date']), len(datas[i]))

    total_effs = [np.mean(eff['value']) for eff in efficiencies]  # mean efficiency for each site over the whole period

    # if not os.path.exists(CACHE_DIR):
    #     os.makedirs(CACHE_DIR)
    # for i in range(len(efficiencies)):  # storing efficiencies as csv
    #     efficiencies[i].to_csv(CACHE_DIR + '/' + "_".join([str(site_ids[i]), str(start), str(end)]).replace(':', '=') +
    #                            '.csv', index_label='date')
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
    return fetching_times


if __name__ == '__main__':
    solaredge_ids = ['613895', '635389', '593260', '605934', '455492', '452807', '453519', '468575', '453884', '516078',
                     '225542', '491336', '605990', '521291', '452828', '571900', '570177', '469767', '548640', '606333',
                     '439323', '515254', '659086', '565251', '691203', '695265', '703541', '704131', '420924', '748679',
                     '988889', '956683', '829735', '615959', '345593']
    solectria_ids = [1759, 3970, 1097, 3506, 3929, 4686, 4635, 4221, 4267, 3452, 4031, 4806, 1630, 918, 682,
                     3750, 715, 3590, 448, 4034, 3117, 694, 2015, 4065, 3113, 3855, 3988, 4656, 3125, 2550, 4661,
                     3778, 4721, 3577, 4381, 2026, 4160, 1700, 1703, 2002, 1701, 3009, 2014, 3347, 3980, 2494,
                     1702, 4234, 644, 4296, 3859, 3862, 3682, 3680, 2030, 3793, 2613, 1751, 3116, 1752, 2197,
                     3858, 2485, 1049, 1982, 2768, 4334, 3585, 3274, 948, 2037, 1603, 3771, 4731, 3421, 530, 1563, 3654,
                     3551, 3552, 3227, 4572, 843, 1745, 1740, 1444, 1850, 2177, 3660, 2437, 3558, 3095, 1881,
                     1892, 871, 716, 895]
    solaredge_effs = average_daily_efficiency(solaredge_ids, datetime(2019, 1, 1), datetime(2020, 1, 1), 15)
    print("Solaredge efficiencies:", solaredge_effs)
    solectria_effs = average_daily_efficiency(solectria_ids, datetime(2019, 1, 1), datetime(2020, 1, 1), 60)
    print("Solectria efficiencies:", solectria_effs)
    print("P-value:", ttest_ind(solaredge_effs, solectria_effs)[1])
