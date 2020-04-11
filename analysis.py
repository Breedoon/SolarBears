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
from misc.helper import time_batches, normalize

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


def plot_sites(solaredge_sites=None, solectria_sites=None):
    if solaredge_sites is None:
        solaredge_sites = pd.read_csv('csv/solaredge_sites.csv')
    if solectria_sites is None:
        solectria_sites = pd.read_csv('csv/solectria_sites.csv')

    solaredge_sites.insert(0, 'source', 'solaredge')
    solectria_sites.insert(0, 'source', 'solectria')
    df = pd.concat([solaredge_sites, solectria_sites], ignore_index=True)
    plot_map(df)


def plot_annual_performance(solaredge_effs, solectria_effs):
    plt.plot_date(pd.to_datetime(solaredge_effs.columns), normalize(np.mean(solaredge_effs), 30),
                  linestyle='solid', marker=None, label='Solaredge', color='orange')
    plt.plot_date(pd.to_datetime(solectria_effs.columns), normalize(np.mean(solectria_effs), 30),
                  linestyle='solid', marker=None, label='Solectria', color='purple')
    plt.xlabel('Date')
    plt.ylabel('Performance')
    plt.show()


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


def copy_from_main_db():
    from fetchers.fetcher_csv import store
    from math import ceil
    site = run_query('SELECT * FROM site', retrieve=True)
    store('site', site, bulk_load=True)
    component_details = run_query('SELECT * FROM component_details', retrieve=True)
    store('component_details', component_details, bulk_load=True)
    n = run_query('SELECT count(*) FROM production')[0]['count']
    l = 500000
    for i in range(ceil(n / l)):
        production = run_query('SELECT * FROM production LIMIT {} OFFSET {}'.format(l, i * l), retrieve=True)
        production.fillna(0, inplace=True)
        production["value"] = production["value"].astype(int)
        store('production', production, bulk_load=True)


# converts production to power
# production - dataframe with columns 'value' and 'date' for production in Wh and date in format '2020-01-20 10:00:00'
# if interval == None, assumes production is sorted by 'date', and calculates interval based on first two 'date's
def df_to_power(production, interval=None):
    # time interval in seconds for conversion to watts; based on production[1] - production[0]
    if not interval:
        interval = sum((np.array(list(map(int, str(production['date'][1]).split(' ')[1].split(':'))))
                        - np.array(list(map(int, str(production['date'][0]).split(' ')[1].split(':')))))
                       * np.array([3600, 60, 1]))
    # efficiency = W / W_max; W = Wh / h; W_max = system size * 1000 (kW to W)
    production['value'] = production['value'].div(interval / 3600)
    # production['value'].fillna(0, inplace=True)
    return production


def df_to_efficiency(production, size, interval=None):
    return df_to_power(production).div(size * 1000)


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
    for day in result:
        result[day] = result[day] / n_points[day]
    return pd.DataFrame(data=list(result.values()), index=result.keys(), columns=['value'])


# returns a file name to store daily efficiencies for a given site for given time period
def get_filename(site_id, start, end):
    return CACHE_DIR + '/' + "_".join([str(site_id), str(start), str(end)]).replace(':', '=') + '.csv'


# conducts a two-tailed t-test comparing two
# 'sites_1' and 'sites_2' - lists of site_ids; 'start', 'end' - datetime objects
# interval in minutes
def average_daily_efficiency(site_ids, start, end, interval):
    site_ids = list(map(str, site_ids))

    site_metadatas = run_query(
        'SELECT site_id, size FROM site '
        'WHERE site_id IN ' + '(' + ("'{}', " * len(site_ids))[:-2].format(*site_ids) + ')', True).set_index('site_id')
    system_sizes = [float(site_metadatas['size'][str(id)]) for id in site_ids]

    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
    existing_files = [CACHE_DIR + '/' + f for f in os.listdir(CACHE_DIR)]
    expected_files = [get_filename(site_id, start, end) for site_id in site_ids]

    efficiencies = [[]] * len(site_ids)
    new_sites = {}

    for i in range(len(site_ids)):
        if expected_files[i] not in existing_files:
            new_sites[site_ids[i]] = i
        else:
            efficiencies[i] = pd.read_csv(expected_files[i]).set_index('date')
    if len(new_sites) > 0:
        new_datas = run_query(
            "SELECT site_id, date, value FROM pss.public.production "
            "WHERE date >= '{start}'::timestamp and date < '{end}'::timestamp AND "
            .format(start=str(start), end=str(end)) +
            "site_id IN " + "(" + ("'{}', " * len(new_sites))[:-2].format(*new_sites) + ')', True)

        daily_power_per_site = {}
        n_elements_per_site = {}
        daily_efficiency_per_site = {}
        powers = df_to_power(new_datas, interval * 60)
        for site_id, date, value in zip(powers['site_id'], powers['date'], powers['value']):
            if not daily_power_per_site.get(site_id):
                daily_power_per_site[site_id] = {}
                n_elements_per_site[site_id] = {}
            day = date._date_repr
            if not daily_power_per_site[site_id].get(day):
                daily_power_per_site[site_id][day] = value
                n_elements_per_site[site_id][day] = 1
                continue
            daily_power_per_site[site_id][day] += value
            n_elements_per_site[site_id][day] += 1

        for site_id in daily_power_per_site:
            daily_efficiency_per_site[site_id] = {}
            for day in daily_power_per_site[site_id]:
                daily_efficiency_per_site[site_id][day] = daily_power_per_site[site_id][day] \
                                                          / n_elements_per_site[site_id][day] \
                                                          / (system_sizes[new_sites[site_id]] * 1000)
            efficiencies[new_sites[site_id]] = pd.DataFrame(data=list(daily_efficiency_per_site[site_id].values()),
                                                            index=daily_efficiency_per_site[site_id].keys(),
                                                            columns=['value'])
            efficiencies[new_sites[site_id]].to_csv(get_filename(site_id, start, end), index_label='date')

    total_effs = efficiencies[0].rename(columns={"value": site_ids[0]}).transpose()
    for i in range(1, len(efficiencies)):
        total_effs = total_effs.append(efficiencies[i].rename(columns={"value": site_ids[i]}).transpose(), sort=True)
    total_effs.fillna(0, inplace=True)

    return total_effs


if __name__ == '__main__':
    solaredge_ids = list(zip(*run_query(
        "SELECT site_id FROM site WHERE length(site_id) > 4 AND state = 'MA'", True).values))[0]
    solectria_ids = list(set(choose_subset(0.2125)).intersection(set(map(int, list(zip(*run_query(
        "SELECT site_id FROM site WHERE length(site_id) <= 4", True).values))[0]))))

    start = datetime(2018, 1, 1)
    end = datetime(2020, 1, 1)
    solectria_effs = average_daily_efficiency(solectria_ids, start, end, 60)
    solaredge_effs = average_daily_efficiency(solaredge_ids, start, end, 15)

    plot_annual_performance(solectria_effs, solaredge_effs)
