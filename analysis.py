import time

import pandas as pd
from site_fetching.get_coordinate import get_coordinates
import geopy.distance
import numpy as np
from scipy.stats import ttest_ind
from datetime import datetime
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt

from fetchers.fetcher_csv import get_site_production, collect_data
from db.helper_db import run_query, run_queries
from misc.helper import time_batches


def random_closest():
    solectria_sites = pd.read_csv('./csv/solectria_sites.csv', index_col='site_id')
    solectria_sites = solectria_sites[solectria_sites['size'].notna()][
        solectria_sites['csv_name'].notna()]  # removing solectria sites with no size
    solectria_sites = solectria_sites[solectria_sites['latitude'] > 39]

    solaredge_sites = pd.read_csv('./csv/solaredge_sites.csv', index_col='site_id')
    solectria_sites = solectria_sites[solectria_sites['size'].notna()]  # removing Boott Mills

    # choosing a random solaredge site
    site = np.random.choice(solaredge_sites.index, 1)[0]
    site_coordinates = get_coordinates(*solaredge_sites.loc[site, ['address', 'city', 'state', 'zip']].values)

    # finding closest solectria site
    min_dist = 1e100
    min_site_id = -1
    for site_id, row in solectria_sites.iterrows():
        dist = geopy.distance.vincenty(site_coordinates, (row['latitude'], row['longitude'])).km
        if dist < min_dist:
            min_dist = dist
            min_site_id = site_id

    get_site_production(min_site_id, datetime(2019, 1, 1), datetime(2020, 1, 1)).to_csv('temp.csv')


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


if __name__ == '__main__':
    site_ids = [605934, 605990, 606333, 613895, 615959, 635389, 659086, 691203, 695265, 703541, 704131, 748679]
    start = datetime(2018, 1, 1)
    end = datetime(2019, 1, 1)
    # collect_data(site_id, start, end, 60)  # for solectria sites
    datas = run_queries(["SELECT * FROM production WHERE site_id = '{}' AND date "
                         "BETWEEN '{}'::timestamp and '{}'::timestamp"
                        .format(site_id, str(start), str(end - relativedelta(minutes=1)))
                         for site_id in site_ids], True)
    site_metadatas = run_queries(["SELECT * FROM site WHERE site_id = '{}'".format(site_id) for site_id in site_ids])
    system_sizes = [float(site_metadata[0]['size']) for site_metadata in site_metadatas]
    efficiencies = [daily_efficiency(data, system_size, 15 * 60) for data, system_size in zip(datas, system_sizes)]

    print("P-value:", ttest_ind(efficiencies[0]['value'], efficiencies[1]['value'])[1])
    plt.hist(efficiencies[1]['value'])
    plt.show()
