import pandas as pd
from site_fetching.get_coordinate import get_coordinates
import geopy.distance
import numpy as np
from datetime import datetime
from fetchers.fetcher_csv import get_site_production, collect_data

from db.helper_db import run_query


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


# production - dataframe with columns 'value' and 'date' meaning production in Wh and date in format 2020-01-20 10:00:00
def df_to_efficiency(production, size):
    # time interval in seconds for conversion to watts; based on production[1] - production[0]
    interval = sum((np.array(list(map(int, str(production['date'][1]).split(' ')[1].split(':'))))
                    - np.array(list(map(int, str(production['date'][0]).split(' ')[1].split(':')))))
                   * np.array([3600, 60, 1]))
    production['efficiency'] = production['value'].div(interval / 3600 * size)  # eff = W / W_max (size); W = Wh / h
    return production


if __name__ == '__main__':
    site_id = 606333
    start = datetime(2018, 10, 1)
    end = datetime(2019, 1, 1)
    # collect_data(site_id, start, end, 60)
    data = run_query("SELECT * FROM production WHERE site_id = '{}' AND date "
                     "BETWEEN '{}'::timestamp and '{}'::timestamp"
                     .format(site_id, str(start), str(end)), True)
    site_metadata = run_query("SELECT * FROM site WHERE site_id = '{}'".format(site_id))[0]
    efficiency = df_to_efficiency(data, float(site_metadata['size']))

