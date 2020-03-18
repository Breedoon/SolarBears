import pandas as pd
from site_fetching.get_coordinate import get_coordinates
import geopy.distance
import numpy as np
from datetime import datetime
from fetchers.fetcher_csv import get_site_production, collect_data

if __name__ == '__main__':
    solectria_sites = pd.read_csv('./csv/solectria_sites.csv', index_col='site_id')
    solectria_sites = solectria_sites[solectria_sites['size'].notna()][solectria_sites['csv_name'].notna()]  # removing solectria sites with no size
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