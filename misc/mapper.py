import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
import time
import requests
import folium
import os
from colour import Color

colors = [
    'lightblue',
    'blue',
    'darkblue',
    'cadetblue',
    'lightgreen',
    'green',
    'darkgreen',
    'beige',
    'orange',
    'pink',
    'purple',
    'darkpurple',
    'red',
    'darkred',
]

DIR = '../dataviz'

if not os.path.exists(DIR):
    os.makedirs(DIR)


def get_color(i, n):
    global colors
    if len(colors) == 0:
        colors = [c.get_hex_l() for c in Color('red').range_to(Color('purple'), n)]
    return colors[i]


def get_lat_long(address, city, state, zip):
    address = address + ' ' + city + ', ' + state + ' ' + str(zip)
    try:
        geolocator = Nominatim()
        location = geolocator.geocode(address)
        return [location.latitude, location.longitude, True]
    except Exception as e:
        print(address + ' didn\'t work...')
        return np.nan, np.nan


def add_lat_long_to_df(data):
    lat = []
    lon = []
    for index, row in data.iterrows():
        # from site_fetching.get_coordinate import get_coordinates
        # _lat, _lon = get_coordinates(row['address'], row['city'], row['state'], row['zip'])
        _lat, _lon = get_lat_long(row['address'], row['city'], row['state'], row['zip'])
        lat.append(_lat)
        lon.append(_lon)

    data['latitude'] = lat
    data['longitude'] = lon
    return data


def initial_data_load(path):

    df = pd.read_csv(path)
    df = add_lat_long_to_df(df)
    return df


def plot_map(df):
    # production ratio demo
    demo_map = folium.Map(location=[df['latitude'].mean(), df['longitude'].mean()], zoom_start=3)
    fg = folium.FeatureGroup(name='Solar Locations')
    max_size = int(np.log2(np.nanmax(df['size']))) + 1
    for lat, lon, site_id, size in zip(df['latitude'], df['longitude'], df['site_id'], df['size']):
        try:
            if size != size:  # if nan
                raise ValueError
            fg.add_child(folium.Marker(location=[lat, lon],
                                       popup=(folium.Popup(str(site_id) + '\n size: ' + str(size))),
                                       icon=folium.Icon(color=get_color(int(np.log2(size)), max_size), icon_color='black')))
        except ValueError:
            print('Skipped: ', site_id)
    demo_map.add_child(fg)
    demo_map.save(DIR + '/sites_map.html')


if __name__ == '__main__':
    path = '../csv/solaredge_sites.csv'
    # df = pd.read_csv('../csv/solectria_sites.csv')
    df = initial_data_load(path)
    plot_map(df)
