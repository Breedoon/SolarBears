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

DIR = './dataviz'

if not os.path.exists(DIR):
    os.makedirs(DIR)


def get_color(size, source):
    if source == 'solaredge':
        if size < 10:
            return "lightgreen"
        if size < 100:
            return "green"
        if size < 1000:
            return "darkgreen"
    if source == 'solectria':
        if size < 10:
            return "pink"
        if size < 100:
            return "purple"
        if size < 1000:
            return "darkpurple"
        return 'red'
    return 'beige'


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
    for lat, lon, site_id, size, source in zip(df['latitude'], df['longitude'], df['site_id'], df['size'], df['source']):
        try:
            if size != size:  # if nan
                raise ValueError
            fg.add_child(folium.Marker(location=[lat, lon],
                                       popup=(folium.Popup(source + '\n' + str(site_id) + '\n size: ' + str(size))),
                                       icon=folium.Icon(color=get_color(size, source), icon_color='black')))
        except ValueError:
            print('Skipped: ', site_id)
    demo_map.add_child(fg)
    demo_map.save(DIR + '/sites_map.html')
