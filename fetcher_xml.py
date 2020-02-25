import os
import time
import xml.etree.ElementTree as ET
import lxml
from datetime import datetime, timedelta
from helper import plot_days, time_batches
import requests
import pandas as pd

DIR = 'active_sites_xml'
URL = "http://solrenview.com/xmlfeed/ss-xmlN.php"
wait_time = 0  # seconds
last_fetch = 0

site_ids = [4760, 5582, 5077]


def get_params(site_id, start, end):
    return {'site_id': str(site_id), 'ts_start': start.isoformat(), 'ts_end': end.isoformat()}


def get_file_name(params):
    return DIR + "/" + "_".join(params.values()).replace(':', '=') + '.xml'


# start & end - datetime objects
def fetch(site_id, start, end):
    params = get_params(site_id, start, end)
    filename = get_file_name(params)
    print("Fetching: " + filename)
    if not os.path.exists(DIR):
        os.makedirs(DIR)
    if not os.path.exists(filename):  # or os.stat(filename).st_size == 0:  # if file does not exist or empty
        global last_fetch
        while time.time() - last_fetch < wait_time:
            time.sleep(0.1)
        last_fetch = time.time()
        r = requests.get(URL, params=params)
        raw = r.text
        with open(filename, 'w+') as f:
            f.write(raw)
    else:
        with open(filename, 'r') as f:
            raw = f.read()

    return raw


# returns datetimes to get stored active sites xml
def get_stored_datetimes():
    return datetime(2000, 1, 1), datetime(2020, 1, 1)


def get_hourly_production(site_id, start, end):
    production = []
    for st, sp in time_batches(start, end):
        production.append(fetch(site_id, st, sp))
    plot_days(production)
    print(production)


# fetches active sites metadata, stores it in a csv
def check_metadata(active_sites):
    sites_xmls = {}
    for site in active_sites:
        xml = fetch(site, *get_stored_datetimes())
        if xml:  # if xml is not empty
            sites_xmls[site] = ET.fromstring(xml)
    interest_tags = ['name', 'activationDate', 'latitude', 'longitude', 'line1', 'city', 'state', 'postal',
                     'timezone']
    df = pd.DataFrame(index=sites_xmls.keys(), columns=interest_tags)
    for i in sites_xmls:
        for tag in interest_tags:
            res = sites_xmls[i].find(".//" + tag).text
            if res:
                df[tag][i] = res.strip().replace('?', '')
            else:
                df[tag][i] = ""

    df.to_csv('active_sites_data.csv')
    return df


def get_active_sites(filename='active_sites.txt'):
    with open(filename, 'r') as f:
        sites = f.readlines()
        sites = [int(s.strip()) for s in sites]  # removing '\n' at the end
        return sites


def fetch_active_sites():
    sites = get_active_sites()
    for site in sites:
        while True:  # to try fetching until no error
            try:
                fetch(site, datetime(2000, 1, 1), datetime(2020, 1, 1))
            except requests.exceptions.ConnectionError:
                print('Interrupted')
                try:
                    os.remove(get_file_name(get_params(site, datetime(2000, 1, 1), datetime(2020, 1, 1))))
                except:
                    pass
                time.sleep(6.0)
                continue
            else:
                break


if __name__ == '__main__':
    check_metadata(get_active_sites())
