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

    # val = root.find('sunspecData').find('d').find('m').findall('p')[1].text  # Power of first inverter

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


# check missing values in sites' metadata and returns a dict
# e.g., {site_id: {'name': 0, ...'}}, 0 - no value for that feature
def check_metadata(active_sites):
    sites_xmls = {}
    for site in active_sites:
        xml = fetch(site, *get_stored_datetimes())
        if xml:  # if xml is not empty
            sites_xmls[site] = ET.fromstring(xml)
    interest_tags = ['name', 'activationDate', 'latitude', 'latitude', 'longitude', 'line1', 'city', 'state', 'postal',
                     'timezone']
    results = {site: {tag: 0 for tag in interest_tags} for site in sites_xmls.keys()}
    for i in sites_xmls:
        for tag in interest_tags:
            try:
                found_tag = sites_xmls[i].find(".//" + tag)
                if found_tag.text.strip().replace('?', '') or len(found_tag.findall('.//')) == 0:  # if tag is not emtpy
                    results[i][tag] = 1
            except:
                continue
    count_empty = lambda x: len([i for i in x[1] if x[1][i] == 0])  # returns number of zeros in for a site
    results = {k: v for k, v in sorted(results.items(), key=count_empty, reverse=True)}
    max_empty = [count_empty(i) for i in results.items()]
    emtpy_nums = {i: max_empty.count(i) for i in set(max_empty)}  # e.g., {2: 695, ...} - 695 sites with 2 features missing
    freq = dict(pd.DataFrame(results.values()).sum())  # e.g., {'name': 1076, ...} - 1076 sites don't have a 'name'
    # freq = {'name': 1076, 'activationDate': 1126, 'latitude': 379, 'longitude': 379, 'line1': 1121, 'city': 1122, 'state': 1122, 'postal': 1085, 'timezone': 1126}
    # emtpy_nums: {0: 335, 1: 43, 2: 695, 3: 50, 5: 3}
    return results


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
    # get_hourly_production(site_ids[0], datetime(2019, 1, 1), datetime(2019, 2, 1))
    check_metadata(get_active_sites())
