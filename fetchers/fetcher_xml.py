import os
import time
import xml.etree.ElementTree as ET
from misc.helper import plot_days, time_batches
import requests
import pandas as pd

DIR = './solectria_raw_xml'
URL = "http://solrenview.com/xmlfeed/ss-xmlN.php"
wait_time = 0  # seconds
last_fetch = 0
metadata_tags = ['name', 'activationDate', 'latitude', 'longitude', 'line1', 'city', 'state', 'postal', 'timezone']

site_ids = [4760, 5582, 5077]


def get_params(site_id, start, end):
    if start:  # if None
        return {'site_id': str(site_id), 'ts_start': start.isoformat(), 'ts_end': end.isoformat()}
    else:
        return {'site_id': str(site_id)}


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
        if 'Invalid site id' in raw or 'Invalid XMLfeed request' in raw or 'Unknown or bad timezone' in raw or len(raw) == 0:
            return ''
        with open(filename, 'w+') as f:
            try:
                f.write(raw)
            except UnicodeEncodeError:
                return fetch(site_id, start, end)
    else:
        with open(filename, 'r') as f:
            raw = f.read()

    return raw


# returns datetimes to get stored active solectria_sites xml
def get_stored_datetimes():
    return None, None


def get_hourly_production(site_id, start, end):
    production = []
    for st, sp in time_batches(start, end):
        production.append(fetch(site_id, st, sp))
    plot_days(production)
    print(production)


def get_site_metadata(site_id):
    raw = fetch(site_id, *get_stored_datetimes())
    if not raw:  # if xml is empty
        return None
    try:
        xml = ET.fromstring(raw)
    except ET.ParseError:
        print(site_id)
        return None

    vals = {}
    for tag in metadata_tags:
        res = xml.find(".//" + tag).text
        if res:
            vals[tag] = res.strip().replace('?', '')
        else:
            vals[tag] = ""
    return vals


# fetches active solectria_sites metadata, stores it in a csv
def get_active_sites_metadata(active_sites):
    df = pd.DataFrame(columns=metadata_tags)

    for site in active_sites:
        data = get_site_metadata(site)
        if not data:  # if None
            continue
        df = df.append(pd.Series(data, name=site))
    df.to_csv('active_sites_data.csv', index_label='site_id')
    return df


def get_active_sites(filename='active_sites.txt'):
    with open(filename, 'r') as f:
        sites = f.readlines()
        sites = [int(s.strip()) for s in sites]  # removing '\t' at the end
        return sites


def fetch_active_sites(sites):
    # solectria_sites = get_active_sites()
    for site in sites:
        while True:  # to try fetching until no error
            try:
                fetch(site, *get_stored_datetimes())
            except requests.exceptions.ConnectionError:
                print('Interrupted')
                try:
                    os.remove(get_file_name(get_params(site, *get_stored_datetimes())))
                except:
                    pass
                time.sleep(6.0)
                continue
            else:
                break


if __name__ == '__main__':
    get_active_sites_metadata(get_active_sites())

