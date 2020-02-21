import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from helper import plot_days, time_batches
import requests

DIR = 'active_sites_xml'
URL = "http://solrenview.com/xmlfeed/ss-xmlN.php"
wait_time = 1  # seconds
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
    if not os.path.exists(filename):
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

    try:
        root = ET.fromstring(raw)
    except:
        return 0
    try:
        val = root.find('sunspecData').find('d').find('m').findall('p')[1].text  # Power of first inverter
    except:
        print('error')  # temporarily for fetching all active sites
        return 0
    return 0 if val == "null" else float(val)


def get_hourly_production(site_id, start, end):
    production = []
    for st, sp in time_batches(start, end):
        production.append(fetch(site_id, st, sp))
    plot_days(production)
    print(production)


def get_active_sites(filename='active_sites.txt'):
    with open(filename, 'r') as f:
        sites = f.readlines()
        for site in sites:
            while True:  # to try fetching until no error
                try:
                    fetch(site.strip(), datetime(2000, 1, 1), datetime(2020, 1, 1))
                except requests.exceptions.ConnectionError:
                    print('Interrupted')
                    try:
                        os.remove(get_file_name(get_params(site.strip(), datetime(2000, 1, 1), datetime(2020, 1, 1))))
                    except:
                        pass
                    time.sleep(6.0)
                    continue
                else:
                    break


if __name__ == '__main__':
    # get_hourly_production(site_ids[0], datetime(2019, 1, 1), datetime(2019, 2, 1))
    get_active_sites()
