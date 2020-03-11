import pandas as pd

def get_address():
    "Return full address string from active_sites_data.csv"

    data = pd.read_csv('active_sites_data.csv')

    # change lattitude and longitude of nan to 0 (for later conditions):
    data[['latitude', 'longitude', 'postal']] = data[['latitude', 'longitude', 'postal']].fillna(0)

    # clean missing data and data type
    data[['line1', 'city', 'state']] = data[['line1', 'city', 'state']].fillna('')
    data[['line1', 'city', 'state', 'postal']] = data[['line1', 'city', 'state', 'postal']].astype(str)

    # change postal to standard form of number
    for i in range(len(data['postal'])):
        if data['postal'][i] == '0.0':
            data['postal'][i] = 'x'
        else:
            data['postal'][i] = str(data['postal'][i])[:-2]

    # concat everything together
    data['address'] = data['line1'] + ' ' + data['city'] + ', ' + data['state'] + ' ' + data['postal']
    data['address'] = data['address'].replace(' ,  x', 'none')

    return data

def get_coordinates_from_url(url):
    """
    Return latitude and longitude form a single url.
    """
    import requests
    import json
    # download JSON file and save it
    r = str(requests.get(url).content)
    f = open("temp.json", "w+")

    # replace problems in string
    x = r.replace('\\n', '\n')[2:-1]
    x = x.replace('\\', ' ')

    # write and close
    f.write(x)
    f.close()

    # open JSON file and load it
    with open('temp.json', 'r+') as json_file:
        json_data = json.load(json_file)
        json_file.truncate(0)

    # parse latitude and logitude data
    lat = json_data['results'][0]['geometry']['location']['lat']
    log = json_data['results'][0]['geometry']['location']['lng']

    return lat, log

def get_all_coordinates():
    """
    Get coordinates of all sites.
    Return DATAFRAME of all sites, along with modified coordinates columns
    """
    data = get_address()
    # Create URL using address made from above
    url = 'https://maps.googleapis.com/maps/api/geocode/json?address='
    for i in range(len(data)):
        # iterate through all data
        if data.iloc[i]['latitude'] == 0 and data.iloc[i]['longitude'] == 0:
            # if no coordinate exists
            if data.iloc[i]['address'] != 'none':
                # if we have address

                address = data.iloc[i]['address'].replace(' ', '+')

                # remove potential bad characters
                address = address.replace('#', '')

                # add api key
                address += '&key=YOUR_API_KEY'  # <-------- ADD YOUR API KEY HERE

                # get full url
                new_url = url + address

                # get coordinates
                try:
                    lat, log = get_coordinates_from_url(new_url)
                except IndexError:
                    pass
                data['latitude'][i] = float(lat)
                data['longitude'][i] = log
    return data