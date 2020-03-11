def get_coordinates(line1, city, state, postal):
    # change type to string
    line1 = str(line1)
    city = str(city)
    state = str(state)
    postal = str(postal)

    # if all are empty
    if line1 == city == state == postal == '':
        return None, None

    else:
        url = 'https://maps.googleapis.com/maps/api/geocode/json?address='
        # create address
        address = line1 + ' ' + city + ', ' + state + ' ' + postal
        # clean and format address
        address = address.replace(' ', '+')
        address = address.replace('#', '')
        address += '&key=YOUR_API_KEY'  # <-------- ADD API KEY HERE

        # get url
        url = url + address
        # try to get coordinates
        try:
            lat, log = get_coordinates_from_url(url)
        except:
            return None, None

        # return coordinates
        return lat, log


def get_coordinates_from_url(url):
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