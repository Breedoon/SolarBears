import pandas as pd
data = pd.read_csv('../active_sites_data.csv')
data = data['site_id']

from bs4 import BeautifulSoup
from selenium import webdriver
import time
URL = 'https://solrenview.com/SolrenView/mainFr.php?siteId=' #website
list_url = []
for i in data:
    url = URL + str(i)
    list_url.append(url)


driver = webdriver.Firefox(executable_path='geckodriver.exe')


def get_info(url):
    driver.get(url)
    driver.switch_to.frame((driver.find_element_by_id('childFrame')))  # switch to inner iframe
    driver.find_element_by_id('2').click()  # click on Project Details
    driver.switch_to.frame((driver.find_element_by_id('frame2')))
    t = time.time()
    while len(driver.find_elements_by_class_name(
            'asd')) == 0 and time.time() - t < 2:  # wait until loads or time out in 2 seconds
        time.sleep(0.1)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = BeautifulSoup(str(soup.findAll('table', {'cellspacing': '10px'})), "html.parser")  # get table
    val = table.findAll('td')[1].text  # get size
    return val[:-6]

ans = []
for url in list_url:
    try:
        x = get_info(url)
        ans.append(x)
    except:
        x = None
        ans.append(x)

d = {'site': data, 'size': ans}
df = pd.DataFrame(d)
df.to_csv('site_size.csv')