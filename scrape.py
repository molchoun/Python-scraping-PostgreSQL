import re
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup as soup
import time
import pandas as pd
from typing import List
from pathlib import Path
from datetime import datetime

from numpy import random


def page_soup(url):
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read().decode('utf-8')
    pg_soup = soup(webpage, "html.parser")
    return pg_soup


def get_category_url():
    url = 'https://www.list.am/en/category/54'
    pg_soup = page_soup(url)
    section_cat = pg_soup.select('div.s')
    categories_dict = dict()
    # Fill dictionary with categories names and links to respective pages:
    # e.g 'Apartments for sale': '/category/60'
    for cat in section_cat[2].select('a'):
        categories_dict[cat.text + ' for sale'] = cat['href']
    for cat in section_cat[3].select('a'):
        categories_dict[cat.text + ' for rent'] = cat['href']

    return categories_dict


def get_location():
    """
    Returns dictionary: key being location name and value - query string
    """
    url = 'https://www.list.am/en/category/'
    pg_soup = page_soup(url)

    # select all `divs` that contain region names
    data_searchname = pg_soup.find_all('div', {'class': 'i', 'data-name': re.compile('^[A-z]')}) # data-name - to
    # select only regions, without cities
    loc_dict = dict()
    for data in data_searchname[1:len(data_searchname)-1]: # slicing to exclude option 'All'
        if data['data-name'] not in loc_dict:
            loc_dict[data['data-name']] = '?n=' + data['data-value']
    return loc_dict


def get_ad_links(url: str, path: str, key_cat: str, key_loc: str):
    try:
        df = pd.read_csv(path)
        links = df[key_cat].to_dict()
        dt_string = df['Datetime'].to_dict()
    except:
        links = dict()
        dt_string = dict()
    a_tags = []
    p = 1
    print(f'{key_cat} category for {key_loc} region')
    while True: # loop through each page of pagination
        pg_soup = page_soup(url)
        # append list with `a` tags containing link to ad, e.g /en/item/16954298
        a_tags.extend(pg_soup.select('div.gl a'))
        # locate the `Next` button
        next_page_element = pg_soup.find('a', text='Next >')
        if next_page_element:
            next_page_url = next_page_element.get('href')
            url = URL_HOME + next_page_url
        else:
            break
        time.sleep(random.randint(2, 5))
        print(f'Page {p} is done')
        p += 1

    for a in a_tags:
        full_url = 'https://list.am' + a['href']
        if full_url not in links.values():
            links.update({len(links):full_url})
            dt_string.update({len(links): datetime.now().strftime("%b-%d-%Y_%H-%M")})
    print('Done')
    return links.values(), dt_string.values()


URL_HOME = 'https://www.list.am/en'

if __name__ == '__main__':
    # For each category pages (Apartments, Houses, Lands, etc.) go over every region (Yerevan, Armavir, Ararat, etc.)
    for key_cat in get_category_url():

        for key_loc in get_location():
            url = URL_HOME + get_category_url()[key_cat] + get_location()[key_loc]
            path_to_file = Path(f'data/{key_cat}/{key_loc}/{key_cat}.csv')
            path_to_folder = Path(f'data/{key_cat}/{key_loc}')
            if not path_to_folder.exists():
                path_to_folder.mkdir(parents=True, exist_ok=True)
            ad_links = get_ad_links(url, path_to_file, key_cat, key_loc)
            dict_apt = {
                        key_cat: ad_links[0],
                       'Datetime': ad_links[1]
                        }
            df_links_apt = pd.DataFrame(dict_apt)
            # df_links_apt = pd.DataFrame({key: pd.Series(value) for key, value in dict_apt.items() })
            df_links_apt.to_csv(path_to_file, index=False)



# url = URL_HOME + get_category_url()['Apartments for sale'] + get_location()['Yerevan']
# dict_apt = {'Apartments': get_ad_links(url)}
# df_links_apt = pd.DataFrame(dict_apt)
# df_links_apt.to_csv('data/apartments-evn-links.csv')

# dict_houses = {'Houses': get_ad_links('https://www.list.am/category/62/', 88)}
# df_links_houses = pd.DataFrame(dict_houses)
# df_links_houses.to_csv('data/houses-evn-links.csv')

