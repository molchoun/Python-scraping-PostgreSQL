import re
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup as soup
import time
import pandas as pd
from typing import List


URL_HOME = 'https://www.list.am/en'


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
    Returns dictionary: key being location name and value - query value
    """
    url = 'https://www.list.am/en/category/'
    pg_soup = page_soup(url)

    # select all `divs` that contain region names
    data_searchname = pg_soup.find_all('div', {'class': 'i', 'data-name': re.compile('^[A-z]')}) # data-name helps to
    # select only regions, without cities
    loc_dict = dict()
    for data in data_searchname:
        if data['data-name'] not in loc_dict:
            loc_dict[data['data-name']] = '?n=' + data['data-value']
    return loc_dict


def get_ad_links(url: str) -> List[str]:
    links = []
    a_tags = []

    while True: # code to handle pagination
        pg_soup = page_soup(url)
        # append list with `a` tags containing link to ad, e.g /en/item/16954298
        a_tags.extend(pg_soup.select('div.gl a'))
        # locate the `Next` button
        next_page_element = pg_soup.find('a', text='Next >')
        if next_page_element:
            next_page_url = next_page_element.get('href')
            url = 'https://www.list.am/en/' + next_page_url
            print('')
        else:
            break

    for a in a_tags:
        full_url = 'https://list.am/en' + a['href']
        if full_url not in links:
            links.append(full_url)

    return links



