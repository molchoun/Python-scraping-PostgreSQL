import re
import urllib
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from bs4 import BeautifulSoup as soup
import time


URL_HOME = 'https://www.list.am/en'


def page_soup(url: str):
    """Parameter `url`: web address of page to parse.
    In case of failure prints error, otherwise returns `BeautifulSoup` object: parsed html document (str),
    """
    try:
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        webpage = urlopen(req).read().decode('utf-8')
        pg_soup = soup(webpage, "html.parser")
    except urllib.error.HTTPError as errh:
        print(f'Error during fetching of website:')
        return errh
    except urllib.error.URLError as erru:
        print(f'Error during fetching of website:')
        return erru
    else:
        return pg_soup


def get_categories():
    """Returns all categories' names and paths in a dictionary.
    e.g. {'Apartments for sale': '/category/60', 'Houses for rent: '/category/63', ...} """

    url = 'https://www.list.am/en/category/54'  # arbitrary category url to fetch all categories paths
    pg_soup = page_soup(url)
    section_cat = pg_soup.select('div.s')
    categories_dict = dict()
    for cat in section_cat:
        tmp = cat.next.lower().strip()
        if tmp == 'for rent':
            for elem in cat.select('a'):
                categories_dict[elem.text.strip().replace(' ', '_').lower() + '_for_rent'] = elem['href']
        elif tmp == 'for sale':
            for elem in cat.select('a'):
                categories_dict[elem.text.strip().replace(' ', '_').lower() + '_for_sale'] = elem['href']
        elif tmp == 'new construction':
            for elem in cat.select('a'):
                categories_dict[elem.text.strip().replace(' ', '_').lower() + '_new_construction'] = elem['href']

    return categories_dict


def get_regions():
    """
    Returns all region names and query string in a dictionary.
    e.g. {'Yerevan': '?n=1', 'Armavir': '?n=23', ...}
    """
    url = 'https://www.list.am/en/category/'  # url to fetch all region paths
    pg_soup = page_soup(url)

    # select all `divs` that contain region names
    data_searchname = pg_soup.find_all('div', {'class': 'i', 'data-name': re.compile('^[A-z]')})  # data-name - to
    # filter out city names
    loc_dict = dict()
    for data in data_searchname[1:len(data_searchname) - 1]:  # slicing to exclude option 'All'
        if data['data-name'] not in loc_dict:
            loc_dict[data['data-name']] = '?n=' + data['data-value']
    return loc_dict


def get_ad_links(url: str, key_cat: str, key_loc: str):
    a_tags = []
    p = 1
    print(f'{key_cat} category for {key_loc} region')
    while True:  # loop through each page of pagination
        pg_soup = page_soup(url)
        # append list with `a` tags containing path to ad, e.g /en/item/16954298
        a_tags.extend(pg_soup.find_all("a", attrs={"href": re.compile('/en/item/')}))
        # locate the `Next` button
        next_page_element = pg_soup.find('a', text='Next >')
        if next_page_element:
            next_page_url = next_page_element.get('href')
            url = URL_HOME + next_page_url
        else:
            break
        # time.sleep(random.randint(2, 5))
        print(f'Page {p} is done')
        p += 1

    print('Done')
    return a_tags
