import urllib
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import pandas as pd
from bs4 import BeautifulSoup as soup


def scrape_apt_ad_page():
    df = pd.read_csv('apartments-evn.csv')
    urls = df['Apartments']

    data_list = []

    for url in urls[:1000]:
        try:
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            webpage = urlopen(req).read().decode('utf-8')
            page_soup = soup(webpage, "html.parser")
        except urllib.error.HTTPError:
            data_list.append([''*13])
            continue

        div_title = page_soup.find_all('div', {'class': 't'})  # titles of apartment descriptive information: e.g. construction type, floor area, number of rooms etc.
        div_value = page_soup.find_all('div', class_='i')  # values of titles above
        if page_soup.find('div', class_="loc") == None:
            address = ''
        else:
            address = page_soup.find('div', class_="loc").text
        if page_soup.find('span', class_='price') == None:
            price = ''
        else:
            price = page_soup.find('span', class_='price').text
        if page_soup.find('meta', {'itemprop': 'priceCurrency'}) == None:
            currency = 'unknown'
        else:
            currency = page_soup.find('meta', {'itemprop': 'priceCurrency'})['content']
        description = page_soup.find('div', class_='body').text

        data_dict = {div_title[i].text: div_value[i].text for i in range(len(div_title)) if div_title[i].text != 'Places Nearby'}
        data_dict['Address'] = address
        data_dict['Price'] = price
        data_dict['Currency'] = currency
        data_dict['Description'] = description

        data_list.append(list(data_dict.values()))

    return data_list, list(data_dict.keys())
