import urllib
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import pandas as pd
from bs4 import BeautifulSoup as soup
import os


def scrape_apt_ad_page(directory, filename):

    files = Path(directory).glob('*')

    # for file in files:
    #     f = file
    #     file = os.path.join(file, filename+'.csv')
    #     df = pd.read_csv(file)
    #     urls = df[filename]
    #
    #     data_list = []
    df = pd.read_csv('data/Apartments for sale/Yerevan/Apartments for sale.csv')
    urls = df['Apartments for sale']
    data_list = []
    counter = len(urls)
    for url in urls:
        try:
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            webpage = urlopen(req).read().decode('utf-8')
            page_soup = soup(webpage, "html.parser")
        except urllib.error.HTTPError or ValueError:
            data_list.append(['' ])
            continue

        div_title = page_soup.find_all('div', {'class': 't'})  # titles of apartment descriptive information: e.g. construction type, floor area, number of rooms etc.
        div_value = page_soup.find_all('div', class_='i')  # values of titles above
        if page_soup.find('div', class_="loc") is None:
            address = ''
        else:
            address = page_soup.find('div', class_="loc").text
        if page_soup.find('span', class_='price') is None:
            price = ''
        else:
            price = page_soup.find('span', class_='price').text
        if page_soup.find('meta', {'itemprop': 'priceCurrency'}) is None:
            currency = 'unknown'
        else:
            currency = page_soup.find('meta', {'itemprop': 'priceCurrency'})['content']
        description = page_soup.find('div', class_='body').text

        # data_dict = {div_title[i].text: div_value[i].text for i in range(len(div_title)) if
        #              div_title[i].text != 'Places Nearby'}

        # data_dict['Address'] = address
        # data_dict['Price'] = price
        # data_dict['Currency'] = currency
        # data_dict['Description'] = description

        # data_list.append(list(data_dict.values()))
        columns = [div_title[i].text for i in range(len(div_title)) if div_title[i].text != 'Places Nearby'] + \
                  ['Address' , 'Price', 'Currency', 'Description']

        values = [div_value[i].text for i in range(len(div_title)) if div_title[i].text != 'Places Nearby'] + \
                 [address, price, currency, description]
        data_list.append(values)
        counter -= 1
        print(f'{(1 - counter/len(urls))*100}% is done')


    df_links_apt = pd.DataFrame(data_list, columns=columns)
    df_links_apt['Links'] = urls
    df_links_apt.to_csv('data/Apartments for sale/Yerevan/Apartments for sale_data.csv', index=False)
    print(f'done!')
    # return data_list, list(data_dict.keys())

if __name__ == '__main__':
    directory = 'data\Apartments for sale'
    filename = 'Apartments for sale'
    # directory = 'data\Apartments for rent'
    # filename = 'Apartments for rent'
    scrape_apt_ad_page(directory, filename)

# def scrape_ad_page(db, col_name, col_count):
#     df = pd.read_csv(db)
#     urls = df[col_name]
#
#     data_list = []
#
#     for url in urls:
#         try:
#             req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
#             webpage = urlopen(req).read().decode('utf-8')
#             page_soup = soup(webpage, "html.parser")
#         except urllib.error.HTTPError:
#             data_list.append([''*col_count])
#             continue
#
#         div_title = page_soup.find_all('div', {'class': 't'})  # titles of apartment descriptive information: e.g. construction type, floor area, number of rooms etc.
#         div_value = page_soup.find_all('div', class_='i')  # values of titles above
#         if page_soup.find('div', class_="loc") == None:
#             address = ''
#         else:
#             address = page_soup.find('div', class_="loc").text
#         if page_soup.find('span', class_='price') == None:
#             price = ''
#         else:
#             price = page_soup.find('span', class_='price').text
#         if page_soup.find('meta', {'itemprop': 'priceCurrency'}) == None:
#             currency = 'unknown'
#         else:
#             currency = page_soup.find('meta', {'itemprop': 'priceCurrency'})['content']
#         description = page_soup.find('div', class_='body').text
#
#         data_dict = {div_title[i].text: div_value[i].text for i in range(len(div_title)) if div_title[i].text != 'Places Nearby'}
#         data_dict['Address'] = address
#         data_dict['Price'] = price
#         data_dict['Currency'] = currency
#         data_dict['Description'] = description
#
#         data_list.append(list(data_dict.values()))
#
#     return data_list, list(data_dict.keys())

