import urllib
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import pandas as pd
from bs4 import BeautifulSoup as soup
import os


def scrape_apt_ad_page(directory, filename):
    files = Path(directory).glob('*')

    for file in files:
        f = file
        file = os.path.join(file, filename+'.csv')
        df = pd.read_csv(file)
        urls = df['Apartments for sale']
        df_ad_data = pd.DataFrame()
        counter = len(urls)

        for url_idx in range(len(urls)):
            try:
                req = Request(urls[url_idx], headers={'User-Agent': 'Mozilla/5.0'})
                webpage = urlopen(req).read().decode('utf-8')
                page_soup = soup(webpage, "html.parser")
                if page_soup.find('span', class_='price') is None:  # delete url and skip if ad has no price provided
                    urls.drop(index=url_idx, inplace=True)
                    continue
            except urllib.error.HTTPError or ValueError:
                urls.drop(index=url_idx, inplace=True)
                continue

            div_title = page_soup.find_all('div', {'class': 't'})  # titles of apartment descriptive information: e.g.
            # construction type, floor area, number of rooms etc.
            div_value = page_soup.find_all('div', class_='i')  # values of titles
            if page_soup.find('div', class_="loc") is None:
                address = ''
            elif page_soup.find('meta', {'itemprop': 'priceCurrency'}) is None:
                currency = 'unknown'
            else:
                currency = page_soup.find('meta', {'itemprop': 'priceCurrency'})['content']
                address = page_soup.find('div', class_="loc").text
            price = page_soup.find('span', class_='price').text
            description = page_soup.find('div', class_='body').text

            data_dict = {div_title[i].text: div_value[i].text for i in range(len(div_title))
                         if div_title[i].text != 'Places Nearby'}
            data_dict['Address'] = address
            data_dict['Price'] = price
            data_dict['Currency'] = currency
            data_dict['Description'] = description

            df_new_columns = pd.DataFrame([data_dict])
            df_ad_data = pd.concat([df_ad_data, df_new_columns], axis=0, ignore_index=True)

            counter -= 1
            print(f'{(1 - counter / len(urls)) * 100}% is done')

    df_ad_data['Links'] = urls
    df_ad_data.to_csv('data/Apartments for sale/Artsakh/Apartments for sale_data1.csv', index=False)
    print(f'done!')
    # return data_list, list(data_dict.keys())


if __name__ == '__main__':
    directory = 'data'
    filename = 'Apartments for sale'
    # directory = 'data\Apartments for rent'
    # filename = 'Apartments for rent'
    scrape_apt_ad_page(directory, filename)
