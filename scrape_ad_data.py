from urllib.request import Request, urlopen
import pandas as pd
from bs4 import BeautifulSoup as soup
from datetime import datetime
import os
import time
import random


def get_filenames(data_dir='data'):
    is_dir = os.path.isdir(data_dir)
    if not is_dir:
        print(f'\"{os.getcwd()}/{data_dir}\" directory does not exist, please specify correct data directory.')
        exit()
    filenames = []
    for root, dirs, files in os.walk(data_dir):
        for name in files:
            f = os.path.join(root, name)
            if f.endswith('data.csv'):
                continue
            filenames.append(f)

    return filenames


def scrape_apt_ad_page():
    files = get_filenames()
    for file in files:
        path_list = file.split('/')
        df = pd.read_csv(file)
        if len(df) == 0 or path_list[1] not in df.columns:
            continue
        try:
            df_ad_data = pd.read_csv(f'{file[:-4]}_data.csv')
            urls_data = df_ad_data['Links']
        except:
            df_ad_data = pd.DataFrame()
            urls_data = pd.Series(dtype=float)
        urls = df[path_list[1]]
        counter = len(urls)
        urls_temp = []
        for url_idx in range(len(urls)):
            urls[url_idx] = str(urls[url_idx])
            if urls[url_idx] in urls_data.tolist():
                continue
            elif urls[url_idx].endswith('price'):
                continue
            try:
                req = Request(urls[url_idx], headers={'User-Agent': 'Mozilla/5.0'})
                webpage = urlopen(req).read().decode('utf-8')
                page_soup = soup(webpage, "html.parser")
                if page_soup.find('span', class_='price') is None:  # delete url and skip if ad has no price provided
                    urls[url_idx] += ' no price'
                    continue
            except:
                df = df.drop([url_idx])
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
            datetime_now = datetime.now().strftime("%b-%d-%Y_%H-%M")

            data_dict = {div_title[i].text: div_value[i].text for i in range(len(div_title))
                         if div_title[i].text != 'Places Nearby'}
            data_dict['Address'] = address
            data_dict['Price'] = price
            data_dict['Currency'] = currency
            data_dict['Description'] = description
            data_dict['Datetime'] = datetime_now

            urls_temp.append(urls[url_idx])
            df_new_columns = pd.DataFrame([data_dict])
            df_ad_data = pd.concat([df_ad_data, df_new_columns], axis=0, ignore_index=True)

            counter -= 1
            print(f'{(1 - counter / len(urls)) * 100}% is done')
        df.loc[:, path_list[1]] = urls
        df.to_csv(file, index=False)
        df_ad_data.loc[len(df_ad_data)-len(urls_temp):, 'Links'] = urls_temp
        df_ad_data.to_csv(f'{file[:-4]}_data.csv', index=False)
        # time.sleep(random.randint(2, 4))
    print(f'done!')
    # return data_list, list(data_dict.keys())


if __name__ == '__main__':
    scrape_apt_ad_page()
