from urllib.request import Request, urlopen
import pandas as pd
from bs4 import BeautifulSoup as soup
from datetime import datetime
import os
import time
import random
from db import DB

def clean_currency(x):
    """ If the value is a string, then remove currency symbol and delimiters
    otherwise, the value is numeric and can be converted
    """
    # df_merged['Price'] = df_merged['Price'].replace({'\$': '', '֏': '', '₽': '', '€': '', ',': ''}, regex=True)
    if isinstance(x, str):
        return x.replace('$', '').replace('֏', '').replace('₽', '').replace('€', '').replace(',', '')


def scrape_apt_ad_page():
    db = DB()
    conn, cur = db.conn, db.cur

    while True:
        cur.execute('SELECT url, reg_id FROM urls WHERE retrieved = 0;')
        url, reg_id = cur.fetchone()

        try:
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            webpage = urlopen(req).read().decode('utf-8')
            page_soup = soup(webpage, "html.parser")
            if page_soup.find('span', class_='price') is None:  # delete url and skip if ad has no price provided
                cur.execute(f'UPDATE urls SET retrieved = -1 WHERE url = {url};')
                conn.commit()
                continue
        except:
            cur.execute(f'UPDATE urls SET retrieved = -2 WHERE url = {url};')
            conn.commit()
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
        datetime_now = datetime.now().strftime("%Y-%m-%d")

        data_dict = {div_title[i].text.strip().replace(' ', '_').lower(): div_value[i].text
                     for i in range(len(div_title)) if div_title[i].text != 'Places Nearby'}
        data_dict['address'] = address
        data_dict['currency'] = currency
        data_dict['datetime'] = datetime_now
        try:
            price = int(clean_currency(price))
        except:
            price, duration = clean_currency(price).split()
            data_dict['duration'] = duration
        data_dict['price'] = int(price)

        del_cols = cols = ['prepayment', 'number_of_guests', 'lease_type', 'minimum_rental_period',
                           'noise_after_hours', 'mortgage_is_possible', 'handover_date', 'window_views']
        clean_cols = ['new_construction']


if __name__ == '__main__':
    scrape_apt_ad_page()
