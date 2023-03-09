from urllib.error import HTTPError
from urllib.request import Request, urlopen
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import clean_data
from sqlalchemy import create_engine
from config import config
import io
import os
import time
import random
import psycopg2
from db import DB
import numpy as np
import re
import psycopg2
from psycopg2 import sql


params = config()
conn_string = f"postgresql://{params['user']}:{params['password']}@{params['host']}/{params['database']}"
engine = create_engine('postgresql+psycopg2://postgres:SecurePas$1@localhost/real_estate')


def clean_currency(x):
    """ If the value is a string, then remove currency symbol and delimiters
    otherwise, the value is numeric and can be converted
    """
    # df_merged['Price'] = df_merged['Price'].replace({'\$': '', '֏': '', '₽': '', '€': '', ',': ''}, regex=True)
    if isinstance(x, str):
        return x.replace('$', '').replace('֏', '').replace('₽', '').replace('€', '').replace(',', '')

 # def clean_data(key, tmp=''):
#
#     clean_dict = {
#         'new_construction': tmp.replace('No', '0').replace('Yes', '1'),
#         'elevator': tmp.replace('Not available', '0').replace('Available', '1'),
#         'floors_in_the_building': tmp.replace('+', ''),
#         'floor_area': re.search(r'\d+', tmp),
#         'land_area': re.search(r'\d+', tmp),
#         'room_area': re.search(r'\d+', tmp),
#         'number_of_rooms': tmp.replace('+', ''),
#         'number_of_bathrooms': tmp.replace('+', ''),
#         'ceiling_height': re.search(r'(\d+(?:\.\d+)?)', tmp).group(0),
#         'floor': tmp.replace('+', ''),
#         'children_are_welcome': tmp.replace('No', '10').replace('Yes', '11').replace('Negotiable', '12'),
#         'pets_allowed': tmp.replace('No', '10').replace('Yes', '11').replace('Negotiable', '12'),
#         'utility_payments': tmp.replace('Not included', '10').replace('Included', '11').replace('By Agreement', '12')
#     }
#     try:
#         return clean_dict[key]
#     except:
#         return False


def del_cols(data_dict):
    to_del = ['prepayment', 'number_of_guests', 'lease_type', 'minimum_rental_period',
              'noise_after_hours', 'mortgage_is_possible', 'handover_date', 'window_views']
    for col in to_del:
        if col in data_dict:
            del data_dict[col]
    return data_dict


def create_table_if_not_exists(db, table_name, field_names):
    fields = (("id", "SERIAL PRIMARY KEY"),
              ("links", "INTEGER REFERENCES urls(id) UNIQUE"),
              ("reg_id", "INTEGER REFERENCES regions(id)"),
              ("price", "BIGINT"),
              ("datetime", "DATE"))

    for col in field_names:
        if isinstance(col, int):
            col_type = "INTEGER"
        elif col == 'datetime':
            col_type = "DATE"
        elif col == 'price':
            col_type = "BIGINT"
        else:
            col_type = "TEXT"
        _t = (col, col_type)
        fields += (_t,)
    db.create_table(table_name, fields)


def fetch_urls(cursor):
    cursor.execute('''SELECT urls.url, urls.reg_id, p.name, urls.id
                   FROM urls
                   INNER JOIN property_type p
                   ON p.id = urls.cat_id
                   WHERE urls.retrieved = 0 and p.name IN ('houses_new_construction', 'apartments_new_construction');''')


def no_price(web_element, url_id):
    if web_element is None:  # delete url and skip if ad has no price provided
        with DB().cur as cur_:
            cur_.execute(f'UPDATE urls SET retrieved = -1 WHERE id = %s;', (url_id,))
            conn.commit()

        return True


def http_error(url_id):
    with DB().cur as cur_:
        cur_.execute(f'UPDATE urls SET retrieved = -2 WHERE id = %s;', (url_id,))
        conn.commit()



def soup(url, url_id):
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read().decode('utf-8')
    page_soup = BeautifulSoup(webpage, "html.parser")
    return page_soup


def url_count():
    # cur.execute('''SELECT COUNT(id) FROM urls WHERE retrieved = 0;''')
    cur.execute('''SELECT COUNT(id) FROM urls WHERE retrieved = 0 and cat_id IN (14, 15);''')
    len_urls = cur.fetchone()[0]
    return len_urls


def url_set_retrieved(url_id):
    with DB().cur as cur_:
        cur_.execute(f'UPDATE urls SET retrieved = 1 WHERE id = %s;', (url_id,))
        conn.commit()


def scrape_apt_ad_page(data_dict=None):
    len_urls = url_count()
    fetch_urls(cur)

    # tbl_dict = dict() # table comparison dict method
    # old_len = len(tbl_dict) + 1 # table comparison dict method
    # i = 1 # table comparison dict method

    df = pd.DataFrame()
    while len_urls:
        len_urls -= 1
        url, reg_id, table_name, url_id = cur.fetchone()
        with DB().cur as cur_:
            cur_.execute('''SELECT urls.url, urls.reg_id, p.name, urls.id
                               FROM urls 
                               INNER JOIN property_type p
                               ON p.id = urls.cat_id 
                               WHERE urls.retrieved = 0 and p.name IN ('houses_new_construction', 'apartments_new_construction')
                               OFFSET 1;''')
            new_table_name = cur_.fetchone()[2]

        # tbl_dict[table_name] = i # table comparison dict method
        # new_len = len(tbl_dict) # table comparison dict method
        # if old_len != new_len: # table comparison dict method
        #     tbl_name = list(tbl_dict.keys())[list(tbl_dict.values()).index(i)] # table comparison dict method

        if table_name != new_table_name:
            clean_data.drop_cols(df)
            df['price'] = df['price'].apply(clean_data.clean_currency)
            if df['price'].str.contains('daily').any():
                df = clean_data.split_price_col(df)
            clean_data.clean_df(df)
            if not db.check_table(table_name):
                df.head(0).to_sql(table_name, engine, if_exists='replace', index=False)
            output = io.StringIO()
            df.to_csv(output, sep='\t', header=False, index=False)
            output.seek(0)
            contents = output.getvalue()
            cur.copy_from(output, table_name, null="")
            conn.commit()
            # create_table_if_not_exists(db, table_name, data_dict.keys())

            df = pd.DataFrame()
            # i += 1 # table comparison dict method
            # old_len = new_len # table comparison dict method

        try:
            page_soup = soup(url, url_id)
            price_elem = page_soup.find('span', class_='price')
        except HTTPError:
            http_error(url_id)
            continue
        if no_price(price_elem, url_id):
            continue

        # titles of apartment descriptive information: e.g. construction type, floor area, number of rooms etc.
        div_title = page_soup.find_all('div', {'class': 't'})
        div_value = page_soup.find_all('div', class_='i')  # values of titles
        data_dict = {div_title[i].text.strip().replace(' ', '_').lower(): div_value[i].text
                     for i in range(len(div_title)) if div_title[i].text != 'Places Nearby'}

        if page_soup.find('div', class_="loc") is None:
            address = ''
        elif page_soup.find('meta', {'itemprop': 'priceCurrency'}) is None:
            currency = 'unknown'
        else:
            currency = page_soup.find('meta', {'itemprop': 'priceCurrency'})['content']
            address = page_soup.find('div', class_="loc").text
        price = page_soup.find('span', class_='price').text
        datetime_now = datetime.now().strftime("%Y-%m-%d")

        data_dict['address'] = address
        data_dict['currency'] = currency
        data_dict['datetime'] = datetime_now
        data_dict['price'] = price
        data_dict['url_id'] = url_id
        data_dict['reg_id'] = reg_id


        df = pd.concat([df, pd.DataFrame.from_records([data_dict])])
        url_set_retrieved(url_id)
        conn.commit()



        # CLEAN WITH pandas
        # --------------------------------------------------------------------------------------------------
        # for i in range(len(div_value)):
        #     div_value[i] = div_value[i].text.strip().replace(' ', '_').lower()
        #     try:
        #         div_value[i] = int(div_value[i].replace('not_included', '0').replace('by_agreement', '2').replace('not_available', '0')
        #                            .replace('yes', '1').replace('no', '0').replace('available', '1')
        #                            .replace('+', '').replace('negotiable', '2').replace('included', '1'))
        #     except ValueError:
        #         if 'sq.m.' in div_value[i]:
        #             div_value[i] = int(re.search(r'\d*[?:\.\d*]', div_value[i]).group())
        #         elif 'from' in div_value[i]:
        #             div_value[i] = float(re.search(r'(\d+(?:\.\d+)?)', div_value[i]).group())
        #         continue
        # try:
        #     price = int(clean_currency(price))
        # except:
        #     price, duration = clean_currency(price).split()
        #     data_dict['duration'] = duration
        # data_dict['price'] = int(price)
        # data_dict = del_cols(data_dict)
        # ---------------------------------------------------------------------------------------------------
        #
        #
        # cur.execute(f"SELECT MAX(id) FROM {table_name};")
        # _id = cur.fetchone()
        # if _id is not None:
        #     table_id = _id[0] + 1
        # else:
        #     table_id = 0
        #
        # cur.execute(sql.SQL("INSERT INTO {table} (id, links, reg_id) VALUES (%s, %s, %s) "
        #                     "ON CONFLICT (links) DO NOTHING;")
        #             .format(table=sql.Identifier(table_name)),
        #             (table_id, url_id, reg_id))
        # conn.commit()
        # for key in data_dict:
        #     cur.execute('''SELECT EXISTS (SELECT column_name
        #                    FROM information_schema.columns
        #                    WHERE table_name = %s and column_name = %s);''', (table_name, key))
        #     if not cur.fetchone()[0]:
        #         cur.execute(sql.SQL("ALTER TABLE {table} ADD COLUMN {column} text;").
        #                     format(table=sql.Identifier(table_name), column=sql.Identifier(key)))
        #         conn.commit()
        #
        #     table_id = cur.execute(sql.SQL(f"SELECT id FROM {table_name} WHERE url = %s")
        #                            .format(table=sql.Identifier(table_name)),
        #                            (url_id, ))
        #     cur.execute(sql.SQL("UPDATE {table} SET {field} = (%s) WHERE id = %s;")
        #                 .format(table=sql.Identifier(table_name), field=sql.Identifier(key)),
        #                 (data_dict[key], table_id))
        # cur.execute("UPDATE urls SET retrieved = 1 WHERE id = %s", (url_id, ))
        # conn.commit()
    print('')
if __name__ == '__main__':
    db = DB()
    conn, cur = db.conn, db.cur
    scrape_apt_ad_page()
