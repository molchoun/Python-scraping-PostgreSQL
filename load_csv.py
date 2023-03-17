# imports
import os
import pandas as pd
import re
import numpy as np
from config import config
from db import DB
from sqlalchemy import create_engine
from sqlalchemy import types
import clean_data

db = DB()
conn, cur = db.conn, db.cur
params = config()
conn_string = f"postgresql://{params['user']}:{params['password']}@{params['host']}/{params['database']}"
engine = create_engine('postgresql+psycopg2://postgres:SecurePas$1@localhost/real_estate')


# get all files full paths in data folder
def get_files(data_dir='data'):
    is_dir = os.path.isdir(data_dir)
    if not is_dir:
        print(f'\"{os.getcwd()}/{data_dir}\" directory does not exist, please specify correct data directory.')
        exit()
    files = []
    for root, dirs, filenames in os.walk(data_dir):
        for name in filenames:
            f = os.path.join(root, name)
            if f.endswith('data.csv'):
                files.append(f)
            else:
                continue
    return files



# populate list by list of files grouped by property type
def files_list():
    files = sorted(get_files())
    df_list = []
    i_temp = 0
    tmp_list = []
    for i in range(len(files) - 1):
        current_name = files[i_temp].split('/')[-1].split('_')[0]
        next_name = files[i + 1].split('/')[-1].split('_')[0]
        if current_name == 'Rooms for rent':
            df_list.append(files[i_temp:])
            break
        elif current_name == next_name:
            tmp_list.append(files[i + 1])
        else:
            tmp_list.insert(0, files[i_temp])
            df_list.append(tmp_list)
            tmp_list = []
            i_temp = i + 1
    return df_list


# concatenate csv files that are same property types
def df_concat(files_list):
    df_merged = pd.DataFrame()
    to_change = {"Yerevan": 1, "Armavir": 2, "Ararat": 3, "Kotayk": 4, "Shirak": 5, "Lorri": 6, "Gegharkunik": 7,
                 "Syunik": 8, "Aragatsotn": 9, "Tavush": 10, "Vayots Dzor": 11, "Artsakh": 12, "International": 13}
    for file in files_list:
        region = to_change[file.split('/')[-2]]
        try:
            df_to_merge = pd.read_csv(file)
            if len(df_to_merge) < 1:
                continue
        except:
            continue
        df_to_merge['reg_id'] = region
        frames = [df_merged, df_to_merge]
        df_merged = pd.concat(frames, ignore_index=True)
    return df_merged


def missing_values(df):
    percent_missing = df.isnull().sum() * 100 / len(df)
    missing_value_df = pd.DataFrame({'percent_missing': percent_missing}, index=None)
    return missing_value_df


def df_to_sql():
    files = files_list()
    for file in files:
        p_type = file[0].split('/')[-1].split('_')[0].lower().strip().replace(' ', '_')
        cur.execute('SELECT id FROM property_type WHERE name = %s', (p_type,))
        cat_id = cur.fetchone()[0]

        df = df_concat(file)
        df = clean_data.fix_displacement(df)
        df['Price'] = df['Price'].apply(clean_data.clean_currency)
        if df['Price'].str.contains('daily').any():
            df = clean_data.split_price_col(df)
        clean_data.rename_cols(df)
        df = clean_data.drop_cols(df)
        df.drop_duplicates('links', inplace=True)
        clean_data.clean_df(df)

        # df.to_sql(p_type, con=engine, index_label='id', if_exists='replace', dtype={'new_construction': types.INT,
        #                                                                            'elevator': types.INT,
        #                                                                            'floors_in_the_building': types.INT,
        #                                                                            'number_of_rooms': types.INT,
        #                                                                            'number_of_bathrooms': types.INT,
        #                                                                            'floor': types.INT,
        #                                                                            'reg_id': types.INT,
        #                                                                            'datetime': types.DATE(),
        #                                                                            'children_are_welcome': types.INT,
        #                                                                            'pets_allowed': types.INT,
        #                                                                            'utility_payments': types.INT,
        #                                                                            'land_area': types.INT
        #                                                                            })

        df['cat_id'] = cat_id
        df['retrieved'] = 1
        df.rename(columns={'links': 'url'}, inplace=True)
        df[['url', 'cat_id', 'reg_id', 'retrieved']].to_sql('urls', con=engine, if_exists='append', index=False,
                                                              dtype={'url': types.VARCHAR(255),
                                                                     'cat_id': types.INT,
                                                                     'reg_id': types.INT,
                                                                     'retrieved': types.INT})

        # with engine.connect() as con:
        #     con.execute(f'ALTER TABLE {p_type} ADD CONSTRAINT pk_{p_type} PRIMARY KEY (id);')
        #     con.execute(f'ALTER TABLE {p_type} ADD CONSTRAINT fk_reg_id FOREIGN KEY(reg_id) REFERENCES regions(id);')
        #     con.execute(f'ALTER TABLE {p_type} ADD CONSTRAINT fk_url_id FOREIGN KEY(links) REFERENCES urls(id);')
        #     con.execute(f'ALTER TABLE {p_type} ADD CONSTRAINT {p_type}_links UNIQUE(links);')

    with engine.connect() as con:
        con.execute('ALTER TABLE urls ADD id SERIAL PRIMARY KEY;')
        con.execute('ALTER TABLE urls ADD CONSTRAINT fk_category_id FOREIGN KEY(cat_id) REFERENCES property_type(id);')
        con.execute('ALTER TABLE urls ADD CONSTRAINT fk_region_id FOREIGN KEY(reg_id) REFERENCES regions(id);')
        con.execute('ALTER TABLE urls RENAME COLUMN links TO url;')
        con.execute('DELETE FROM urls a USING urls b WHERE a.id > b.id AND a.url = b.url;')
        con.execute('ALTER TABLE urls ADD CONSTRAINT url_unique UNIQUE (url);')
        con.execute('ALTER TABLE urls ALTER COLUMN retrieved SET DEFAULT 0;')

if __name__ == '__main__':
    df_to_sql()
