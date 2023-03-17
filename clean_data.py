# imports
import pandas as pd
import re
import numpy as np


# rename columns: make lowercase, replace 'space' with '_'
def rename_cols(df):
    cols = []
    for col in df.columns.to_list():
        col = col.strip().replace(' ', '_').lower()
        cols.append(col)
    # df.rename(columns={'links': 'url'}, inplace=True)
    df.columns = cols


# fix data displacement occurred while scraping
def fix_displacement(df):
    df_fix = df[~df['Links'].str.startswith('https', na=False)].loc[:, 'Datetime':]
    links = []
    date = []

    for row in df_fix.astype(str).values:
        l = len(row)
        for i in range(len(row)):
            if row[i].startswith('https'):
                links.append(row[i])
                if len(links) > len(date):
                    date.append(tmp_date)
                break
            if re.match(r'^[A-Z][a-z]{2}-\d+-\d+_\d+-\d+', row[i]):
                tmp_date = row[i]
                date.append(row[i])

    df.loc[~df['Links'].str.startswith('https', na=False), 'Datetime'] = date
    df.loc[~df['Links'].str.startswith('https', na=False), 'Links'] = links
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    return df


def clean_currency(x):
    """ If the value is a string, then remove currency symbol and delimiters
    otherwise, the value is numeric and can be converted
    """
    # df_merged['Price'] = df_merged['Price'].replace({'\$': '', '֏': '', '₽': '', '€': '', ',': ''}, regex=True)
    if isinstance(x, str):
        return x.replace('$', '').replace('֏', '').replace('₽', '').replace('€', '').replace(',', '')


# split 'Price' column (e.g. 10000 monthly) to 2 columns and reorder df columns
def split_price_col(df):
    df[['Price', 'Duration']] = df.Price.str.split(expand=True)
    df['Price'] = df.loc[:, 'Price'].astype(int)
    cols = df.columns.to_list()
    idx_currency = cols.index('Currency')
    idx_duration = cols.index('Duration')
    cols.insert(idx_currency + 1, 'Duration')
    cols.pop()
    df = df[cols]
    return df


def clean_df(df):
    df_cols = df.columns
    df['datetime'] = pd.to_datetime(df['datetime'].str.split('_').str[0], dayfirst=True, format='%b-%d-%Y', infer_datetime_format=True)
    for col in ['floor_area', 'land_area', 'room_area']:
        if col in df_cols:
            df[col] = df[col].str.extract('(\d+)').astype(int)
            # df[col] = df[col].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)
    for col in ['floors_in_the_building', 'number_of_rooms', 'number_of_bathrooms', 'floor']:
        if col in df_cols:
            df[col] = df[col].replace({'\+': ''}, regex=True).fillna(0).astype(int)
    for col in ['children_are_welcome', 'pets_allowed']:
        if col in df_cols:
            df[col] = df[col].replace({'No': 10, 'Yes': 11, 'Negotiable': 12}, regex=True).fillna(0).astype(int)
    if 'ceiling_height' in df_cols:
        df['ceiling_height'] = df['ceiling_height'].fillna(0)
        df['ceiling_height'] = df['ceiling_height'].str.extract('(\d+(?:\.\d+)?)').astype(float)
        # df['ceiling_height'] = df['ceiling_height'].astype('str').str.extractall('(\d+(?:\.\d+)?)').unstack().fillna('').sum(axis=1).astype(float)
    if 'utility_payments' in df_cols:
        df['utility_payments'] = df['utility_payments'].replace(
            {'Not included': 10, 'Included': 11, 'By Agreement': 12}, regex=True).fillna(0).astype(int)
    if 'new_construction' in df_cols:
        df['new_construction'] = pd.Series(np.where(df['new_construction'].values == 'Yes', 1, 0), df.index, dtype=int)
    if 'elevator' in df_cols:
        df['elevator'] = pd.Series(np.where(df['elevator'].values == 'Available', 1, 0), df.index, dtype=int)


def drop_cols(df):
    df = df.dropna(thresh=int(df.shape[0] * 0.2), axis=1)
    df_cols = df.columns.to_list()
    cols = ['description', 'prepayment', 'number_of_guests', 'lease_type', 'minimum_rental_period', 'noise_after_hours',
            'mortgage_is_possible', 'handover_date', 'the_house_has', 'parking']
    for col in cols:
        if col in df_cols:
            df = df.drop(col, axis=1)

    return df


