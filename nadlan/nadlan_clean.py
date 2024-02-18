# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from .nadlan_utils import nominatim_api, complete_neighborhood
from .sql_reader_nadlan import read_raw_data_table, read_from_nadlan_clean, distinct_city_list\
    , read_from_nadlan_rank_find_floor, fetch_all_cache_data_by_city
from utils.base import add_id_columns
from .sql_save_nadlan import add_new_deals_nadlan_clean, add_new_deals_nadlan_clean_neighborhood_complete
from tqdm import tqdm
import traceback
from utils.utils_sql import DatabaseManager



# def main_add_street_id(df):
#     city_ids = df['city_id'].unique()
#     result_df = pd.DataFrame()
#     for city_id in city_ids:
#         df_temp = df[df['city_id'] == city_id]
#         st_df_temp = st_df[st_df['city_id'] == city_id]
#         merged_df = add_street_id(df_temp, st_df_temp)
#
#         result_df = pd.concat([result_df, merged_df], ignore_index=True)
#
#     result_df = result_df.dropna(subset=['street_id'])
#     result_df.loc[:, 'street_id'] = result_df.loc[:, 'street_id'].astype(np.int32)
#
#     return result_df.drop(columns=['city_id_st', 'city_st'])

floors = {-1: 'מרתף', 0: 'קרקע', 1: 'ראשונה', 2: 'שניה', 3: 'שלישית', 4: 'רביעית', 5: 'חמישית', 6: 'שישית', 7: 'שביעית', 8: 'שמינית', 9: 'תשיעית', 10: 'עשירית', 11: 'אחת עשרה', 12: 'שתים עשרה', 13: 'שלוש עשרה', 14: 'ארבע עשרה', 15: 'חמש עשרה', 16: 'שש עשרה', 17: 'שבע עשרה', 18: 'שמונה עשרה', 19: 'תשע עשרה', 20: 'עשרים', 21: 'עשרים ואחת', 22: 'עשרים ושתים', 23: 'עשרים ושלוש', 24: 'עשרים וארבע', 25: 'עשרים וחמש', 26: 'עשרים ושש', 27: 'עשרים ושבע', 28: 'עשרים ושמונה', 29: 'עשרים ותשע', 30: 'שלושים', 31: 'שלושים ואחת', 32: 'שלושים ושתים', 33: 'שלושים ושלוש', 34: 'שלושים וארבע', 35: 'שלושים וחמש', 36: 'שלושים ושש', 37: 'שלושים ושבע', 38: 'שלושים ושמונה', 39: 'שלושים ותשע', 40: 'ארבעים'}

def floor_to_numeric(df, floors):
    df = df.dropna(subset=['floor']).copy()
    replacements = {"יי": "י", "קומה": ""}
    df.loc[:, 'floor'] = df['floor'].replace(replacements, regex=True)

    # Enhance the floors dictionary
    floor_dict = {v: k for k, v in floors.items()}
    floor_dict.update({'א': 1, 'ב': 2, 'ג': 3, 'ד': 4})

    # Function to match floor names to numbers
    def match_floor_name(floor_name):
        if floor_name in floor_dict:
            return floor_dict[floor_name]
        split_floor = floor_name.split()
        return floor_dict.get(split_floor[0], np.nan) if split_floor else np.nan

    # Apply conversion to each row
    df['floor'] = df['floor'].apply(match_floor_name)

    df = df.dropna(subset= 'floor')
    df['floor'] = df['floor'].astype(np.int32)
    return df

# tqdm.pandas(desc="Enriching location Data")
def enrich_df_with_location_data(df, city_id):
    temp_df = fetch_all_cache_data_by_city(city_id)
    df['details'] = df.apply(lambda row: nominatim_api(row, temp_df, save=True), axis=1)

    # df['details'] = df.progress_apply(lambda row: nominatim_api(row, temp_df, save=True), axis=1)

    df['neighborhood'] = df['details'].apply(lambda x: x.get('neighborhood', np.nan) if x else np.nan)
    df['lat'] = df['details'].apply(lambda x: x.get('lat', np.nan) if x else np.nan)
    df['long'] = df['details'].apply(lambda x: x.get('long', np.nan) if x else np.nan)
    df['x'] = df['details'].apply(lambda x: x.get('x', np.nan) if x else np.nan)
    df['y'] = df['details'].apply(lambda x: x.get('y', np.nan) if x else np.nan)
    df['zip'] = df['details'].apply(lambda x: x.get('zip', np.nan) if x else np.nan)
    df.drop(columns=['details'], inplace=True)
    return df.dropna(subset=['x'])

def find_missing_floors(df):
    def update_floor(row):
        if pd.isna(row['floors']):
            street =row['street'].replace("'",'')
            return read_from_nadlan_rank_find_floor(row['city'], street, row['home_number'])
        else:
            return row['floors']

    df['floors'] = df.apply(update_floor, axis=1)
    df = df.dropna(subset=['floors'])
    df = df[df['floors'].apply(lambda x: isinstance(x, (int, np.integer)) or (isinstance(x, float) and x.is_integer()))]
    df['floors'] = df['floors'].astype(np.int32)
    df['floors'] = np.where(df['floor'] > df['floors'], df['floor'], df['floors'])
    return df


def clean_outliers(df):
    df['PPM'] = (df["price"] / df['size']).astype(np.int32)
    columns = ['PPM']
    for col in columns:
        q1, q3 = np.percentile(df[col], [25, 75])
        iqr = q3 - q1
        lower_bound = q1 - (1.5 * iqr)
        upper_bound = q3 + (1.5 * iqr)
        df = df[(df[col] > lower_bound) & (df[col] < upper_bound)]
    df = df.drop(columns=['PPM'])
    return df


def columns_strip_df(df):
    for col in df.columns:
        try:
            df[col] = df[col].str.strip()
        except:
            pass
    return df

def maintenance_neighborhood(table):
    big_df = pd.DataFrame()
    city_list = distinct_city_list(table)
    for city in city_list:
        df = read_from_nadlan_clean(city)
        df = complete_neighborhood(df)

        big_df = pd.concat([big_df, df], ignore_index=True)

    add_new_deals_nadlan_clean_neighborhood_complete(big_df)
    return big_df

def run_nadlan_clean(city_id, city):
    maintance = False
    nadlan_clean_status = {}
    try:
        df = read_raw_data_table(num_of_rows=120000, city_id=city_id)
        print(f"(run_nadlan_clean) start shape : {df.shape} , city: {city}")
        df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y')
        df = add_id_columns(df, 'street_id', 'street')
        df = floor_to_numeric(df, floors)
        df = columns_strip_df(df)
        df = enrich_df_with_location_data(df, city_id)
        if maintance:
            df = find_missing_floors(df)
            maintenance_neighborhood('nadlan_clean')
        else:
            df['floors'] = df['floors'].replace(['NaN', np.nan, ''], None)
            df = df.dropna(subset=['floors'])
            df['floors'] = df['floors'].astype(float).astype(int)

        # df = clean_outliers(df)
        df['new'].fillna(0, inplace=True)
        df['new'] = df['new'].astype(np.int32)
        # data = add_new_deals_nadlan_clean(df)
        db_manager = DatabaseManager('nextroof_db', 'localhost', 'nadlan_clean')
        success, new_rows, conflict_rows = db_manager.insert_dataframe(df, 'key')

        # data2 = add_new_deals_nadlan_clean(df,'13.50.98.191')

        nadlan_clean_status['success'] = success
        nadlan_clean_status['new_rows'] = new_rows
        nadlan_clean_status['conflict_rows'] = conflict_rows
        print(f"Status:(run_nadlan_clean){city} {db_manager}")
    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        print(error_message)
        nadlan_clean_status['success'] = False
        nadlan_clean_status['error'] = error_message

    return nadlan_clean_status

