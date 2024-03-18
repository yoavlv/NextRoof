# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from nadlan.nadlan_utils import nominatim_api
from nadlan.sql_reader_nadlan import read_raw_data_table, read_from_nadlan_rank_find_floor, fetch_all_cache_data_by_city, read_floors_to_dict
from utils.base import add_id_columns , complete_lat_long
import traceback
from utils.utils_sql import DatabaseManager
from utils.base import find_most_similar_word
import re

def clean_floor_name(floor_name: str) -> str:
    """
    Cleans the floor_name string by removing non-digit characters
    and leading/trailing whitespace.
    """
    cleaned_name = re.sub(r'[^\d\w]+', '', floor_name)
    return cleaned_name.strip()
def floor_to_numeric(df: pd.DataFrame) -> pd.DataFrame:
    floor_dict = read_floors_to_dict()
    df['floor'] = df['floor'].str.replace('קומה', '', regex=False).str.strip()
    for i in range(0, 41):
        floor_dict[str(i)] = str(i)

    df = df.dropna(subset=['floor']).copy()

    def match_floor_name(floor_name: str):
        direct_match = floor_dict.get(floor_name)
        if direct_match is not None:
            return direct_match
        split_floor = floor_name.split()
        if split_floor:
            split_match = floor_dict.get(split_floor[0])
            if split_match is not None:
                return split_match

        most_similar = find_most_similar_word(list(floor_dict.keys()), floor_name)
        if most_similar is not None:
            return floor_dict.get(most_similar, np.nan)

        cleaned_name = clean_floor_name(floor_name)
        most_similar_cleaned = find_most_similar_word(list(floor_dict.keys()), cleaned_name)
        return floor_dict.get(most_similar_cleaned, np.nan)

    df['floor'] = df['floor'].apply(match_floor_name)
    df = df.dropna(subset=['floor'])

    df['floor'] = df['floor'].astype(np.int32)
    return df


# tqdm.pandas(desc="Enriching location Data")
def enrich_df_with_location_data(df: pd.DataFrame, city_id:int)->pd.DataFrame:
    temp_df = fetch_all_cache_data_by_city(city_id)
    df['details'] = df.apply(lambda row: nominatim_api(row, temp_df, save=True), axis=1)

    # df['details'] = df.progress_apply(lambda row: nominatim_api(row, temp_df, save=True), axis=1)
    df['neighborhood'] = df['details'].apply(lambda x: x.get('neighborhood', np.nan) if x else np.nan)
    df['x'] = df['details'].apply(lambda x: x.get('x', np.nan) if x else np.nan)
    df['y'] = df['details'].apply(lambda x: x.get('y', np.nan) if x else np.nan)
    df['zip'] = df['details'].apply(lambda x: x.get('zip', np.nan) if x else np.nan)
    df.drop(columns=['details'], inplace=True)
    df = df.dropna(subset=['x'])
    df = complete_lat_long(df)
    return df

def find_missing_floors(df: pd.DataFrame)-> pd.DataFrame:
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



def columns_strip_df(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip()
    return df



def run_nadlan_clean(city_id: int, city: str, local_host=False)-> dict:
    maintance = False
    nadlan_clean_status = {}
    try:
        df = read_raw_data_table(num_of_rows=120000, city_id=city_id)
        if not df.empty:
            print(f"(run_nadlan_clean) start shape : {df.shape} , city: {city}")
            df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y')
            df = add_id_columns(df, 'street_id', 'street')
            df = floor_to_numeric(df)
            df = columns_strip_df(df)
            df = enrich_df_with_location_data(df, city_id)
            if maintance:
                df = find_missing_floors(df)
            else:
                df['floors'] = df['floors'].replace(['NaN', np.nan, ''], None)
                df = df.dropna(subset=['floors'])
                df['floors'] = df['floors'].astype(float).astype(int)

            df['new'].fillna(0, inplace=True)
            df['new'] = df['new'].astype(np.int32)
            new_rows = 0
            conflict_rows = 0
            if not df.empty:
                if local_host:
                    db_manager = DatabaseManager(table_name='nadlan_clean', db_name='nextroof_db', host_name='localhost')
                    success, new_rows, conflict_rows = db_manager.insert_dataframe(df, 'key')


                db_manager = DatabaseManager(table_name='nadlan_clean', db_name='nextroof_db')
                success, new_rows, conflict_rows = db_manager.insert_dataframe_batch(df, batch_size=int(df.shape[0]), replace=True, pk_columns='key')

            nadlan_clean_status['success'] = True
            nadlan_clean_status['new_rows'] = new_rows
            nadlan_clean_status['conflict_rows'] = conflict_rows
            print(f"Status:(run_nadlan_clean){city} new_rows:{new_rows}, conflict_rows:{conflict_rows}")
        else:
            nadlan_clean_status['success'] = True
            nadlan_clean_status['new_rows'] = 0
            nadlan_clean_status['conflict_rows'] = 0
            print(f"Status:(run_nadlan_clean){city} No new deals")

    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        print(error_message)
        nadlan_clean_status['success'] = False
        nadlan_clean_status['error'] = error_message

    return nadlan_clean_status

