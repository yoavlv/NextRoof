import pandas as pd
import sys
import numpy as np
sys.path.append('C:/Users/yoavl/NextRoof/utils')
from utils.apis import get_gush_helka_api
import traceback
from .sql_reader_madlan import read_from_madlan_raw
from tqdm import tqdm
tqdm.pandas()
import logging
logging.basicConfig(level=logging.WARNING)
from nadlan.nadlan_utils import nominatim_api
from nadlan.sql_reader_nadlan import read_from_address_addr_cache
from utils.base import add_id_columns
from utils.utils_sql import DatabaseManager



def complete_data(df):
    df.dropna(subset=['street', 'city', 'home_number'], inplace=True)

    def update_row(row):
        city_id, street_id, home_number = int(row['city_id']), int(row['street_id']), int(row['home_number'])
        results = read_from_address_addr_cache(city_id, street_id, home_number)
        if results:
            for key in ['x', 'y', 'long', 'lat', 'type', 'gush', 'helka', 'floors', 'zip', 'build_year']:
                if key in results and pd.isna(row.get(key)):
                    row[key] = results.get(key)
        return row

    df = df.apply(update_row, axis=1)

    # Update gush, helka columns
    needs_gush_helka = df[pd.isna(df['gush']) | pd.isna(df['helka'])]
    updated_info = needs_gush_helka.apply(lambda row: get_gush_helka_api(row['city_id'], row['street_id'], row['home_number']), axis=1)

    for index, result in updated_info.items():
        if result:
            for key, value in result.items():
                df.at[index, key] = value

    df.dropna(subset=['gush', 'helka'], inplace=True)

    needs_x_y = df.loc[pd.isna(df['x']) | pd.isna(df['y'])]
    if not needs_x_y.empty:
        # df in none beacuse it's already search data in the dataframe
        df.loc[needs_x_y.index, 'details'] = needs_x_y.apply(lambda row: nominatim_api(row, df=None, save=False),axis=1)
        detail_keys = ['neighborhood', 'lat', 'long', 'x', 'y', 'zip']
        for key in detail_keys:
            df[key] = df['details'].apply(lambda x: x.get(key, np.nan) if isinstance(x, dict) else np.nan)

    if 'details' in df.columns:
        df.drop(columns=['details'], inplace=True)

    return df



def main_madlan_clean(city, city_id, local_host=False):
    status = {}
    try:
        df = read_from_madlan_raw(city_id)
        df = add_id_columns(df, 'street_id', 'street')

        df = complete_data(df)
        # df = find_nearest_neighborhood(df,city)
        df.replace({'NaN': np.nan, 'None': np.nan}, inplace=True)
        df = df.dropna(subset='gush')

        df['gush'] = df['gush'].astype(float).astype(np.int32)
        df['helka'] = df['helka'].astype(float).astype(np.int32)

        df['floors'] = df['floors'].astype(float).astype('Int32')
        df['home_number'] = df['home_number'].astype(float).astype('Int32')
        df['build_year'] = df['build_year'].astype(float).astype('Int32')

        new_rows = 0
        updated_rows = 0
        success = False
        if not df.empty:
            if local_host:
                db_manager = DatabaseManager(table_name='madlan_clean', db_name='nadlan_db', host_name='localhost')
                success, new_rows, updated_rows = db_manager.insert_dataframe(df, 'item_id')

            db_manager = DatabaseManager(table_name='madlan_clean', db_name='nextroof_db')
            success, new_rows, updated_rows = db_manager.insert_dataframe_batch(df, batch_size=int(df.shape[0]),
                                                                                replace=True, pk_columns='item_id')
        status['success'] = success
        status['new_rows'] = new_rows
        status['updated_rows'] = updated_rows
        print(f"{city}\nstatus {status}")
    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        status['success'] = False
        status['error'] = error_message
    return status