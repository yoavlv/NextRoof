import pandas as pd
import sys
import numpy as np
import math
sys.path.append('C:/Users/yoavl/NextRoof/utils')
from utils.apis import enrich_df_with_location_data
import traceback
from .sql_reader_madlan import read_from_madlan_raw ,read_addr_table
from .sql_save_madlan import add_new_deals_madlan_clean
from tqdm import tqdm
tqdm.pandas()
import logging
logging.basicConfig(level=logging.WARNING)

def min_point(x, y, df):
    distance = 10000
    neighborhood = None
    for index, row in df.iterrows():
        df_x = row['x']
        df_y = row['y']
        df_neighborhood = row['neighborhood']
        distance_check = math.sqrt(((df_x - x) ** 2) + ((df_y - y) ** 2))
        if distance_check < distance:
            if distance_check < 70:
                return df_neighborhood
            distance = distance_check
            neighborhood = df_neighborhood

    return neighborhood


def find_nearest_neighborhood(df,city):
    df = df.dropna(subset=['x'])
    df_no_neighborhood = df[df['neighborhood'].isna()].copy()
    df_neighborhood = df[df['neighborhood'].notna()]

    nearest_neighborhoods = []
    addr_table = read_addr_table(city)
    for index, row in tqdm(df_no_neighborhood.iterrows(), total=df_no_neighborhood.shape[0]):
        x = row['x']
        y = row['y']
        nearest_neighborhood = min_point(x, y, addr_table)
        nearest_neighborhoods.append(nearest_neighborhood)

    df_no_neighborhood['neighborhood'] = nearest_neighborhoods

    df = pd.concat([df_no_neighborhood, df_neighborhood])
    df = df.dropna(subset=['neighborhood'])
    return df

def main_madlan_clean(city):
    status = {}
    try:
        df = read_from_madlan_raw(city)
        df = enrich_df_with_location_data(df,False)
        df = find_nearest_neighborhood(df,city)
        df.replace({'NaN': np.nan, 'None': np.nan}, inplace=True)
        df = df.dropna(subset='gush')
        df['gush'] = df['gush'].astype(float).astype(np.int32)
        df['helka'] = df['helka'].astype(float).astype(np.int32)
        data = add_new_deals_madlan_clean(df)
        status['success'] = True
        status['new_rows'] = data['new_rows']
        status['updated_rows'] = data['updated_rows']

    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        status['success'] = False
        status['error'] = error_message
    return status