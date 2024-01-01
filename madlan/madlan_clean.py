import pandas as pd
import sys
import numpy as np
import math
sys.path.append('C:/Users/yoavl/NextRoof/utils')
from utils.apis import enrich_df_with_location_data
import traceback
from .sql_reader_madlan import read_from_madlan_raw
from .sql_save_madlan import add_new_deals_madlan_clean
from nadlan.sql_reader_nadlan import read_from_nadlan_clean
from tqdm import tqdm
from .madlan_utils import min_point
tqdm.pandas()
import logging
logging.basicConfig(level=logging.WARNING)



def find_nearest_neighborhood(df,city):
    df = df.dropna(subset=['x'])
    df.loc[:, 'neighborhood'] = df['neighborhood'].replace('', np.nan)
    df_no_neighborhood = df[df['neighborhood'].isna()].copy()
    df_neighborhood = df[df['neighborhood'].notna()]

    nearest_neighborhoods = []
    nadlan_clean = read_from_nadlan_clean(city)
    nadlan_clean['neighborhood'] = nadlan_clean['neighborhood'].replace('', np.nan)
    nadlan_clean = nadlan_clean.dropna(subset=['neighborhood'])
    for index, row in tqdm(df_no_neighborhood.iterrows(), total=df_no_neighborhood.shape[0]):
        x = row['x']
        y = row['y']
        nearest_neighborhood = min_point(x, y, nadlan_clean, target_distance=50)
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