import numpy as np
import sys
sys.path.append('C:/Users/yoavl/NextRoof/nadlan')
from nadlan.sql_reader_nadlan import read_from_nadlan_rank
from .sql_save_madlan import add_new_deals_madlan_rank
import pandas as pd
from .sql_reader_madlan import read_from_madlan_clean
import traceback
from utils.utils_sql import DatabaseManager


def rank(city_id):
    df_rank = read_from_nadlan_rank(city_id)
    df = read_from_madlan_clean(city_id)

    df_rank_gush_helka = df_rank[['gush', 'helka', 'helka_rank']].drop_duplicates(subset=['gush', 'helka'])
    df_rank_street = df_rank[['street_id', 'street_rank']].drop_duplicates(subset=['street_id'])
    df_rank_neighborhood = df_rank[['gush', 'gush_rank']].drop_duplicates(subset=['gush'])

    df_merged = pd.merge(df, df_rank_gush_helka, on=['gush', 'helka'], how='left')
    df_merged = pd.merge(df_merged, df_rank_street, on=['street_id'], how='left')
    df_merged = pd.merge(df_merged, df_rank_neighborhood, on=['gush'], how='left')

    return df_merged


def update_floors_and_build_year(df, city_id):
    df_nadlan = read_from_nadlan_rank(city_id)
    # Prepare nadlan data for merging
    df_nadlan_unique = df_nadlan[['gush', 'helka', 'floors', 'build_year']].drop_duplicates(subset=['gush', 'helka'])
    df_floors_general = df_nadlan[['street_id', 'home_number', 'gush', 'floors']].drop_duplicates(
        subset=['street_id', 'home_number'])
    df_build_year_general = df_nadlan[['street_id', 'home_number', 'gush', 'build_year']].drop_duplicates(
        subset=['street_id', 'home_number'])

    # First, merge based on 'gush' and 'helka' for more specific data
    df_merged = df.copy()
    for column in ['floors', 'build_year']:
        df_merged = pd.merge(df_merged, df_nadlan_unique[['gush', 'helka', column]], on=['gush', 'helka'], how='left',
                             suffixes=('', '_from_nadlan_specific'))
        mask = df_merged[column].isna()
        df_merged.loc[mask, column] = df_merged.loc[mask, f'{column}_from_nadlan_specific']
        df_merged.drop(columns=[f'{column}_from_nadlan_specific'], inplace=True)

    # Then, update using general data where still missing
    for column, df_nadlan_subset in [('floors', df_floors_general), ('build_year', df_build_year_general)]:
        df_merged = pd.merge(df_merged, df_nadlan_subset, on=['street_id', 'home_number', 'gush'], how='left',
                             suffixes=('', '_from_nadlan_general'))
        mask = df_merged[column].isna()
        df_merged.loc[mask, column] = df_merged.loc[mask, f'{column}_from_nadlan_general']
        df_merged.drop(columns=[f'{column}_from_nadlan_general'], inplace=True)

    return df_merged


def fill_nan_by_avg(df, column, city_id):
    df_nadlan = read_from_nadlan_rank(city_id)

    avg_values = df_nadlan.groupby('street_id')[column].mean().reset_index()

    # Merge the average values with the original DataFrame where 'column' is NaN
    df_merged = pd.merge(df, avg_values, on='street_id', how='left', suffixes=('', '_avg'))

    # Update the NaN values in 'column' with the corresponding average values
    df_merged[column].fillna(df_merged[column + '_avg'], inplace=True)

    df_merged.drop(columns=[column + '_avg'], inplace=True)
    df_merged = df_merged.dropna(subset=[column])
    df_merged[column] = df_merged[column].astype(float).astype(np.int32)
    return df_merged


def main_madlan_ranking(city_id,city):
    status = {}
    try:
        df = rank(city_id)
        df = update_floors_and_build_year(df,city_id)
        df = fill_nan_by_avg(df, 'build_year',city_id)
        df = fill_nan_by_avg(df, 'floors',city_id)
        db_manager = DatabaseManager('nextroof_db', 'localhost', 'madlan_rank')
        success, new_rows, updated_rows = db_manager.insert_dataframe(df, 'item_id')
        # data2 = add_new_deals_madlan_rank(df,'13.50.98.191')
        status['success'] = success
        status['new_rows'] = new_rows
        status['updated_rows'] = updated_rows

    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        status['success'] = False
        status['error'] = error_message

    return status