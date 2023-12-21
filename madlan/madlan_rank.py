import numpy as np
import sys
sys.path.append('C:/Users/yoavl/NextRoof/nadlan')
from nadlan.sql_reader_nadlan import read_from_nadlan_rank
from .sql_save_madlan import add_new_deals_madlan_rank
import pandas as pd
from .sql_reader_madlan import read_from_madlan_clean
import logging
import traceback
logging.basicConfig(level=logging.WARNING)
def rank(city):
    df_rank = read_from_nadlan_rank(city)
    df = read_from_madlan_clean(city)
    df = df.dropna(subset=['neighborhood'])


    df_rank_gush_helka = df_rank[['gush', 'helka', 'helka_rank']].drop_duplicates(subset=['gush', 'helka'])

    df_rank_street = df_rank[['street', 'street_rank']].drop_duplicates(subset=['street'])
    df_rank_neighborhood = df_rank[['neighborhood', 'neighborhood_rank']].drop_duplicates(subset=['neighborhood'])

    # Merge df with df_rank_gush_helka
    df_merged = pd.merge(df, df_rank_gush_helka, on=['gush', 'helka'], how='left')

    # Merge the result with df_rank_street
    df_merged = pd.merge(df_merged, df_rank_street, on=['street'], how='left')

    # Merge the result with df_rank_neighborhood
    df_merged = pd.merge(df_merged, df_rank_neighborhood, on=['neighborhood'], how='left')

    return df_merged


def update_floors_and_build_year_by_helka(df,city):
    df_nadlan = read_from_nadlan_rank(city)
    df_nadlan = df_nadlan[['gush', 'helka', 'floors', 'build_year']].drop_duplicates(subset=['gush', 'helka'])
    # Merge floors information
    df_merged = pd.merge(df, df_nadlan[['gush', 'helka', 'floors']], on=['gush', 'helka'], how='left',
                         suffixes=('', '_from_nadlan'))

    # Update 'floors' column in df only if it is NaN
    df_merged['floors'].fillna(df_merged['floors_from_nadlan'], inplace=True)
    df_merged.drop(columns=['floors_from_nadlan'], axis=1, inplace=True)

    # Merge build_year information separately
    df_merged = pd.merge(df_merged, df_nadlan[['gush', 'helka', 'build_year']], on=['gush', 'helka'], how='left',
                         suffixes=('', '_from_nadlan'))

    # Update 'build_year' column in df only if it is NaN
    df_merged['build_year'].fillna(df_merged['build_year_from_nadlan'], inplace=True)
    df_merged.drop(columns=['build_year_from_nadlan'], axis=1, inplace=True)

    return df_merged


def update_floors_and_build_year(df,city):
    df_nadlan = read_from_nadlan_rank(city)
    df_floors = df_nadlan[['street', 'home_number', 'neighborhood', 'floors']].drop_duplicates(
        subset=['street', 'home_number'])
    df_build_year = df_nadlan[['street', 'home_number', 'neighborhood', 'build_year']].drop_duplicates(
        subset=['street', 'home_number'])

    # Merging floors and build year information
    df_merged = pd.merge(df, df_floors, on=['street', 'home_number', 'neighborhood'], how='left')
    df_merged = pd.merge(df_merged, df_build_year, on=['street', 'home_number', 'neighborhood'], how='left',
                         suffixes=('', '_from_nadlan'))

    # Update build_year in df only if it is NaN
    df_merged.loc[df_merged['build_year'].isna(), 'build_year'] = df_merged['build_year_from_nadlan']

    # Drop the additional build_year column
    df_merged.drop(columns=['build_year_from_nadlan'], inplace=True)

    df_merged = update_floors_and_build_year_by_helka(df_merged,city)
    return df_merged


def fill_nan_by_avg(df, column,city):
    df_nadlan = read_from_nadlan_rank(city)

    avg_values = df_nadlan.groupby('street')[column].mean().reset_index()

    # Merge the average values with the original DataFrame where 'column' is NaN
    df_merged = pd.merge(df, avg_values, on='street', how='left', suffixes=('', '_avg'))

    # Update the NaN values in 'column' with the corresponding average values
    df_merged[column].fillna(df_merged[column + '_avg'], inplace=True)

    df_merged.drop(columns=[column + '_avg'], inplace=True)
    df_merged = df_merged.dropna(subset=[column])
    df_merged[column] = df_merged[column].astype(float).astype(np.int32)
    return df_merged


def main_madlan_ranking(city):
    status = {}
    try:
        df = rank(city)
        df = update_floors_and_build_year(df,city)
        df = fill_nan_by_avg(df, 'build_year',city)
        df = fill_nan_by_avg(df, 'floors',city)
        data = add_new_deals_madlan_rank(df)
        status['success'] = True
        status['new_rows'] = data['new_rows']
        status['updated_rows'] = data['updated_rows']

    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        status['success'] = False
        status['error'] = error_message
    return status