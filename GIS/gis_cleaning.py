import pandas as pd
import numpy as np
from sql_gis import read_distinct_cities_id ,read_city_from_nadlan_raw ,save_into_db_city_rank


def calculate_city_price_per_year(df, years):
    rank_dict = {}
    for year in years:
        temp_df = df[df['year'] == year]
        total_price = round(temp_df['price'].sum())
        total_size = round(temp_df['size'].sum())
        avg = total_price / total_size
        rank_dict[year] = int(avg)
    return rank_dict

def rank_city_by_year():
    distinct_city_id_list = read_distinct_cities_id()
    for city_id in distinct_city_id_list:
        df = read_city_from_nadlan_raw(city_id)
        years = list(df['year'].unique())
        rank_dict = calculate_city_price_per_year(df,years)
        rank = rank_dict.values()
        data = {
            'year': years,
            'rank': rank,
            'city_id': city_id,
        }
        df = pd.DataFrame(data)
        df = df.sort_values('year')
        save_into_db_city_rank(df)

def main_gis_clean():
    rank_city_by_year()

rank_city_by_year()
