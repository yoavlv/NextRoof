import sys
sys.path.append('C:/Users/yoavl/NextRoof/')
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")
import difflib
from nadlan.sql_reader_nadlan import read_from_cities_table, read_from_streets_table
import pandas as pd
import numpy as np
import math

def find_most_similar_word(word_list, target_word):
    if target_word != None:
        target_word = target_word.decode('utf-8') if isinstance(target_word, bytes) else target_word
        matches = difflib.get_close_matches(target_word, word_list, n=1, cutoff=0.67)
        return matches[0] if matches else None
    return None

def add_id_columns(df, id_column, name_column):
    if name_column == 'street':
        city_id = df['city_id'].iloc[0]
        help_df = read_from_streets_table(city_id,reverse=True)
    else:
        if 'city_id' in df.columns:
            df = df.drop('city_id', axis=1)
        help_df = read_from_cities_table()

    merged_df = df.merge(help_df, on=name_column, how='left', suffixes=('', f'_{id_column}'))
    word_list = list(help_df[name_column])

    for index, row in merged_df.iterrows():
        if pd.isna(row[id_column]):
            target_word = row[name_column]
            closest_word = find_most_similar_word(word_list, target_word)

            if closest_word is not None:
                id_value = help_df[help_df[name_column] == closest_word][id_column].values[0]
                merged_df.at[index, id_column] = id_value

    merged_df = merged_df.dropna(subset=[id_column])
    merged_df[id_column] = merged_df[id_column].astype(np.int32)
    merged_df = merged_df.filter(regex=f'^(?!.*_{id_column}$)')
    return merged_df



def complete_lat_long(df: pd.DataFrame) -> pd.DataFrame:
    def convert_coordinates(df: pd.DataFrame):
        return [itm_to_wgs84(float(x), float(y)) for x, y in zip(df['x'], df['y'])]
    lat_long_list = convert_coordinates(df)
    lats, longs = zip(*lat_long_list)  # This unzips the list of tuples into two tuples

    df.loc[:, 'lat'] = lats
    df.loc[:, 'long'] = longs

    return df
def itm_to_wgs84(easting: float, northing: float)-> tuple:
    # Constants for GRS80 Ellipsoid
    a = 6378137
    f = 1 / 298.257222101
    b = a * (1 - f)
    e_squared = (a ** 2 - b ** 2) / a ** 2

    # ITM Projection Parameters
    lat_origin = 31.7344
    lon_origin = 35.2074
    k0 = 1.0000067
    E0 = 219529.584
    N0 = 626907.39

    # Inverse calculations
    e_prime_squared = e_squared / (1 - e_squared)
    M0 = a * ((1 - e_squared / 4 - 3 * e_squared ** 2 / 64 - 5 * e_squared ** 3 / 256) * math.radians(lat_origin) -
              (3 * e_squared / 8 + 3 * e_squared ** 2 / 32 + 45 * e_squared ** 3 / 1024) * math.sin(
                2 * math.radians(lat_origin)) +
              (15 * e_squared ** 2 / 256 + 45 * e_squared ** 3 / 1024) * math.sin(4 * math.radians(lat_origin)) -
              (35 * e_squared ** 3 / 3072) * math.sin(6 * math.radians(lat_origin)))

    M = (northing - N0) / k0 + M0
    mu = M / (a * (1 - e_squared / 4 - 3 * e_squared ** 2 / 64 - 5 * e_squared ** 3 / 256))

    e1 = (1 - math.sqrt(1 - e_squared)) / (1 + math.sqrt(1 - e_squared))
    lat_rad = mu + (3 * e1 / 2 - 27 * e1 ** 3 / 32) * math.sin(2 * mu) + (
                21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * math.sin(4 * mu) + (151 * e1 ** 3 / 96) * math.sin(6 * mu) + (
                          1097 * e1 ** 4 / 512) * math.sin(8 * mu)

    N = a / math.sqrt(1 - e_squared * math.sin(lat_rad) ** 2)
    T = math.tan(lat_rad) ** 2
    C = e_prime_squared * math.cos(lat_rad) ** 2
    D = (easting - E0) / (N * k0)

    lat = lat_rad - (N * math.tan(lat_rad) / a) * (
                D ** 2 / 2 - (5 + 3 * T + 10 * C - 4 * C ** 2 - 9 * e_prime_squared) * D ** 4 / 24 +
                (61 + 90 * T + 298 * C + 45 * T ** 2 - 252 * e_prime_squared - 3 * C ** 2) * D ** 6 / 720)

    lon = math.radians(lon_origin) + (D - (1 + 2 * T + C) * D ** 3 / 6 +
                                      (
                                                  5 - 2 * C + 28 * T - 3 * C ** 2 + 8 * e_prime_squared + 24 * T ** 2) * D ** 5 / 120) / math.cos(
        lat_rad)

    # Convert radians to degrees
    lat_deg = math.degrees(lat)
    lon_deg = math.degrees(lon)
    lon_deg = lon_deg - 0.002108
    return round(lat_deg,5), round(lon_deg,5)
