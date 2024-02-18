import sys
sys.path.append('C:/Users/yoavl/NextRoof/')
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")
import difflib
from nadlan.sql_reader_nadlan import read_from_cities_table, read_from_streets_table
import pandas as pd
import numpy as np

def find_most_similar_word(word_list, target_word):
    target_word = target_word.decode('utf-8') if isinstance(target_word, bytes) else target_word
    matches = difflib.get_close_matches(target_word, word_list, n=1, cutoff=0.67)
    return matches[0] if matches else None

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


def check_for_match(df_nadlan, parcel):
    parcel = int(parcel)
    parcel_offsets = [parcel - 1, parcel + 1, parcel + 2, parcel - 2, parcel + 3, parcel - 3, parcel + 4, parcel - 4]
    for parcel_offset in parcel_offsets:
        try:
            match = df_nadlan.loc[(df_nadlan['gush_helka'] == str(parcel_offset)), 'helka_rank']
        except:
            match = df_nadlan.loc[(df_nadlan['Gush_Helka'] == str(parcel_offset)), 'Helka_rank']
        if not match.empty:
            return match
    return None


def strip_columns(df):
    string_cols = df.select_dtypes(include=['object']).columns
    df[string_cols] = df[string_cols].apply(lambda col: col.str.strip())
    return df
