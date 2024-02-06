# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from .nadlan_utils import nominatim_api ,complete_neighborhood
from .sql_reader_nadlan import read_raw_data_table ,read_from_nadlan_clean,distinct_city_list , read_from_nadlan_rank_find_floor
from .sql_save_nadlan import add_new_deals_nadlan_clean , add_new_deals_nadlan_clean_neighborhood_complete
from tqdm import tqdm
import traceback
import logging
logging.basicConfig(level=logging.WARNING)

def convert_data_types(df):
    try:
        df.columns = df.columns.str.lower()
        float_columns = ['assetroomnum', 'dealnature', 'newprojecttext', 'buildingyear', 'yearbuilt', 'buildingfloors','type']
        for col in float_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)
    except Exception as e:
        print(f"An error occurred while converting data types: {e}")
    return df

def pre_process(df):
    try:
        for col in df.columns:
            df[col] = df[col].replace('NaN', np.nan).replace('', np.nan).replace('None', np.nan)
        df = convert_data_types(df)
    except Exception as e:
        print(f"An error occurred during data pre-processing: {e}")
    return df

def rename_cols_update_data_types(df):
    df = pre_process(df)
    # Extract and clean the city name
    df['city'] = df['fulladress'].str.split(',', n=2, expand=True)[1].str.strip()
    df['city'] = df['city'].apply(lambda x: '-'.join([word.strip() for word in x.split('-')]) if '-' in x else x)
    df['home_number'] = pd.to_numeric(df['fulladress'].str.extract('([0-9]+)', expand=False), errors='coerce').astype(
        np.int32)

    df['street'] = df['fulladress'].str.split(',', n=2, expand=True)[0].str.strip()
    df['street'] = df['street'].str.replace(r'\d+', '', regex=True).str.strip()
    df = df[df['street'].str.len() > 2]

    # Drop unnecessary columns and rows
    columns_to_drop = ["projectname", 'polygon_id', 'type', 'displayadress']
    df = df.drop(columns=columns_to_drop)
    df = df.dropna(subset=['dealamount', 'city', 'dealnature', 'dealnaturedescription']).reset_index(drop=True)

    unwanted_types = [
        "nan", "מיני פנטהאוז", "מגורים", "בית בודד", "דופלקס", "קוטג' חד משפחתי", "קוטג' דו משפחתי",
        'מלונאות', 'חנות', 'קרקע למגורים', 'קבוצת רכישה - קרקע מגורים', 'None', 'אופציה',
        'קבוצת רכישה - קרקע מסחרי', 'חניה', 'מסחרי + מגורים', 'דירת נופש', 'דיור מוגן', 'קומבינציה', 'מבנים חקלאיים',
        'תעשיה', 'מסחרי + משרדים', 'בניני ציבור', 'חלוקה/יחוד דירות', 'מחסנים', 'אחר', 'בית אבות', 'עסק',
        "קוטג' טורי", 'ניוד זכויות בניה', 'משרד', 'ללא תיכנון', 'מלונאות ונופש', 'משרדים + מגורים', 'מלאכה'
    ]
    df = df[~df['dealnaturedescription'].isin(unwanted_types)].reset_index(drop=True)

    # Rename columns
    column_mapping = {
        'dealamount': 'price',
        'dealnature': 'size',
        'dealnaturedescription': 'type',
        'assetroomnum': 'rooms',
        'newprojecttext': 'new',
        'buildingfloors': 'floors',
        'buildingyear': 'build_year',
        'yearbuilt': 'rebuilt',
        'dealdate': 'date',
        'floorno': 'floor',
        'keyvalue': 'key',

    }
    df.rename(columns=column_mapping, inplace=True)

    # Data Cleaning and Transformation
    df['build_year'] = np.where(df['rebuilt'].isna(), df['build_year'], df['rebuilt'])

    df['new'] = pd.to_numeric(df['new'], errors='coerce').fillna(0).astype(int)

    df.loc[df['rooms'].isna(), 'rooms'] = (df['size'] / 30).round()
    df = df[df['size'] > 24]
    df['price'] = df['price'].str.replace(',', '').astype(np.int32)
    df['build_year'] = df['build_year'].fillna('0').astype(np.int32)
    df['rebuilt'] = df['rebuilt'].fillna('0').astype(np.int32)

    df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y')
    df['year'] = df['date'].dt.year.astype(np.int32)
    df[['gush', 'helka', 'tat']] = df['gush'].str.split('-|/', n=2, expand=True).astype(np.int32)
    df = df.drop(columns=['fulladress'], axis=1)
    df = df.dropna(subset=['price', 'size', 'type']).reset_index(drop=True)

    return df


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

tqdm.pandas(desc="Enriching location Data")
def enrich_df_with_location_data(df):
    df['details'] = df.progress_apply(lambda row: nominatim_api(row['city'], row['street'], row['gush'],
                                                               row['helka'], row['build_year'], row['floors'], row['home_number']), axis=1)
    df['neighborhood'] = df['details'].apply(lambda x: x.get('neighborhood', '') if x else np.nan)
    df['lat'] = df['details'].apply(lambda x: x.get('lat', '') if x else np.nan)
    df['long'] = df['details'].apply(lambda x: x.get('long', '') if x else np.nan)
    df['x'] = df['details'].apply(lambda x: x.get('x', '') if x else np.nan)
    df['y'] = df['details'].apply(lambda x: x.get('y', '') if x else np.nan)
    df['zip'] = df['details'].apply(lambda x: x.get('zip', '') if x else np.nan)
    df['street'] = df['details'].apply(lambda x: x.get('street', '') if x else np.nan)
    df['addr_key'] = df['details'].apply(lambda x: x.get('addr_key', '') if x else np.nan)

    df.drop(columns=['details'], inplace=True)
    return df

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

def run_nadlan_clean(maintenance=False, overhead= False):
    nadlan_clean_status = {}
    try:
        df = read_raw_data_table(num_of_rows=30000)
        print(f"start shape : {df.shape}")
        df = rename_cols_update_data_types(df)
        df = floor_to_numeric(df, floors)
        df = columns_strip_df(df)
        df = enrich_df_with_location_data(df)
        if overhead:
            df = find_missing_floors(df)
        else:
            df = df.dropna(subset=['floors'])
        # df = clean_outliers(df)
        df['new'].fillna(0, inplace=True)
        df['new'] = df['new'].astype(np.int32)
        data = add_new_deals_nadlan_clean(df)
        data2 = add_new_deals_nadlan_clean(df,'13.50.98.191')
        if maintenance:
            maintenance_neighborhood('nadlan_clean')
        nadlan_clean_status['success'] = True
        nadlan_clean_status['new_rows'] = data['new_rows']
        nadlan_clean_status['conflict_rows'] = data['conflict_rows']

    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        nadlan_clean_status['success'] = False
        nadlan_clean_status['error'] = error_message

    return nadlan_clean_status

