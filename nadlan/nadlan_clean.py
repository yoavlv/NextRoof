# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from utils.nadlan_utils import nominatim_api
from .nadlan_utils import complete_neighborhood
from sql_reader_nadlan import read_raw_data_table ,read_from_nadlan_clean,distinct_city_list
from sql_save_nadlan import add_new_deals_nadlan_clean , add_new_deals_nadlan_clean_neighborhood_complete
from tqdm import tqdm
import traceback
import logging
logging.basicConfig(level=logging.WARNING)


def rename_cols_update_data_types(df):
    # Extract and clean the city name
    df['City'] = df['FULLADRESS'].str.split(',', n=2, expand=True)[1].str.strip()
    df['City'] = df['City'].apply(lambda x: '-'.join([word.strip() for word in x.split('-')]) if '-' in x else x)

    # Drop unnecessary columns and rows
    columns_to_drop = ["FULLADRESS", "PROJECTNAME", 'POLYGON_ID', 'TYPE']
    df.drop(columns=columns_to_drop, inplace=True)
    df = df.dropna(subset=['DEALAMOUNT', 'City', 'DEALNATURE','DEALNATUREDESCRIPTION']).reset_index(drop=True)

    unwanted_types = [
        "nan", "מיני פנטהאוז", "מגורים", "בית בודד", "דופלקס", "קוטג' חד משפחתי", "קוטג' דו משפחתי",
        'מלונאות', 'חנות', 'קרקע למגורים', 'קבוצת רכישה - קרקע מגורים', 'None', 'אופציה',
        'קבוצת רכישה - קרקע מסחרי', 'חניה', 'מסחרי + מגורים', 'דירת נופש', 'דיור מוגן', 'קומבינציה', 'מבנים חקלאיים',
        'תעשיה', 'מסחרי + משרדים', 'בניני ציבור', 'חלוקה/יחוד דירות', 'מחסנים', 'אחר', 'בית אבות', 'עסק',
        "קוטג' טורי", 'ניוד זכויות בניה', 'משרד', 'ללא תיכנון', 'מלונאות ונופש', 'משרדים + מגורים', 'מלאכה'
    ]
    df = df[~df['DEALNATUREDESCRIPTION'].isin(unwanted_types)].reset_index(drop=True)

    # Rename columns
    column_mapping = {
        'DEALAMOUNT': 'Price',
        'DEALNATURE': 'Size',
        'DEALNATUREDESCRIPTION': 'Type',
        'ASSETROOMNUM': 'Rooms',
        'NEWPROJECTTEXT': 'New',
        'BUILDINGFLOORS': 'Floors',
        'BUILDINGYEAR': 'Build_year',
        'YEARBUILT': 'Rebuilt',
        'DEALDATE': 'Date',
        'FLOORNO': 'Floor',
        'KEYVALUE':'Key',
    }
    df.rename(columns=column_mapping, inplace=True)

    # Data Cleaning and Transformation
    df['Build_year'] = np.where(df['Rebuilt'].isna(), df['Build_year'], df['Rebuilt'])
    df['New'] = df['New'].fillna(0).astype(float).astype(int)

    df.loc[df['Rooms'].isna(), 'Rooms'] = (df['Size'] / 30).round()
    df = df[df['Size'] > 24]
    df['Price'] = df['Price'].str.replace(',', '').astype(np.int32)
    df['Build_year'] = df['Build_year'].fillna('0').astype(np.int32)
    df['Rebuilt'] = df['Rebuilt'].fillna('0').astype(np.int32)

    df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y')
    df['Year'] = df['Date'].dt.year.astype(np.int32)

    return df


def fill_missing_addresses_gen(df):
    df[['Gush', 'Helka', 'Tat']] = df['GUSH'].str.split('-|/', n=2, expand=True).astype(np.int32)
    df = df.drop(columns='GUSH', axis=1)
    missing_addresses = df.loc[df['DISPLAYADRESS'].isna(), ['Gush', 'Helka']]
    for index, row in missing_addresses.iterrows():
        gush = row['Gush']
        helka = row['Helka']

        match1 = df.loc[(df['Gush'] == gush) & (df['Helka'] == helka), 'DISPLAYADRESS']

        if not match1.empty:
            df.loc[index, 'DISPLAYADRESS'] = match1.values[0]

    missing_addresses = df.loc[df['DISPLAYADRESS'].isna(), ['Gush', 'Helka']]
    df = df.dropna(subset=['Price', 'Size', 'Type', 'DISPLAYADRESS']).reset_index(drop=True)
    df['Home_number'] = pd.to_numeric(df['DISPLAYADRESS'].str.extract('([0-9]+)', expand=False),
                                      errors='coerce').astype(np.int32)
    df['Street'] = df['DISPLAYADRESS'].str.replace('\d+', '', regex=True).str.strip()
    df = df.drop(columns='DISPLAYADRESS', axis=1)
    return df


def rep(df):
    replacements_words = {
                "קומה": "",
                "+": " ",
                "גג": "",
                "יי": "י",
                "(גג)": "",
                "(": "",
                ")": "",

            }
    for old, new in replacements_words.items():
        df.loc[:, 'Floor'] = df.loc[:, 'Floor'].str.replace(old, new)

    return df
floors = {-1:'מרתף', 0:'קרקע', 1:'ראשונה', 2:'שניה', 3:'שלישית', 4:'רביעית', 5:'חמישית', 6:'שישית', 7:'שביעית', 8:'שמינית', 9:'תשיעית', 10:'עשירית', 11:'אחת עשרה', 12:'שתים עשרה', 13:'שלוש עשרה', 14:'ארבע עשרה', 15:'חמש עשרה', 16:'שש עשרה', 17:'שבע עשרה', 18:'שמונה עשרה', 19:'תשע עשרה', 20:'עשרים', 21:'עשרים ואחת', 22:'עשרים ושתיים', 23:'עשרים ושלוש', 24:'עשרים וארבע', 25:'עשרים וחמש', 26:'עשרים ושש', 27:'עשרים ושבע', 28:'עשרים ושמונה', 29:'עשרים ותשע', 30:'שלושיים', 31:'שלושיים ואחת', 32:'שלושיים ושתיים', 33:'שלושים ושלוש', 34:'שלושים וארבע', 35:'שלושים וחמש', 36:'שלושים ושש', 37:'שלושים ושבע', 38:'שלושים ושמונה', 39:'שלושים ותשע', 40:'ארבעים'}
old = "יי"
new = "י"
floors = {k: v.replace(old, new) for k, v in floors.items()}
def extract_numeric(value):
    """Extract numeric part from a string and return it, else None."""
    cleaned_value = ''.join(filter(str.isdigit, value))
    return int(cleaned_value) if cleaned_value else None


def check_floor_match(floor_dict, floor):
    values = list(floors.values())
    if isinstance(floor, list):
        for item in floor:
            item = item.replace(',', " ").strip()
            item = str(item).strip()

            if item in values:
                return item
            else:
                try:
                    floor_int = int(item)
                    print(f"int {floor_int}")
                    return floor_int
                except ValueError:
                    sec_split = item.split('+')
    return None


def floor_to_numeric(df, floors):
    values = list(floors.values())
    floor_dict = {value: key for key, value in floors.items()}
    old = "יי"
    new = "י"
    df['Floor'] = df['Floor'].str.replace(old, new)
    df = df.dropna(subset=['Floor'])
    count = 0
    indices_to_remove = []

    for index, row in df.iterrows():
        floor = str(row['Floor']).strip()
        if floor in values:
            df.at[index, 'Floor'] = floor_dict[floor]
        else:
            floor_int = extract_numeric(floor)
            if floor_int is not None:
                df.at[index, 'Floor'] = floor_int
            else:
                floor_split = floor.split(' ')
                if len(floor_split) < 1:
                    floor_split = floor.split('+')
                match = check_floor_match(floor_dict, floor_split)
                if match is not None:
                    df.at[index, 'Floor'] = match
                else:
                    count += 1
                    indices_to_remove.append(index)

    df = df.drop(index=indices_to_remove)
    # Convert 'Floor' to integers after replacing NaN with np.nan
    df.loc[:, 'Floor'] = pd.to_numeric(df['Floor'], errors='coerce', downcast='integer')

    # Filter rows where 'Floor' is less than 40 and not NaN
    df = df[(df['Floor'] < 40) & (df['Floor'].notna())]
    df.loc[:,'Floor'] = df.loc[:,'Floor'].astype(int)
    return df

tqdm.pandas()
def enrich_df_with_location_data(df):
    df['Details'] = df.progress_apply(lambda row: nominatim_api(row['City'], row['Street'],row['Gush'],
                                                               row['Helka'],row['Build_year'],row['Floors'],row['Home_number']), axis=1)
    df['Neighborhood'] = df['Details'].apply(lambda x: x.get('neighborhood', '') if x else np.nan)
    df['Lat'] = df['Details'].apply(lambda x: x.get('lat', '') if x else np.nan)
    df['Long'] = df['Details'].apply(lambda x: x.get('long', '') if x else np.nan)
    df['X'] = df['Details'].apply(lambda x: x.get('x', '') if x else np.nan)
    df['Y'] = df['Details'].apply(lambda x: x.get('y', '') if x else np.nan)
    df['Zip'] = df['Details'].apply(lambda x: x.get('zip', '') if x else np.nan)
    df['Street'] = df['Details'].apply(lambda x: x.get('street', '') if x else np.nan)
    df['Addr_key'] = df['Details'].apply(lambda x: x.get('addr_key', '') if x else np.nan)

    df.drop(columns=['Details'], inplace=True)
    return df

def add_missing_floors(df):
    df['Floors'] = df['Floors'].where(df['Floors'].notna(), np.nan)
    max_floors_by_group = df.groupby(['Street', 'Home_number'])['Floors'].transform('max')
    df['Floors'] = df['Floors'].combine_first(max_floors_by_group)
    df['Floors'].fillna(df['Floor'], inplace=True)
    df['Floors'] = np.where(df['Floor'] > df['Floors'], df['Floor'], df['Floors'])
    df['Floors'] = df['Floors'].astype(np.int32)
    return df


def clean_outliers(df):
    df['PPM'] = (df["Price"] / df['Size']).astype(np.int32)
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

def run_nadlan_clean(maintenance=False):
    nadlan_clean_status = {}
    try:
        df = read_raw_data_table(num_of_rows=50000)
        print(f"start shape : {df.shape}")
        df = rename_cols_update_data_types(df)
        df = fill_missing_addresses_gen(df)
        df = rep(df)
        df = floor_to_numeric(df, floors)
        df = columns_strip_df(df)
        df = enrich_df_with_location_data(df)
        df = add_missing_floors(df)
        # df = clean_outliers(df)
        df['New'].fillna(0, inplace=True)
        df['New'] = df['New'].astype(np.int32)
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

