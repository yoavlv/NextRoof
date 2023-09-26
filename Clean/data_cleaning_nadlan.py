import pandas as pd
import csv
import numpy as np
from datetime import datetime
from datetime import timedelta
import math
import re
from monitor import monitor_data
import os
import sys
sys.path.append('C:/Users/yoavl/NextRoof/Clean')
from sql_functions import table_to_df
import traceback

def rename_cols_update_data_types(df):
    df.drop(columns=["FULLADRESS", "PROJECTNAME", 'KEYVALUE', 'POLYGON_ID', 'TYPE'], axis=1, inplace=True)
    df = df.dropna(subset=['DEALAMOUNT']).reset_index(drop=True)
    types = ["nan", "מיני פנטהאוז", "מגורים", "בית בודד", "דופלקס", "קוטג' חד משפחתי", "קוטג' דו משפחתי", 'מלונאות',
             'חנות', 'קרקע למגורים', 'קבוצת רכישה - קרקע מגורים', 'None', 'אופציה', 'קבוצת רכישה - קרקע מסחרי', 'חניה',
             'מסחרי + מגורים', 'דירת נופש', 'דיור מוגן', 'קומבינציה', 'מבנים חקלאיים', 'תעשיה', 'מסחרי + משרדים',
             'בניני ציבור', 'חלוקה/יחוד דירות', 'מחסנים', 'אחר', 'בית אבות', 'עסק', "קוטג' טורי", 'ניוד זכויות בניה',
             'משרד', 'ללא תיכנון', 'מלונאות ונופש', 'משרדים + מגורים', 'מלאכה', ]

    df = df[~df['DEALNATUREDESCRIPTION'].isin(types)].reset_index(drop=True)

    df.rename(columns={'DEALAMOUNT': 'Price', 'DEALNATURE': 'Size', 'DEALNATUREDESCRIPTION': 'Type',
                       'ASSETROOMNUM': 'Rooms', 'NEWPROJECTTEXT': 'New', 'BUILDINGFLOORS': 'Floors',
                       'BUILDINGYEAR': 'Build_year', 'YEARBUILT': 'Rebuilt', 'DEALDATE': 'Date'}, inplace=True)

    df['Build_year'] = np.where(df['Rebuilt'].isna(), df['Build_year'], df['Rebuilt'])
    df['New'] = df['New'].fillna(0).astype(int)
    df['Rooms'] = df['Rooms'].fillna(lambda x: round(x['Size'] / 30))
    df['Size'] = df['Size'].fillna('0').astype(np.int32)
    df = df[df['Size'] > 24]
    df['Price'] = df['Price'].str.replace(',', '').astype(np.int32)
    df['Build_year'] = df['Build_year'].fillna('0').astype(np.int32)
    df['Rebuilt'] = df['Rebuilt'].fillna('0').astype(np.int32)
    df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y')
    df['Year'] = df['Date'].dt.year.astype(np.int32)
    # df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y').dt.strftime('%d.%m.%Y')
    # df['Year'] = pd.to_datetime(df['Date']).dt.strftime('%Y').astype(np.int32)

    return df


def fill_missing_addresses(df):
    df_address = table_to_df('address',['gush','helka','street','home_number'])
    df_nadlan = table_to_df('nadlan',['gush','helka','street','home_number'])
    df1_gov = pd.read_csv("C:/Users/yoavl/NextRoof/Data/Real_Estate_TLV_GOVMAPS_1.csv", index_col=0)
    df2_gov = pd.read_csv("C:/Users/yoavl/NextRoof/Data/Real_Estate_TLV_GOVMAPS_2.csv", index_col=0)

    df_gov = pd.merge(df1_gov, df2_gov, how='outer')
    df_gov[['Gush', 'Helka', 'Tat']] = df_gov['GUSHHELKATAT'].str.split('-|/', n=2, expand=True).astype(np.int32)

    df[['Gush', 'Helka', 'Tat']] = df['GUSH'].str.split('-|/', n=2, expand=True).astype(np.int32)
    df = df.drop(columns='GUSH', axis=1)
    missing_addresses = df.loc[df['DISPLAYADRESS'].isna(), ['Gush', 'Helka']]

    for index, row in missing_addresses.iterrows():
        gush = row['Gush']
        helka = row['Helka']

        filtered_rows = df_nadlan.loc[(df_nadlan['gush'] == gush) & (df_nadlan['helka'] == helka)]
        match1 = filtered_rows['street'] + ' ' + filtered_rows['home_number'].astype(str)

        filtered_rows = df_address.loc[(df_address['gush'] == gush) & (df_address['helka'] == helka)]
        match2 = filtered_rows['street'] + ' ' + filtered_rows['home_number'].astype(str)

        match3 = df_gov.loc[(df_gov['Gush'] == gush) & (df_gov['Helka'] == helka), 'ADDRESS']

        if not match1.empty:
            df.loc[index, 'DISPLAYADRESS'] = match1.values[0]
        elif not match2.empty:
            df.loc[index, 'DISPLAYADRESS'] = match2.values[0]

        elif not match3.empty:
            df.loc[index, 'DISPLAYADRESS'] = match3.values[0]

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
        df.loc[:,'FLOORNO'] = df.loc[:,'FLOORNO'].str.replace(old, new)

    return df

floors = {-1: 'מרתף', 0: 'קרקע', 1: 'ראשונה', 2: 'שניה', 3: 'שלישית', 4: 'רביעית', 5: 'חמישית', 6: 'שישית', 7: 'שביעית',
          8: 'שמינית', 9: 'תשיעית', 10: 'עשירית', 11: 'אחת עשרה', 12: 'שתים עשרה', 13: 'שלוש עשרה', 14: 'ארבע עשרה',
          15: 'חמש עשרה', 16: 'שש עשרה', 17: 'שבע עשרה', 18: 'שמונה עשרה', 19: 'תשע עשרה', 20: 'עשרים',
          21: 'עשרים ואחת', 22: 'עשרים ושתיים', 23: 'עשרים ושלוש', 24: 'עשרים וארבע', 25: 'עשרים וחמש', 26: 'עשרים ושש',
          27: 'עשרים ושבע', 28: 'עשרים ושמונה', 29: 'עשרים ותשע', 30: 'שלושיים', 31: 'שלושיים ואחת',
          32: 'שלושיים ושתיים', 33: 'שלושים ושלוש', 34: 'שלושים וארבע', 35: 'שלושים וחמש', 36: 'שלושים ושש',
          37: 'שלושים ושבע', 38: 'שלושים ושמונה', 39: 'שלושים ותשע', 40: 'ארבעים'}

def clean_and_convert_to_int(value):
    # Remove non-numeric characters and whitespace
    cleaned_value = ''.join(filter(str.isdigit, value))
    if cleaned_value:
        return int(cleaned_value)
    return None


def check_floor_match(floor_dict, floor):
    values = list(floors.values())
    if isinstance(floor, list):
        for item in floor:
            item = item.replace(',', "").strip()

            if item in values:
                return item
            else:
                try:
                    floor_int = int(item)
                    return floor_int
                except ValueError:
                    pass
    return None

def floor_to_numeric(df, floors):
    values = list(floors.values())
    floor_dict = {value: key for key, value in floors.items()}
    df = df.dropna(subset=['FLOORNO'])

    count = 0
    indices_to_remove = []

    for index, row in df.iterrows():
        floor = row['FLOORNO']
        if floor in values:
            df.at[index, 'FLOORNO'] = floor_dict[floor]
        else:
            floor_int = clean_and_convert_to_int(floor)
            if floor_int is not None:
                df.at[index, 'FLOORNO'] = floor_int
            else:
                floor_split = floor.split(' ')
                if len(floor_split) < 1:
                    floor_split = floor.split('+')
                match = check_floor_match(floor_dict, floor_split)
                if match is not None:
                    df.at[index, 'FLOORNO'] = match
                else:
                    count += 1
                    indices_to_remove.append(index)

    df = df.drop(index=indices_to_remove)


    df = df.rename(columns={'FLOORNO': 'Floor'})
    # Convert 'Floor' to integers after replacing NaN with np.nan
    df.loc[:, 'Floor'] = pd.to_numeric(df['Floor'], errors='coerce', downcast='integer')

    # Filter rows where 'Floor' is less than 40 and not NaN
    df = df[(df['Floor'] < 40) & (df['Floor'].notna())]
    df['Floor'] = df['Floor'].astype(int)
    return df



def edit_addresses(df):
    df = df.dropna(subset=['ms_gush', 'ms_chelka']).reset_index(drop=True)
    df.rename(columns={'ms_gush': 'Gush'}, inplace=True)
    df['Gush'] = df['Gush'].astype(np.int32)

    df.rename(columns={'ms_chelka': 'Helka'}, inplace=True)
    df['Helka'] = df['Helka'].astype(np.int32)

    df.rename(columns={'t_rechov': 'Street'}, inplace=True)
    df.rename(columns={'ms_bayit': 'Home_number'}, inplace=True)
    return df



def megre_df(df1):
    df1 = df1.dropna(subset=['Home_number', 'Street']).reset_index(drop=True)
    # df2 = df2.dropna(subset=['Home_number', 'Street']).reset_index(drop=True)
    df2 = table_to_df('address',['street','home_number','long','lat','gush','helka','uniqueid'])

    df1['Gush'] = df1['Gush'].astype(str)
    df1['Helka'] = df1['Helka'].astype(str)
    df1['Home_number'] = df1['Home_number'].astype(str)

    df2['gush'] = df2['gush'].astype(str)
    df2['helka'] = df2['helka'].astype(str)
    df2['home_number'] = df2['home_number'].astype(str)

    df1['m'] = df1['Gush'] + df1['Helka'] + df1['Home_number']
    df2['m'] = df2['gush'] + df2['helka'] + df2['home_number']

    df2 = df2.drop(columns=['street', 'home_number', 'gush', 'helka'], axis=1)
    df2.drop_duplicates(subset=['m'], inplace=True)
    merged = pd.merge(df1, df2, on=['m'], how='left')

    # cols = ['m', 't_ktovet_melea', 't_bayit_veknisa', 'knisa', 'k_status_hesder', 'k_rechov', 'lon', 'lat', 'id_ktovet']
    merged = merged.drop(columns=['m'], axis=1)

    merged.rename(columns={'long': 'Long'}, inplace=True)
    merged.rename(columns={'lat': 'Lat'}, inplace=True)
    merged = merged.rename(columns={'uniqueid': 'UniqueId'})

    return merged



def add_neighborhood_column(df):
    def add_neighborhoods():
        df = pd.read_csv("C:/Users/yoavl/NextRoof/Data/Real_Estate_TLV_GOVMAPS_1.csv", index_col=0)
        neighborhoods = {}
        for index, row in df.iterrows():
            neighborhood = row['NEIGHBORHOOD']
            street = row['STREENNAME']
            if neighborhood not in neighborhoods:
                neighborhoods[neighborhood] = set()
            neighborhoods[neighborhood].add(street)
        return neighborhoods

    neighborhoods = add_neighborhoods()

    df['Neighborhood'] = np.nan
    for index, row in df.iterrows():
        street = row['Street']

        for neighborhood, streets in neighborhoods.items():
            # Check if the street is in the set of streets for the neighborhood
            if street in streets:
                # If it is, assign the neighborhood to the 'neighborhood' column for this row
                df.loc[index, 'Neighborhood'] = neighborhood
                break  # Break out of the inner loop once we find a match

    df['Neighborhood'] = df['Neighborhood'].str.replace('-', ' ')
    t = "'"
    df['Neighborhood'] = df['Neighborhood'].str.replace(t, "")
    df['Neighborhood'] = df['Neighborhood'].str.replace('נוה', 'נווה')
    df = df.dropna(subset=['Neighborhood']).reset_index(drop=True)
    return df


def add_missing_floors(df):
    df['Floors'] = df['Floors'].where(df['Floors'].notna(), np.nan)
    max_floors_by_group = df.groupby(['Street', 'Home_number'])['Floors'].transform('max')
    df['Floors'] = df['Floors'].combine_first(max_floors_by_group)
    df['Floors'].fillna(df['Floor'], inplace=True)
    df['Floors'] = np.where(df['Floor'] > df['Floors'], df['Floor'], df['Floors'])
    df['Floors'] = df['Floors'].astype(np.int32)
    return df


def fill_missing_type(df):
    df['UniqueId'] = df['UniqueId'].where(df['Type'].notna(), np.nan)
    most_frequent_type_by_group = df.groupby(['UniqueId'])['Type'].apply(lambda x: x.mode().iat[0])
    df['Type'] = df['Type'].combine_first(most_frequent_type_by_group)
    return df.dropna(subset=['Type'])


def fill_rooms(df):
    df['Rooms'] = df.apply(lambda row: round(row['Size'] / 30) if pd.isna(row['Rooms']) else row['Rooms'], axis=1)
    df['Rooms'] = df['Rooms'].astype(np.int32)
    return df


def fill_missing_neighborhood(df):
    def euclidean_distance(lat1, lon1, lat2, lon2):
        dx = (lon2 - lon1) ** 2
        dy = (lat2 - lat1) ** 2
        distance = np.sqrt(dx + dy)
        return distance

    # Find rows with missing 'Neighborhood' values
    missing_neighborhood = df[df['Neighborhood'].isna()]
    not_missing_neighborhood = df.dropna(subset=['Neighborhood'])

    for idx, row in missing_neighborhood.iterrows():
        lat = row['Lat']
        lon = row['Long']
        distances = euclidean_distance(lat, lon, not_missing_neighborhood['Lat'], not_missing_neighborhood['Long'])
        closest_index = distances.idxmin()
        df.at[idx, 'Neighborhood'] = df.at[closest_index, 'Neighborhood']

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


def update_neighborhood_street_token(df):
    df['Street'] = df['Street'].str.strip().str.replace("'", "")
    df['Neighborhood'] =df['Neighborhood'].str.strip()

    replacements_street = {
            "כרמייה": "כרמיה"
        }

    replacements_neighborhood = {
            "'": "",
            "-": " ",
            "נווה אביבים": "נווה אביבים וסביבתה",
            "לב תל אביב, לב העיר צפון": "הצפון הישן החלק הצפוני",
            "נוה": "נווה",
            "גני צהלה, רמות צהלה": "גני צהלה",
            "נווה אליעזר וכפר שלם מזרח": "כפר שלם מזרח נווה אליעזר",
            "גני שרונה, קרית הממשלה": "גני שרונה",
            "הצפון הישן   דרום": "הצפון החדש החלק הדרומי",
            "מכללת תל אביב יפו, דקר": "דקר"
        }

    for old, new in replacements_street.items():
        df['Street'] = df['Street'].str.replace(old, new)

    for old, new in replacements_neighborhood.items():
        df['Neighborhood'] =df['Neighborhood'].str.replace(old, new)
    return df

from utils.base import check_for_match
def parcel_rank(df):
    count = 0
    df['Helka_rank'] = np.nan

    df = df.dropna(subset=['Gush', 'Helka']).reset_index(drop=True)
    df['Gush'] = df['Gush'].astype(np.int32)
    df['Helka'] = df['Helka'].astype(np.int32)
    df_nadlan = table_to_df('nadlan',['gush', 'helka', 'helka_rank'])
    df_nadlan['gush_Helka'] = df_nadlan['gush'].astype(str) + df_nadlan['helka'].astype(str)
    column = 'Gush_Helka'
    df[column] = df['Gush'].astype(str) + df['Helka'].astype(str)
    df[column] = df[column].str.replace('.', '', regex=False)

    for index, row in df.iterrows():
        gush_helka = row['Gush_Helka']
        match = df_nadlan.loc[(df_nadlan['gush_Helka'] == gush_helka), 'helka_rank']

        if match.empty:
            match = check_for_match(df_nadlan, gush_helka)

        if not match.empty:
            df.at[index, 'Helka_rank'] = match.values[0]

        if match.empty:
            count += 1

        df['Helka_rank'] = df['Helka_rank'].fillna(df_nadlan['helka_rank'].mean()).astype(np.int32)
    df = df.drop(columns='Gush_Helka')

    return df


def general_rank(df, column):
    df = update_neighborhood_street_token(df)
    df = df.dropna(subset=['Neighborhood', 'Street']).reset_index(drop=True)

    columns_needed = [
        'year', 'neighborhood',
        'street', 'gush', 'street_rank',
        'neighborhood_rank', 'gush_rank'
    ]
    df_nadlan = table_to_df('nadlan', columns_needed)

    new_column_name = column + '_rank'
    df[new_column_name] = np.nan

    for index, row in df.iterrows():
        year = int(row['Year'])
        col_to_rank = row[column]
        try:
            col_to_rank = int(row[column])
        except:
            pass

        match = df_nadlan.loc[
            (df_nadlan[column.lower()] == col_to_rank) & (df_nadlan['year'] == year),
            new_column_name.lower()
        ]

        if not match.empty:
            df.at[index, new_column_name] = match.iloc[0]
        else:
            print(f"No match found for Index: {index}, Year: {year}, col_to_rank: {col_to_rank}")

            match = df_nadlan.loc[
                (df_nadlan[column.lower()] == col_to_rank) & (df_nadlan['year'] == year - 1),
                new_column_name.lower()
            ]

            if not match.empty:
                df.at[index, new_column_name] = match.iloc[0]

    col = new_column_name.lower()
    df_nadlan[col] = df_nadlan[col].astype(np.int32)
    mean = df_nadlan[col].mean()

    r = df[df[new_column_name].isna()]
    f = df[df[new_column_name].notna()]

    print(f"col {col}\ntotal na: {r.shape}\ntotal not na: {f.shape}")

    df[new_column_name] = df[new_column_name].fillna(mean)
    df[new_column_name] = df[new_column_name].astype(np.int32)

    return df

def run_nadlan_clean():
    try:
        df = pd.read_csv("C:/Users/yoavl/NextRoof/Data/nadlan_p.csv",index_col=0)
        print(f"start shape : {df.shape}")
        df = rename_cols_update_data_types(df)
        df = fill_missing_addresses(df)
        df = rep(df)
        df = floor_to_numeric(df, floors)
            # df_a = pd.read_csv("C:/Users/yoavl/NextRoof/Data/Addresses.csv")
            # df_a = edit_addresses(df_a)
        df_m = megre_df(df)
        df_m = add_neighborhood_column(df_m)
        df_m = fill_missing_neighborhood(df_m)
        # df_m = fill_rooms(df_m)
        df_m = add_missing_floors(df_m)
        df_m = fill_missing_type(df_m)
        df_m = add_neighborhood_column(df_m)

        df_m = general_rank(df_m, 'Street')

        df_m = general_rank(df_m, 'Neighborhood')
        df_m = clean_outliers(df_m)
        df_m = general_rank(df_m, 'Gush')
        df_m = parcel_rank(df_m)
        df_m['New'].fillna(0, inplace=True)
        df_m['New'] = df_m['New'].astype(np.int32)
        df_m.to_csv('C:/Users/yoavl/NextRoof/Data/nadlan_clean_p.csv')

        print(f"final nadlan shape : {df_m.shape}")
        monitor_data['Clean']['nadlan']['Total_size'] = df_m.shape
        monitor_data['Clean']['nadlan']['status'] = 'Success'

    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        print(f"error :{error_message}")
        monitor_data['Clean']['nadlan']['status'] = 'Fail'
        monitor_data['Clean']['nadlan']['error'] = e



