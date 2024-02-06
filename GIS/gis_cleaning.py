import pandas as pd
import numpy as np
from sql_gis import read_distinct_cities_id ,read_city_from_deals ,save_into_db_city_rank
def rename_cols_update_data_types_gis(df):
    # Extract and clean the city name
    # df['city'] = df['fulladress'].str.split(',', n=2, expand=True)[1].str.strip()
    # df['city'] = df['city'].apply(lambda x: '-'.join([word.strip() for word in x.split('-')]) if '-' in x else x)
    # df['home_number'] = pd.to_numeric(df['fulladress'].str.extract('([0-9]+)', expand=False), errors='coerce').astype(
    #     np.int32)

    # df['street'] = df['fulladress'].str.split(',', n=2, expand=True)[0].str.strip()
    # df['street'] = df['street'].str.replace('\d+', '', regex=True).str.strip()
    # df = df[df['street'].str.len() > 2]

    # Drop unnecessary columns and rows
    # columns_to_drop = ["projectname", 'polygon_id', 'type', 'displayadress']
    # df = df.drop(columns=columns_to_drop)
    # df = df.dropna(subset=['dealamount', 'city', 'dealnature', 'dealnaturedescription']).reset_index(drop=True)
    df = df.dropna(subset=['dealamount', 'dealnature', 'dealnaturedescription']).reset_index(drop=True)

    unwanted_types = [
        "nan",
        'מלונאות', 'חנות', 'קרקע למגורים', 'קבוצת רכישה - קרקע מגורים', 'None', 'אופציה',
        'קבוצת רכישה - קרקע מסחרי', 'חניה' , 'קומבינציה', 'מבנים חקלאיים',
        'תעשיה', 'מסחרי + משרדים', 'בניני ציבור', 'חלוקה/יחוד דירות', 'מחסנים', 'אחר', 'בית אבות', 'עסק',
        'ניוד זכויות בניה', 'משרד', 'ללא תיכנון', 'מלונאות ונופש', 'משרדים + מגורים', 'מלאכה'
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
    # df['build_year'] = np.where(df['rebuilt'].isna(), df['build_year'], df['rebuilt'])
    for col in df.columns:
        df[col] = df[col].replace('',np.nan)
    df['new'] = df['new'].fillna(0).astype(float).astype(int)

    # df.loc[df['rooms'].isna(), 'rooms'] = (df['size'] / 30).round()
    df['size'] = df['size'].astype(float)
    df = df[df['size'] > 24]
    df['price'] = df['price'].str.replace(',', '').astype(np.int32)
    # df['build_year'] = df['build_year'].fillna('0').astype(np.int32)
    # df['rebuilt'] = df['rebuilt'].fillna('0').astype(np.int32)

    df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y')
    df['year'] = df['date'].dt.year.astype(np.int32)
    df[['gush', 'helka', 'tat']] = df['gush'].str.split('-|/', n=2, expand=True).astype(np.int32)
    df = df.drop(columns=['fulladress'], axis=1)
    df = df.dropna(subset=['price', 'size', 'type']).reset_index(drop=True)
    # cols = ['year','type','size','key','city_id','city','street','price']
    cols = ['year','type','size','key','city_id','price']
    print(df.shape)
    return df[cols]


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
        city_df = read_city_from_deals(city_id)
        df = rename_cols_update_data_types_gis(city_df)
        years = list(df['year'].unique())
        rank_dict = calculate_city_price_per_year(df,years)
        rank = rank_dict.values()
        data = {
            'year':years,
            'rank':rank,
            'city_id':city_id,
        }
        df = pd.DataFrame(data)
        df = df.sort_values('year')
        save_into_db_city_rank(df)

def main_gis_clean():
    rank_city_by_year()

rank_city_by_year()
