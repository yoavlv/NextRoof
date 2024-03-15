import requests
import pandas as pd
import logging
logger = logging.getLogger(__name__)
import re
import httpx
import numpy as np
logging.basicConfig(level=logging.WARNING)
from backoff import on_exception, expo
from dev import get_db_connection
import psycopg2
from utils.base import add_id_columns
from utils.utils_sql import DatabaseManager
from typing import Optional, Dict, Any

def split_address(address: str)->dict:
    try:
        match = re.match(r'([\u0590-\u05FF\'"׳״\-\s]+)\s(\d+).*?,\s*([\u0590-\u05FF\s-]+)', address)

        if not match:
            raise ValueError(f"Invalid address format: {address}")

        street, home_number, city = match.groups()
        return {'street': street.strip(), 'home_number': home_number, 'city': city.strip()}
    except Exception as e:
        raise ValueError(f"Error parsing address: {e}")


def fetch_from_df(row: Dict[str, Any], df: pd.DataFrame, save: bool) -> Optional[Dict[str, Any]]:

    try:
        street_id = row.get('street_id')
        city_id = row.get('city_id')
        home_number = int(float(row.get('home_number')))
    except Exception as e:
        raise ValueError(f"(fetch_from_df) Error processing row fields: {e}")

    record = df[(df["city_id"] == int(city_id)) & (df["street_id"] == street_id) & (df["home_number"] == int(home_number))]

    if not record.empty:
        rec = record.iloc[0].to_dict()

        rec['gush'] = row['gush']
        rec['helka'] = row['helka']
        if rec['build_year']:
            rec['build_year'] = row['build_year']

        if rec['floors']:
            rec['floors'] = row['floors']

        rec['city'] = row['city']
        rec['street'] = row['street']

        if rec['date'] < row['date'] and save:
            rec['date'] = row['date']
            series_rec = pd.Series(rec)
            db_manager = DatabaseManager(db_name='nextroof_db', table_name='addr_cache')
            db_manager.insert_record(series_rec, rec.keys(), ['city_id', 'street_id','home_number'])

        return rec

    return None


def save_to_db(data: dict) -> None:
    """
    Save a DataFrame to the addr_cache table in the database.
    """
    conn = get_db_connection(db_name='nextroof_db')
    if conn:
        try:
            with conn.cursor() as cur:
                # Prepare the INSERT INTO statement with the correct number of placeholders
                insert_stmt = """
                    INSERT INTO addr_cache (city, neighborhood, street, home_number, type, x, y, zip, gush, helka, build_year, floors, city_id, street_id, date)
                    VALUES (%(city)s, %(neighborhood)s, %(street)s, %(home_number)s, %(type)s, %(x)s, %(y)s, %(zip)s, %(gush)s, %(helka)s, %(build_year)s, %(floors)s, %(city_id)s, %(street_id)s, %(date)s)
                    ON CONFLICT (city_id, street_id, home_number) DO UPDATE SET
                        city = EXCLUDED.city,
                        neighborhood = EXCLUDED.neighborhood,
                        street = EXCLUDED.street,
                        home_number = EXCLUDED.home_number,
                        type = EXCLUDED.type,
                        x = EXCLUDED.x,
                        y = EXCLUDED.y,
                        zip = EXCLUDED.zip,
                        gush = EXCLUDED.gush,
                        helka = EXCLUDED.helka,
                        build_year = EXCLUDED.build_year,
                        floors = EXCLUDED.floors,
                        city_id = EXCLUDED.city_id,
                        street_id = EXCLUDED.street_id,
                        date = EXCLUDED.date;
                """
                # Ensure data_tuples matches the structure expected by the insert statement
                cur.execute(insert_stmt, data)
                conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error (save_to_db): {error}")
            conn.rollback()
        finally:
            conn.close()

COUNTER = 0
def nominatim_api(row: Dict[str, Any], df: Optional[pd.DataFrame] = None, save: bool = True) -> Dict[str, Any]:
    global COUNTER
    city = str(row['city']).strip()
    street = str(row['street']).strip()
    home_number = int(float(row['home_number']))
    city_id = row['city_id']
    street_id = row['street_id']

    search_key = f"{city} {street} {home_number}"

    if df is not None:
        data_from_df = fetch_from_df(row, df, save)
        if data_from_df:
            return data_from_df

    COUNTER += 1
    if COUNTER % 100 == 0:
        print(COUNTER)

    result_1 = nominatim_addr(search_key)
    result_gov = govmap_addr(search_key)

    try:
        if result_gov['success']:
            res_dict = {
                'city': row['city'],
                'street': row['street'],
                'y': result_gov['y'],
                'x': result_gov['x'],
                'neighborhood': result_1['neighborhood'],
                'zip': result_1['zip'],
                'type': result_1['type'],
                'home_number': home_number,
                'gush': row['gush'],
                'helka': row['helka'],
                'build_year': row['build_year'],
                'floors': row['floors'],
                'street_id': street_id,
                'city_id': city_id,
            }
            if save:
                res_dict['date'] = row['date']

                save_to_db(res_dict)
            return res_dict
    except requests.exceptions.RequestException as e:
        print(f"(nominatim_api) An error occurred with nominatim_api : {e} ")
    return {
        'y': np.nan,
        'x': np.nan,
        'neighborhood': np.nan,
        'zip': np.nan,
        'type': np.nan,
    }


@on_exception(expo, httpx.HTTPError, max_tries=3)
def govmap_addr(addr: str) -> Dict[str, Any]:
    link = f"https://es.govmap.gov.il/TldSearch/api/DetailsByQuery?query={addr}&lyrs=276267023&gid=govmap"
    try:
        response = httpx.get(link, timeout=30)
        response.raise_for_status()

        json_obj = response.json()
        if json_obj['Error'] == 0:
            address_label = json_obj['data']['ADDRESS'][0]['ResultLable']
            # address = split_address(address_label)
            result = {
                # 'city': address['city'],
                # 'street': address['street'],
                # 'home_number': address['home_number'],
                'y': json_obj['data']['ADDRESS'][0]['Y'],
                'x': json_obj['data']['ADDRESS'][0]['X'],
                'success': True,
            }
            return result

    except httpx.HTTPError as e:
        print(f"HTTP error occurred: {e}")

    except Exception as e:
        print(f"An error occurred with GovMap API: {e}")
    result = {
        'y': None,
        'x': None,
        'success': True,
    }
    return result

@on_exception(expo, httpx.HTTPError, max_tries=3)
def nominatim_addr(query: str, client: Optional[httpx.Client] = None) -> Dict[str, Any]:
    base_url = "https://nominatim.openstreetmap.org/search"
    params = {
        'q': query,
        'format': 'jsonv2',
        'addressdetails': 1
    }
    result = {
        "neighborhood": None,
        "type": None,
        "zip": None,
        'success': False,
    }

    if client is None:
        with httpx.Client(timeout=30) as client:
            response = client.get(base_url, params=params)
    else:
        response = client.get(base_url, params=params)

    if response.status_code != 200:
        raise httpx.HTTPStatusError("An error occurred.", request=response.request, response=response)

    response.raise_for_status()

    if not response.json():
        return result

    data = response.json()
    address = data[0].get('address', {})

    result.update({
        "neighborhood": address.get("suburb") or address.get("neighborhood", ""),
        "zip": address.get("postcode", ""),
        "type": data[0].get("type", ""),
        'success': True,
    })

    return result


def calc_distance(df: pd.DataFrame, x2: float, y2: float) -> Optional[str]:
    distance = 10000
    closest_neighborhood = None

    for index, row in df.iterrows():
        x1, y1 = row['x'], row['y']
        temp_distance = np.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)

        if distance > temp_distance:
            distance = temp_distance
            closest_neighborhood = row['neighborhood']

            if distance < 40:
                break
    return closest_neighborhood


def complete_neighborhood(df: pd.DataFrame) -> pd.DataFrame:
    df['neighborhood'] = df['neighborhood'].replace('', np.nan)
    df_na = df[df['neighborhood'].isna()].copy()
    df_notna = df[df['neighborhood'].notna()]

    for index in df_na.index:
        row = df_na.loc[index]
        x, y = row['x'], row['y']
        neighborhood = calc_distance(df_notna, x, y)
        df_na.at[index, 'neighborhood'] = neighborhood

    return pd.concat([df_na, df_notna], ignore_index=True)


def rename_cols_update_data_types(df: pd.DataFrame) -> pd.DataFrame:
    df = pre_process(df).copy()
    df = df.dropna(subset=['fulladress'])
    # Extract and clean the city name
    df['city'] = df['fulladress'].str.split(',', n=2, expand=True)[1].str.strip()
    df['city'] = df['city'].apply(lambda x: '-'.join([word.strip() for word in x.split('-')]) if '-' in x else x)
    df['home_number'] = pd.to_numeric(df['fulladress'].str.extract('([0-9]+)', expand=False), errors='coerce').astype(
        float)

    df['street'] = df['fulladress'].str.split(',', n=2, expand=True)[0].str.strip()
    df.loc[:, 'street'] = df.loc[:, 'street'].str.replace(r'\d+', '', regex=True).str.strip()
    df = df[df['street'].str.len() > 2]

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
    df = add_id_columns(df, 'city_id', 'city')

    return df


def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df.columns = df.columns.str.lower()
        float_columns = ['assetroomnum', 'dealnature', 'newprojecttext', 'buildingyear', 'yearbuilt', 'buildingfloors','type']
        for col in float_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)
    except Exception as e:
        print(f"An error occurred while converting data types: {e}")
    return df

def pre_process(df: pd.DataFrame) -> pd.DataFrame:
    try:
        for col in df.columns:
            df[col] = df[col].replace('NaN', np.nan).replace('', np.nan).replace('None', np.nan)
        df = convert_data_types(df)
    except Exception as e:
        print(f"An error occurred during data pre-processing: {e}")
    return df

payload = {
    "ObjectID": "5000",
    "CurrentLavel": 2,
    "PageNo": 1,
    "OrderByFilled": "DEALDATETIME",
    "OrderByDescending": True,
}

cookies = {
    'APP_CTX_USER_ID': 'a123e7c7-66fd-4c46-b286-eb7e43d52e38',
    'Infinite_user_id_key': 'a123e7c7-66fd-4c46-b286-eb7e43d52e38',
    'G_ENABLED_IDPS': 'google',
    'Infinite_ab_tests_context_v2_key': '{%22context%22:{%22_be_sortMarketplaceByDate%22:%22modeA%22%2C%22_be_sortMarketplaceAgeWeight%22:%22modeA%22%2C%22uploadRangeFilter%22:%22modeA%22%2C%22mapLayersV1%22:%22modeB%22%2C%22tabuViewMode%22:%22modeA%22%2C%22homepageSearch%22:%22modeA%22%2C%22removeWizard%22:%22modeB%22%2C%22whatsAppPoc%22:%22modeB%22%2C%22_be_addLastUpdateToWeights%22:%22modeB%22%2C%22quickFilters%22:%22modeA%22%2C%22projectPageNewLayout%22:%22modeB%22}}',
    'USER_TOKEN_V2': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleGFjdC10aW1lIjoxNjkzOTEyNzc0NjEwLCJwYXlsb2FkIjoie1widWlkXCI6XCJkNDg5YWE2Zi01MDQ5LTQxMzAtOTU0Yi04ZWI3ZTZhMzRjNDJcIixcInNlc3Npb24taWRcIjpcIjJhZWM0ZGVhLWQ2ZTgtNGUwNS05YzUzLWUzOTUyZmY3OGRkN1wiLFwidHRsXCI6NjMxMTUyMDB9IiwiaWF0IjoxNjkzOTEyNzc0LCJpc3MiOiJsb2NhbGl6ZSIsInVzZXJJZCI6ImQ0ODlhYTZmLTUwNDktNDEzMC05NTRiLThlYjdlNmEzNGM0MiIsInJlZ2lzdHJhdGlvblR5cGUiOiJWSVNJVE9SIiwicm9sZXMiOlsiVklTSVRPUiJdLCJpc0ltcGVyc29uYXRpb25Mb2dJbiI6ZmFsc2UsInNhbHQiOiIyYWVjNGRlYS1kNmU4LTRlMDUtOWM1My1lMzk1MmZmNzhkZDciLCJ2IjoyLCJleHAiOjE3NTcwMjc5NzR9.VotKqMoFvOetPjvbtpXlfJdtRmrA7wPpV7fqlC8JC74',
    'PA_STORAGE_SESSION_KEY': '{%22marketPlaceDialog%22:{%22expiredDate%22:1693999364192%2C%22closeClickCount%22:2}%2C%22marketPlaceBanner%22:{%22expiredDate%22:1679688327477%2C%22closeClickCount%22:1}}',
    'MORTGAGE_STORAGE_SESSION_KEY': '{%22closeClickCount%22:0%2C%22time%22:null%2C%22sessionStartMs%22:1693912773463%2C%22hideElements%22:false%2C%22lastShownPopupOnListingPage%22:%22mortgage_popup%22%2C%22listingPagePopupShownAtMs%22:1693919946929%2C%22popupOpenCount%22:1}',
    'APP_CTX_SESSION_ID': '28c980dc-e695-4c6e-8974-54ba3dec9c60',
    'g_state': '{"i_l":2,"i_p":1694069673816}',
    '_sp_ses.549d': '*',
    'AWSALB': 'RBoIUI9A5zTMy8owT0PTfyyQVdVcdWziNE5l8DChnvekJJzQCeCEYSI2pEKlEFUwpkZiVwRIROebsMsKIUAqroTfTKdZyLF7n8rJ+lIV9so5UVKF+KASlsWWamMb',
    '_ud': 'e5e30c9d3f4fa07ff253e581a1332b8a88bd8de1-cid=93b255cf-c6cb-4747-af1a-e159b5a30449&_ts=1693984674278',
    '_sp_id.549d': '065d6cdd-43ee-42e4-aa08-b131f450b231.1693913431.2.1693984679.1693914698.00a8ce71-f3c1-42ae-bced-202ea803ffe5',
    'WINDOW_WIDTH': '1075',
    '_pxhd': 'jUtmbGaZxN8SdV0whjtJsE9e-LKDC/JYU42jxFi6v52gvWlse9yKPCoGmvHk0MRfm6cKsD1gcdLhwwU3StEZKA==:vKJHWREy1wPg4LAD0NnaornHxpl1WgD7sGHxfr410FudPlb-ykkAPl0Q9Oal9h635/4qI0npPrn8-JvfiOJDL4smJPT5eGPgOo7Cs9HeFn0=',
}
