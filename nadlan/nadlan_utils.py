import requests
import pandas as pd
import logging
logger = logging.getLogger(__name__)
import re
import time
import httpx
import numpy as np
df_cache = None
logging.basicConfig(level=logging.WARNING)
from backoff import on_exception, expo
from dev import get_db_connection

city_dict = {
    'אשדוד': 1100,
    'באר שבע': 1200,
    'בני ברק': 1300,
    'בת ים': 1400,
    'גבעתיים': 1500,
    'הרצלייה': 1600,
    'חולון': 1700,
    'חיפה': 1800,
    'ירושלים': 1900,
    'כפר סבא': 2000,
    'נתניה': 2100,
    'פתח תקווה': 2200,
    'ראשון לציון': 2300,
    'רמת גן': 2400,
    'רמת השרון': 2500,
    'רעננה': 2600,
    'תל אביב-יפו': 2700
}


def split_address(address):
    try:
        # Adjusted regular expression to capture the first number in the address
        match = re.match(r'([\u0590-\u05FF\'"׳״\-\s]+)\s(\d+).*?,\s*([\u0590-\u05FF\s-]+)', address)
        if not match:
            raise ValueError(f"Invalid address format: {address}")

        street, home_number, city = match.groups()
        return {'street': street.strip(), 'home_number': home_number, 'city': city.strip()}
    except Exception as e:
        raise Exception(f"Error parsing address: {e}")

def fetch_all_for_city(city):
    global df_cache
    conn = get_db_connection(db_name='nextroof_db')
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM addr_cache WHERE city = %s", (city,))
        records = cur.fetchall()
        columns = ['addr_key', "key", "city", "neighborhood", "street", "home_number", "lat", "long", "type", 'x', 'y',
                   'zip', 'created_at', 'gush', 'helka', 'build_year', 'floors']
        df_cache = pd.DataFrame(records, columns=columns)


def fetch_all_cache_data():
    conn = get_db_connection(db_name='nextroof_db')
    global df_cache
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM addr_cache ")
        records = cur.fetchall()
        columns = ['addr_key', "key", "city", "neighborhood", "street", "home_number", "lat", "long", "type", 'x', 'y',
                   'zip', 'created_at', 'gush', 'helka', 'build_year', 'floors']
        df_cache = pd.DataFrame(records, columns=columns)




def fetch_from_df(cache_key):
    global df_cache
    record = df_cache[df_cache["key"] == cache_key]
    if not record.empty:
        return record.iloc[0].to_dict()
    return None


def save_to_db(cache_key, data):
    global df_cache
    conn = get_db_connection(db_name='nextroof_db')
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO addr_cache (Key, City, Neighborhood, Street, Home_number, Lat, Long, Type, X, Y, Zip, Gush, Helka, Build_year, Floors) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (Key) DO NOTHING
                    RETURNING addr_key
                    """,
                    (cache_key, data['city'], data['neighborhood'], data['street'], data['home_number'], data['lat'],
                     data['long'],
                     data['type'], data['x'], data['y'], data['zip'], data['gush'], data['helka'], data['build_year'],
                     data['floors'])
                )

                result = cur.fetchone()
                conn.commit()
                if result:
                    addr_key = result[0]
                    fetch_all_cache_data()  # improve: instead of read all the table and create new connection add only the spesific row
                    return addr_key
                else:
                    logger.warning(f"No new row was inserted for cache_key: {cache_key}")
                    return None
        except Exception as e:
            print(data)
            logger.error(f"Failed to save data to DB: {e}")
        finally:
            conn.close()


def nominatim_api(city, street, gush, helka, build_year, floors, home_number=None):
    global df_cache
    if pd.isna(city) or pd.isna(street):
        return None

    city = city.strip()
    street = street.strip()
    home_number = str(home_number).strip()

    cache_key = f"{city} {street} {home_number}" if home_number else f"{city} {street}"

    if df_cache is None:
        fetch_all_cache_data()

    data_from_df = fetch_from_df(cache_key)
    if data_from_df:
        return data_from_df

    result_1 = nominatim_addr(cache_key)
    result_gov = govmap_addr(cache_key)
    if result_gov['success']:
        try:
            res_dict = {
                'city': result_gov['city'],
                'street': result_gov['street'],
                'y': result_gov['y'],
                'x': result_gov['x'],
                'neighborhood': result_1['neighborhood'],
                'zip': result_1['zip'],
                'lat': result_1['lat'],
                'long': result_1['long'],
                'type': result_1['type'],
                'home_number': home_number,
                'gush': gush,
                'helka': helka,
                'build_year': build_year,
                'floors': floors,
            }

        except requests.exceptions.RequestException as e:
            print(f"An error occurred with nominatim_api : {e} , res_dict: {res_dict}")
        addr_key = save_to_db(cache_key, res_dict)
        res_dict['addr_key'] = addr_key
        time.sleep(1)
        return res_dict
    return False


@on_exception(expo, requests.HTTPError, max_tries=3)
def govmap_addr(addr):
    link = f"https://es.govmap.gov.il/TldSearch/api/DetailsByQuery?query={addr}&lyrs=276267023&gid=govmap"

    try:
        response = requests.get(link, timeout=30)
        response.raise_for_status()

        json_obj = response.json()
        if json_obj['Error'] == 0:
            address_label = json_obj['data']['ADDRESS'][0]['ResultLable']
            address = split_address(address_label)
            result = {
                'city': address['city'],
                'street': address['street'],
                'home_number': address['home_number'],
                'y': json_obj['data']['ADDRESS'][0]['Y'],
                'x': json_obj['data']['ADDRESS'][0]['X'],
                'success': True,
            }
            return result

    except requests.HTTPError as e:
        print(f"HTTP error occurred: {e}")
        raise
    except Exception as e:
        print(f"An error occurred with GovMap API: {e}")

    return {'success': False}

@on_exception(expo, httpx.HTTPError, max_tries=3)
def nominatim_addr(query):
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
        "lat": None,
        "long": None,
        'success': False,
    }


    with httpx.Client(timeout=30) as client:
        response = client.get(base_url, params=params)
        response.raise_for_status()

        data = response.json()
        if data:
            address = data[0].get('address', {})
            result = {
                "city": address.get("city", ""),
                "neighborhood": address.get("suburb") or address.get("neighborhood", ""),
                "street": address.get("road", ""),
                "zip": address.get("postcode", ""),
                "type": data[0].get("type", ""),
                "lat": round(float(data[0].get("lat", "0")), 5),
                "long": round(float(data[0].get("lon", "0")), 5),
                'success': True,
            }
            return result

    return result


def calc_distance(df, x2, y2):
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


def complete_neighborhood(df):
    df['neighborhood'] = df['neighborhood'].replace('', np.nan)
    df_na = df[df['neighborhood'].isna()].copy()
    df_notna = df[df['neighborhood'].notna()]

    for index in df_na.index:
        row = df_na.loc[index]
        x, y = row['x'], row['y']
        neighborhood = calc_distance(df_notna, x, y)
        df_na.at[index, 'neighborhood'] = neighborhood

    return pd.concat([df_na, df_notna], ignore_index=True)

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

city_code = {
    'תל אביב': 5000,
    'רמת גן': 8600,
    'גבעתיים': 6300,
    'ירושלים': 3000,
    'פתח תקווה': 7900,
    'חולון': 6600,
    'הרצליה': 6400,
    'רעננה': 8700,
    'רעננה_2': 2700,
    'בת ים': 6200,
    'בני ברק': 6100,
    'כפר סבא': 6900,
    'רמת השרון': 2650,
    'ראשון לציון': 8300,
    'חיפה': 4000,
    'אשדוד': 70,
    'נתניה': 7400,
    'באר שבע': 9000
}