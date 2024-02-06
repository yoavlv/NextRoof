import requests
import sys
import re
sys.path.append('C:/Users/yoavl/NextRoof/')
import pandas as pd
import time
import numpy as np
import logging
logging.basicConfig(level=logging.WARNING)
from tqdm import tqdm
tqdm.pandas()
df_cache = None
import httpx
from dev import get_db_connection

def get_gush_helka_api(city,street,home_number):
    if pd.isna(city) or pd.isna(street):
        return None
    city = city.strip()
    street = street.strip()
    home_number = str(home_number).strip()

    address = f"{city} {street} {home_number}" if home_number else f"{city} {street}"
    json_data = {
        'whereValues': [
            address,
        ],
        'locateType': 2,
    }
    result = {}

    try:
        with httpx.Client(timeout=30, verify=False) as client:
            response = client.post('https://ags.govmap.gov.il/Search/ParcelLocate', json=json_data)
            if response.status_code == 200:
                json_obj = response.json()
            if json_obj['errorCode'] == 0 and json_obj['status'] == 0:
                try:
                    result = {
                        'gush': int(json_obj['data']['ResultData']['Values'][0]['Values'][0]),
                        'helka': int(json_obj['data']['ResultData']['Values'][0]['Values'][1]),
                    }
                except:
                    result = {
                        'gush':np.nan,
                        'helka':np.nan,
                    }
            return result
    except requests.exceptions.RequestException as e:
        print(f"An error occurred with {e}")

    return None


def enrich_df_with_location_data(df, nadlan=True):
    df = df.copy()
    df['gush_helka'] = df.progress_apply(
        lambda row: get_gush_helka_api(row['city'], row['street'], row['home_number']), axis=1)


    df['gush'] = df['gush_helka'].apply(lambda x: x.get('gush', '') if x else np.nan)
    df['helka'] = df['gush_helka'].apply(lambda x: x.get('helka', '') if x else np.nan)
    df = df.dropna(subset =['gush'])

    df['Details'] = df.progress_apply(
        lambda row: nominatim_api(row['city'], row['street'], home_number=row['home_number'],gush = row['gush'],helka=row['helka'],build_year=row['build_year']), axis=1)

    df['neighborhood'] = df['Details'].apply(lambda x: x.get('neighborhood', '') if x else np.nan)
    if nadlan:
        df['lat'] = df['Details'].apply(lambda x: x.get('lat', '') if x else np.nan)
        df['long'] = df['Details'].apply(lambda x: x.get('long', '') if x else np.nan)

    df['city'] = df['Details'].apply(lambda x: x.get('city', '') if x else np.nan) # New

    df['x'] = df['Details'].apply(lambda x: x.get('x', '') if x else np.nan)
    df['y'] = df['Details'].apply(lambda x: x.get('y', '') if x else np.nan)
    df['zip'] = df['Details'].apply(lambda x: x.get('zip', '') if x else np.nan)
    df['street'] = df['Details'].apply(lambda x: x.get('street', '') if x else np.nan)
    df['addr_key'] = df['Details'].apply(lambda x: x.get('addr_key', '') if x else np.nan)

    df.drop(columns=['Details','gush_helka'], inplace=True)
    return df
def split_address(address):
    try:
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
        columns = ['addr_key',"key", "city", "neighborhood", "street", "home_number","lat", "long", "type", 'x','y','zip','created_at','gush','helka','build_year','floors']
        df_cache = pd.DataFrame(records, columns=columns)


def fetch_all_cache_data():
    conn = get_db_connection(db_name='nextroof_db')

    global df_cache
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM addr_cache ")
        records = cur.fetchall()
        columns = ['addr_key',"key", "city", "neighborhood", "street", "home_number","lat", "long", "type", 'x','y','zip','created_at','gush','helka','build_year','floors']
        df_cache = pd.DataFrame(records, columns=columns)

def fetch_from_db(cache_key):
    conn = get_db_connection(db_name='nextroof_db')
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM addr_cache WHERE key = %s", (cache_key,))
        record = cur.fetchone()
        if record:
            columns = ['addr_key',"key", "city", "neighborhood", "street", "home_number","lat", "long", "type", 'x','y','zip','created_at','gush','helka','build_year','floors']
            return dict(zip(columns, record))
    return None


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
                return None
        except Exception as e:
            print(data)
        finally:
            conn.close()

def nominatim_api(city, street,gush,helka,build_year, home_number=None):
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
                'floors': None,
            }

        except requests.exceptions.RequestException as e:
            print(f"An error occurred with nominatim_api : {e} , res_dict: {res_dict}")
        addr_key = save_to_db(cache_key, res_dict)
        res_dict['addr_key'] = addr_key
        time.sleep(1)
        return res_dict
    return False

def govmap_addr(addr):
    link = f"https://es.govmap.gov.il/TldSearch/api/DetailsByQuery?query={addr}&lyrs=276267023&gid=govmap"
    result = {}
    try:
        with httpx.Client(timeout=30) as client:
            response = client.get(link)
            if response.status_code == 200:
                json_obj = response.json()
                if json_obj['Error'] == 0:
                    address = split_address(json_obj['data']['ADDRESS'][0]['ResultLable'])
                    result = {
                        'city': address['city'],
                        'street': address['street'],
                        'y': json_obj['data']['ADDRESS'][0]['Y'],
                        'x': json_obj['data']['ADDRESS'][0]['X'],
                        'success': True,
                    }
                    return result

        return {'success': False}
    except httpx.HTTPStatusError as e:
        print(f"An error occurred with GovMap API: {e}")
        return {'success': False}


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

    try:
        with httpx.Client(timeout=30) as client:
            response = client.get(base_url, params=params)
            if response.status_code == 200:
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
    except httpx.RequestError as e:
        print(f"Failed to connect to the Nominatim API: {e}")
        return result


def complete_gush_chelka_db():
    conn = get_db_connection(db_name='nextroof_db')

    query = "SELECT * FROM addr_cache WHERE LENGTH(gush) < 3 OR gush IS NULL"
    df = pd.read_sql_query(query, conn)
    for index, row in df.iterrows():
        # Construct the address
        address = f"{row['street']} {row['home_number']}, {row['city']}"
        gush_helka = get_gush_helka_api(address)

        # Update the database with the new gush and helka
        if gush_helka:
            update_query = """
            UPDATE addr_cache
            SET gush = %s, helka = %s
            WHERE city = %s AND street = %s AND home_number = %s
            """
            with conn.cursor() as cursor:
                cursor.execute(update_query, (gush_helka.get('gush'), gush_helka.get('helka'), row['city'], row['street'], row['home_number']))
                conn.commit()

    conn.close()