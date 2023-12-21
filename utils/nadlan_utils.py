import requests
import sys
import pandas as pd
import logging
logging.basicConfig(level=logging.WARNING)
import psycopg2
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re

df_cache = None

def create_db_connection():
    try:
        conn = psycopg2.connect(
            host="localhost",
            dbname='nextroof_db',
            user='postgres',
            password="43234323",
            port=5432
        )
        return conn
    except Exception as e:
        return None


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
    conn = create_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM addr_cache WHERE city = %s", (city,))
        records = cur.fetchall()
        columns = ['addr_key', "key", "city", "neighborhood", "street", "home_number", "lat", "long", "type", 'x', 'y',
                   'zip', 'created_at', 'gush', 'helka', 'build_year', 'floors']
        df_cache = pd.DataFrame(records, columns=columns)


def fetch_all_cache_data():
    conn = create_db_connection()
    global df_cache
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM addr_cache ")
        records = cur.fetchall()
        columns = ['addr_key', "key", "city", "neighborhood", "street", "home_number", "lat", "long", "type", 'x', 'y',
                   'zip', 'created_at', 'gush', 'helka', 'build_year', 'floors']
        df_cache = pd.DataFrame(records, columns=columns)


def fetch_from_db(cache_key):
    conn = create_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM addr_cache WHERE key = %s", (cache_key,))
        record = cur.fetchone()
        if record:
            columns = ['addr_key', "key", "city", "neighborhood", "street", "home_number", "lat", "long", "type", 'x',
                       'y', 'zip', 'created_at', 'gush', 'helka', 'build_year', 'floors']
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
    conn = create_db_connection()
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
                    return None
        except Exception as e:
            print(f"Failed to save data to DB: {e}")
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


def govmap_addr(addr):
    link = f"https://es.govmap.gov.il/TldSearch/api/DetailsByQuery?query={addr}&lyrs=276267023&gid=govmap"
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session = requests.Session()
    session.mount('https://', adapter)
    result = {}

    try:
        response = session.get(link, timeout=10)
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
    except requests.exceptions.RequestException as e:
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
        response = requests.get(base_url, params=params, timeout=30)
    except requests.ConnectionError:
        print("Failed to connect to the Nominatim API")
        return result
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