import sys
import re
sys.path.append('C:/Users/yoavl/NextRoof/')
import pandas as pd
import logging
logging.basicConfig(level=logging.WARNING)
from tqdm import tqdm
tqdm.pandas()
import httpx
from dev import get_db_connection


def get_gush_helka_api(city_id, street_id, home_number):
    params = {
        'idCity': city_id,
        'streetCode': street_id,
        'HouseNo': home_number,
    }
    url = 'https://www.tabucheck.co.il/getGoshHelka.asp'

    try:
        response = httpx.get(url, params=params, timeout=10)
        response.raise_for_status()

        pattern = r'<strong>(.*?)</strong>'
        matches = re.findall(pattern, response.text)

        if len(matches) == 3:
            return {
                'gush': matches[1],
                'helka': matches[2],
            }

        else:
            print("Expected data not found in response (invalid address.")
            return None
    except httpx.RequestError as e:
        print(f"An error occurred while requesting {e.request.url!r}.")
    except httpx.HTTPStatusError as e:
        print(f"Error response {e.response.status_code} while requesting {e.request.url!r}.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return None


def split_address(address):
    try:
        match = re.match(r'([\u0590-\u05FF\'"׳״\-\s]+)\s(\d+).*?,\s*([\u0590-\u05FF\s-]+)', address)
        if not match:
            raise ValueError(f"Invalid address format: {address}")

        street, home_number, city = match.groups()
        return {'street': street.strip(), 'home_number': home_number, 'city': city.strip()}
    except Exception as e:
        raise Exception(f"Error parsing address: {e}")



def complete_gush_chelka_db():
    conn = get_db_connection(db_name='nextroof_db')

    query = "SELECT * FROM addr_cache WHERE LENGTH(gush) < 3 OR gush IS NULL"
    df = pd.read_sql_query(query, conn)
    for index, row in df.iterrows():
        # Construct the address
        gush_helka = get_gush_helka_api(row['city_id'],row['street_id'], row['home_number'])

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