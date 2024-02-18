import pandas as pd
import time
import re
from .madlan_utils import cookies, json_data, headers
import traceback
import requests
from .sql_save_madlan import add_new_deals_madlan_raw
import logging
from utils.base import add_id_columns
logging.basicConfig(level=logging.WARNING)
from utils.utils_sql import DatabaseManager
import httpx
def get_madlan_data(json_data, cookies, headers):
    try:
        response = httpx.post('https://www.madlan.co.il/api2', cookies=cookies, headers=headers, json=json_data, timeout=30)
        response.raise_for_status()
        responseJson = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching initial data: {e}")
        return

    total = responseJson['data']['searchPoiV2']['total']
    print(f"total {total}")
    offset_value = 0
    limit_value = 50
    extracted_data = []

    for page in range(0, total, limit_value):
        if page % 100 == 0:
            print(f"(get_madlan_data) page: {page}")

        time.sleep(3)
        json_data['variables']['offset'] += offset_value
        response = requests.post('https://www.madlan.co.il/api2', cookies=cookies, headers=headers, json=json_data, timeout=10)
        try:
            responseJson = response.json()
            chunckJson = responseJson['data']['searchPoiV2']['poi']
            if len(chunckJson) == 0:
                return pd.DataFrame(extracted_data)
        except:
            print(response)
            break

        for item in chunckJson:
            check_id = len(item['id'])  # Check if it is a real ad or Advertising
            if check_id == 11:
                image_urls = []
                images = item.get('images', [])
                for image in images:
                    url = image.get('imageUrl')
                    if url:
                        image_urls.append(url)
                splitAdress = item['address'].split(',')
                home_number_search = re.search(r'\d+', splitAdress[0])

                # If found, extract home number
                if home_number_search:
                    homeNumber = home_number_search.group()
                    street = splitAdress[0].replace(homeNumber, '').strip()
                else:
                    homeNumber = None
                    street = splitAdress[0].strip()
                try:
                    single_data = {
                        "item_id": item['id'],
                        "lat": item['locationPoint']['lat'],
                        "long": item['locationPoint']['lng'],
                        "city": splitAdress[1],
                        "home_number": homeNumber,
                        "street": street,
                        "rooms": item.get('beds'),
                        "neighborhood": item['addressDetails']['neighbourhood'],
                        "floor": item.get('floor'),
                        "build_year": item.get('buildingYear'),
                        "size": item.get('area'),
                        "price": item.get('price'),
                        "condition": item.get('generalCondition'),
                        "last_update": item.get('lastUpdated'),
                        "agency": item['poc']['type'],
                        "asset_type": item.get('buildingClass'),
                        "images": image_urls,
                    }
                except:
                    return pd.DataFrame(extracted_data)
                extracted_data.append(single_data)
        offset_value += limit_value

    df = pd.DataFrame(extracted_data)
    df = df.dropna(subset=['size'])
    df = df[df['asset_type'].apply(lambda x: x in ['flat', 'gardenapartment', 'roofflat', 'building', 'studio'])]

    return df


def madlan_scrape():
    status = {}
    try:
        df_madlan = get_madlan_data(json_data, cookies, headers)
        print(f"madlan_scrape shape {df_madlan.shape}")
        if not df_madlan.empty:
            df_madlan = add_id_columns(df_madlan, 'city_id', 'city')
            db_manager = DatabaseManager('nadlan_db', 'localhost', 'madlan_raw')
            success, new_rows, updated_rows = db_manager.insert_dataframe(df_madlan, 'item_id')
            status['success'] = success
            status['new_rows'] = new_rows
            status['updated_rows'] = updated_rows

    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        status['success'] = False
        status['error'] = error_message
    return status

