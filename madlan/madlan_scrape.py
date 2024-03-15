import pandas as pd
import time
import re
from .madlan_utils import cookies, json_data, headers
import traceback
import logging
from utils.base import add_id_columns
logging.basicConfig(level=logging.WARNING)
from utils.utils_sql import DatabaseManager
import httpx

def find_total_pages(json_data, cookies, headers):
    try:
        with httpx.Client(timeout=30) as client:
            response = client.post('https://www.madlan.co.il/api2', cookies=cookies, headers=headers, json=json_data)
            response.raise_for_status()
            response_json = response.json()
            total_ads = response_json['data']['searchPoiV2']['total']
            total_pages = int(total_ads % 50)
    except Exception as e:
        print(f"Error (find_total_pages) {e}")
        return False
    return total_pages


def get_madlan_data(json_data, cookies, headers):
    extracted_data = []
    total = find_total_pages(json_data, cookies, headers)
    if not total:
        return False
    try:
        offset_value = 0
        limit_value = 50
        for _ in range(total):
            time.sleep(2)
            json_data['variables']['offset'] = offset_value

            with httpx.Client(timeout=10) as client:
                response = client.post('https://www.madlan.co.il/api2', cookies=cookies, headers=headers,
                                       json=json_data)
                response_json = response.json()
                chunk_json = response_json['data']['searchPoiV2']['poi']

            if len(chunk_json) == 0:
                break

            for item in chunk_json:
                if len(item['id']) == 11: # Check if it is a real ad or Advertising
                    image_urls = [image.get('imageUrl') for image in item.get('images', []) if image.get('imageUrl')]
                    splitAddress = item['address'].split(',')
                    home_number_search = re.search(r'\d+', splitAddress[0])
                    homeNumber = home_number_search.group() if home_number_search else None
                    street = splitAddress[0].replace(homeNumber, '').strip() if home_number_search else splitAddress[
                        0].strip()

                    single_data = {
                        "item_id": item['id'],
                        "lat": item['locationPoint']['lat'],
                        "long": item['locationPoint']['lng'],
                        "city": splitAddress[1].strip() if len(splitAddress) > 1 else None,
                        "home_number": homeNumber,
                        "street": street,
                        "rooms": item.get('beds'),
                        "neighborhood": item['addressDetails'].get('neighbourhood'),
                        "floor": item.get('floor'),
                        "build_year": item.get('buildingYear'),
                        "size": item.get('area'),
                        "price": item.get('price'),
                        "condition": item.get('generalCondition'),
                        "last_update": item.get('lastUpdated'),
                        "agency": item['poc'].get('type'),
                        "asset_type": item.get('buildingClass'),
                        "images": image_urls,
                    }
                    extracted_data.append(single_data)

                offset_value += limit_value

    except httpx.HTTPError as e:
        print(f"HTTP request failed: {e}")

    df = pd.DataFrame(extracted_data)
    if not df.empty:
        df = df.dropna(subset=['size'])
        df = df[df['asset_type'].apply(lambda x: x in ['flat', 'gardenapartment', 'roofflat', 'building', 'studio'])]

    return df


def madlan_scrape(local_host=False):
    status = {}
    try:
        df_madlan = get_madlan_data(json_data, cookies, headers)
        print(f"madlan_scrape shape {df_madlan.shape}")
        if not df_madlan.empty:
            df_madlan = add_id_columns(df_madlan, 'city_id', 'city')
            if local_host:
                db_manager = DatabaseManager(table_name='madlan_raw', db_name='nadlan_db', host_name='localhost')
                success, new_rows, updated_rows = db_manager.insert_dataframe(df_madlan, 'item_id')

            db_manager = DatabaseManager(table_name='madlan_raw', db_name='nextroof_db')
            success, new_rows, updated_rows = db_manager.insert_dataframe_batch(df_madlan, batch_size=int(df_madlan.shape[0]),
                                                                                replace=True, pk_columns='item_id')
            status['success'] = success
            status['new_rows'] = new_rows
            status['updated_rows'] = updated_rows

    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        status['success'] = False
        status['error'] = error_message
    return status

