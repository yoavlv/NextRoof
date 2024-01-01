import pandas as pd
import time
import re
from .madlan_utils import cookies, json_data, headers
import traceback
from requests.adapters import HTTPAdapter
import requests
from urllib3.util.retry import Retry
from .sql_save_madlan import add_new_deals_madlan_raw
import logging
logging.basicConfig(level=logging.WARNING)

def get_madlan_data(json_data, cookies, headers):
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    try:
        response = session.post('https://www.madlan.co.il/api2', cookies=cookies, headers=headers, json=json_data)
        response.raise_for_status()
        responseJson = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching initial data: {e}")
        return

    total = responseJson['data']['searchPoiV2']['total']
    offset_value = 0
    limit_value = 50
    extracted_data = []

    for _ in range(0, total, limit_value):
        time.sleep(15)
        json_data['variables']['offset'] += offset_value
        response = requests.post('https://www.madlan.co.il/api2', cookies=cookies, headers=headers, json=json_data)
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
            if check_id == 11:  # and 'תל אביב' in item['address']:
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
                        "Item_id": item['id'],
                        "Lat": item['locationPoint']['lat'],
                        "Long": item['locationPoint']['lng'],
                        "City": splitAdress[1],
                        "Home_number": homeNumber,
                        "Street": street,
                        "Rooms": item.get('beds'),
                        "Neighborhood": item['addressDetails']['neighbourhood'],
                        "Floor": item.get('floor'),
                        "Build_year": item.get('buildingYear'),
                        "Size": item.get('area'),
                        "Price": item.get('price'),
                        "Condition": item.get('generalCondition'),
                        "Last_update": item.get('lastUpdated'),
                        "Agency": item['poc']['type'],
                        "Asset_type": item.get('buildingClass'),
                        "Images": image_urls,
                    }
                except:
                    return pd.DataFrame(extracted_data)
                extracted_data.append(single_data)
        offset_value += limit_value

    df = pd.DataFrame(extracted_data)
    df = df.dropna(subset=['Size'])
    df = df[df['Asset_type'].apply(lambda x: x in ['flat', 'gardenapartment', 'roofflat', 'building', 'studio'])]

    print(f"scraping madaln shape {df.shape}")
    return df


def madlan_scrape():
    status = {}
    try:
        df_madlan = get_madlan_data(json_data, cookies, headers)
        if not df_madlan.empty:
            data = add_new_deals_madlan_raw(df_madlan)
            status['success'] = True
            status['new_rows'] = data['new_rows']
            status['updated_rows'] = data['updated_rows']

    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        status['success'] = False
        status['error'] = error_message
    return status

