from network import payload
import pandas as pd
import requests
import bs4
import json
import csv
import time
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import ssl
print(ssl.OPENSSL_VERSION)
url =  "https://www.nadlan.gov.il/Nadlan.REST/Main/GetAssestAndDeals"
def fetch_data(url, payload, page_no, lost_pages):
    payload['PageNo'] = page_no
    if page_no % 100 == 0:
        print(f'page:{page_no}/5200')
    try:
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        session = requests.Session()
        session.mount('https://', adapter)
        response = session.post(url, json=payload, timeout=120)
        json_data = response.json()

    except Exception as e:
        print(f"Error: {e}\npage{page_no}")
#         lost_pages.append(page_no)
        time.sleep(5)
        try:
            response = session.post(url, json=payload, timeout=120)
            json_data = response.json()
        except Exception as e:
            print(f"Error: {e}\n page{page_no}")
#             lost_pages.append(page_no)
            return None

    return pd.DataFrame(json_data['AllResults'])


def get_nandlan_data(url, payload, number_of_pages, max_threads=10):
    '''
    This function gets all real estate deals in Tel-Aviv from nadlan.gov.il - from 1998 until today (- 2 months)
    We use threads to optimize the running time of this function.
    '''
    df = pd.DataFrame()
    lost_pages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for page_no in range(1, number_of_pages):
            futures.append(executor.submit(fetch_data, url, payload, page_no, lost_pages))
        for future in concurrent.futures.as_completed(futures):
            new_df = future.result()
            if new_df is not None and new_df.shape[0] != 0:
                df = pd.concat([df, new_df], ignore_index=True)
    if df.shape[0] > 10:
        cols = ["DEALDATETIME", "TREND_IS_NEGATIVE", "TREND_FORMAT"]
        df.drop(cols, axis=1, inplace=True)

    return df



def update_nadlan_df(df, print_summary=False):
    nadlan_old = pd.read_csv("../Data/Nadlan.csv")
    nadlan_old.drop_duplicates(inplace=True)

    nadlan_old_shape = nadlan_old.shape[0]

    nadlan_update = nadlan_old._append(df, ignore_index=True)
    nadlan_update.drop_duplicates(inplace=True)

    if print_summary:
        new_rows = abs(nadlan_update.shape[0] - nadlan_old_shape)
        print(f"Total new rows: {new_rows}")
        print(f'nadlan_old shape: {nadlan_update.shape}')

    nadlan_update.to_csv("../Data/Nadlan.csv", index=False)
    return nadlan_update

nadlan_df = get_nandlan_data(url, payload, 10, 1)
df_update = update_nadlan_df(nadlan_df, print_summary=True)
print(f'df_update shape: {df_update.shape}')