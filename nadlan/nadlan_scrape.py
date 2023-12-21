import sys
sys.path.append('/nadlan')
from .nadlan_utils import payload ,city_code
import time
import concurrent.futures
import pandas as pd
from monitor import monitor_data
import httpx
from .sql_save_nadlan import add_new_deals_nadlan_raw
import logging
logging.basicConfig(level=logging.WARNING)
import traceback

url = "https://www.nadlan.gov.il/Nadlan.REST/Main/GetAssestAndDeals"

def fetch_data(url, payload, page_no):
    payload['PageNo'] = page_no
    if page_no % 200 == 0:
        print(f'page:{page_no}/5200')

    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            with httpx.Client(timeout=120) as client:
                response = client.post(url, json=payload)
                response.raise_for_status()
                json_data = response.json()
                return pd.DataFrame(json_data['AllResults'])

        except (httpx.HTTPStatusError, httpx.RequestError) as e:
            print(f"Attempt {retry_count + 1} failed: {e}")
            retry_count += 1
            time.sleep(0.5 * retry_count)  # Exponential backoff

    print(f"Failed to fetch data after {max_retries} retries.")
    return None
def get_nandlan_data(url, payload, number_of_pages, max_threads=10):
    df = pd.DataFrame()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = []
        for page_no in range(1, number_of_pages):
            futures.append(executor.submit(fetch_data, url, payload, page_no))
        for future in concurrent.futures.as_completed(futures):
            new_df = future.result()
            if new_df is not None and new_df.shape[0] != 0:
                df = pd.concat([df, new_df], ignore_index=True)
    if df.shape[0] > 10:
        cols_to_drop = ["DEALDATETIME", "TREND_IS_NEGATIVE", "TREND_FORMAT"]
        return df.drop(cols_to_drop, axis=1)
    return df


def get_city_data(payload,city_code,num_of_pages=5):
    payload['ObjectID'] = str(city_code)
    new_df = get_nandlan_data(url , payload ,number_of_pages=num_of_pages,max_threads=10)
    return new_df


def main_loop(city_code_list,payload,num_of_pages):
    df = pd.DataFrame()
    for code in city_code_list:
        new_df = get_city_data(payload,code , num_of_pages =num_of_pages)
        df = pd.concat([df, new_df], ignore_index=True)
        df.drop_duplicates(inplace=True)
        df = df[df['FULLADRESS'].str.len() > 5]
    return df

def run_nadlan(num_of_pages = 20 ,threads = 10 ):
    nadlan_scrape_status = {}
    try:
        nadlan_df = main_loop(list(city_code.values()), payload , num_of_pages = num_of_pages )
        print(nadlan_df.shape)
        data = add_new_deals_nadlan_raw(nadlan_df)
        nadlan_scrape_status['status'] = True
        nadlan_scrape_status['new_rows'] = data['new_rows']
        nadlan_scrape_status['conflict'] = data['conflict_rows']
    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        nadlan_scrape_status['status'] = False
        nadlan_scrape_status['error'] = error_message

    return nadlan_scrape_status

