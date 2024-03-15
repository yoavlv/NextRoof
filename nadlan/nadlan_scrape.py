from .nadlan_utils import payload
import time
import concurrent.futures
import pandas as pd
import httpx
import logging
logging.basicConfig(level=logging.WARNING)
import traceback
from .nadlan_utils import rename_cols_update_data_types
from utils.utils_sql import DatabaseManager
url = "https://www.nadlan.gov.il/Nadlan.REST/Main/GetAssestAndDeals"

def one_thread(url: str, payload: dict, max_pages=10)-> dict:
    last_page = False
    payload['PageNo'] = 1
    df = pd.DataFrame()
    max_retries = 3
    data = {'new_rows': 0, 'conflict_rows': 0}

    while payload['PageNo'] < max_pages and not last_page:
        retry_count = 0
        success = False
        while retry_count < max_retries and not success:
            try:
                with httpx.Client(timeout=120) as client:
                    response = client.post(url, json=payload)
                    response.raise_for_status()
                    json_data = response.json()
                    last_page = json_data.get('IsLastPage', False)

                    if json_data.get('AllResults'):
                        new_df = pd.DataFrame(json_data['AllResults'])
                        df = pd.concat([df, new_df], ignore_index=True)
                    success = True
            except Exception as e:
                logging.warning(f"Attempt {retry_count + 1} failed: {e}")
                retry_count += 1
                time.sleep(10 * retry_count ** 2)

            payload['PageNo'] += 1

        if retry_count == max_retries:
            logging.error("Failed to fetch data after maximum retries.")
            break

    if not df.empty:
        df = preprocess_dataframe(df)
        new_data = insert_to_db(df)
        data = aggregate_insert_results(data, new_data)

    return data
def preprocess_dataframe(df: pd.DataFrame)->pd.DataFrame:
    df.drop_duplicates(inplace=True)
    cols_to_drop = ["DEALDATETIME", "TREND_IS_NEGATIVE", "TREND_FORMAT"]
    df = df.drop(cols_to_drop, axis=1)
    df = rename_cols_update_data_types(df)
    df.columns = [col.lower() for col in df.columns]
    return df

def aggregate_insert_results(existing_data: dict, new_data:dict)->dict:
    existing_data['new_rows'] += new_data['new_rows']
    existing_data['conflict_rows'] += new_data['conflict_rows']
    return existing_data

def insert_to_db(df: pd.DataFrame, local_host=False)->dict:
    if local_host:
        db_manager = DatabaseManager(table_name='nadlan_raw', db_name='nadlan_db', host_name='localhost')
        success, new_rows, conflict_rows = db_manager.insert_dataframe(df, pk_columns='key', replace=False)

    db_manager = DatabaseManager(table_name='nadlan_raw', db_name='nextroof_db')
    success, new_rows, conflict_rows = db_manager.insert_dataframe_batch(df, batch_size=int(df.shape[0]),replace=True, pk_columns='key')
    return {"new_rows":new_rows,"conflict_rows":conflict_rows}


def main_loop(city_code_dict:dict, payload:dict, num_of_pages:int)->dict:
    data = {'new_rows': 0, 'conflict_rows': 0}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(city_code_dict)) as executor:
        futures = {executor.submit(one_thread, url, {**payload, 'ObjectID': str(city_code)}, num_of_pages):
                       city_code for city_code, city in city_code_dict.items()}

        for future in concurrent.futures.as_completed(futures):
            city = futures[future]
            try:
                insert_result = future.result()
                data = aggregate_insert_results(data, insert_result)
            except Exception as e:
                logging.error(f"City {city} data fetch generated an exception: {e}")

    logging.info("All city tasks completed.")
    return data

def run_nadlan_scrape(city_dict:dict,num_of_pages=20, threads=10)->dict:
    nadlan_scrape_status = {'status': False, 'new_rows': 0, 'conflict_rows': 0}
    try:
        data = main_loop(city_dict, payload, num_of_pages=num_of_pages)
        nadlan_scrape_status.update(status=True, new_rows=data['new_rows'], conflict=data['conflict_rows'])
    except Exception as e:
        nadlan_scrape_status.update(status=False, error=f"{e}\n{traceback.format_exc()}")
    print(nadlan_scrape_status)
    return nadlan_scrape_status

