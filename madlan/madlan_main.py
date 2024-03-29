from .madlan_scrape import madlan_scrape
from .madlan_clean import main_madlan_clean
from .madlan_calc import main_madlan_calc
from .madlan_utils import check_availability, headers_delete
from .sql_reader_madlan import delete_records_by_item_ids
from utils.utils_sql import sql_script
import threading


def madlan_main(city_dict, params):
    madlan_status = {'status_scrape': None, 'status_clean': {}, 'status_rank': {}, 'status_calc': {}}
    print(f"(madlan_main) START clean={params['clean']}")

    if params['clean']:
        print("clean_old_ads: START")
        clean_old_ads(headers_delete)
        print("clean_old_ads: FINISH")

    # Scrape data
    # madlan_status['status_scrape'] = madlan_scrape()
    # if not madlan_status['status_scrape'].get('success', False):
    #     return madlan_status
    #
    # if madlan_status['status_scrape']['success'] == False:
    #     return madlan_status

    print(f"madlan_main: clean")
    for city_id, city in city_dict.items():
        madlan_status['status_clean'][city] = main_madlan_clean(city_id=city_id, city=city)

    print(f"madlan_main: rank")
    madlan_rank = sql_script('madlan.sql')
    madlan_status['status_rank'] = madlan_rank

    # for city_id, city in city_dict.items():
    #     madlan_status['status_rank'][city] = main_madlan_ranking(city_id=city_id, city=city)

    print(f"madlan_main: calc")
    for city_id, city in city_dict.items():
        madlan_status['status_calc'][city] = main_madlan_calc(city_id=city_id, city=city)

    return madlan_status



def clean_old_ads(headers_delete):
    try:
        to_delete = check_availability(headers=headers_delete)
        if len(to_delete) >= 1:
            # delete_records_by_item_ids(item_ids=to_delete, db_name='nadlan_db', host_name='localhost')
            # delete_records_by_item_ids(item_ids=to_delete, db_name='nextroof_db', host_name='13.50.98.191')
            delete_records_by_item_ids(item_ids=to_delete, db_name='nextroof_db', host_name='nextroof-rds.cboisuqgg7m3.eu-north-1.rds.amazonaws.com')

    except Exception as e:
        print(e)