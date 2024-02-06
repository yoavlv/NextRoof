from .madlan_scrape import madlan_scrape
from .madlan_clean import main_madlan_clean
from .madlan_rank import main_madlan_ranking
from .madlan_calc import main_madlan_calc
from .madlan_utils import check_availability, headers_delete
from .sql_reader_madlan import  delete_records_by_item_ids
import logging
logging.basicConfig(level=logging.WARNING)

def madlan_main(city_dict,clean = False):
    madlan_status = {}
    if clean:
        clean_old_ads(headers_delete)
    madlan_status['status_scrape'] = madlan_scrape()
    madlan_status['status_clean'] = {}
    madlan_status['status_rank'] = {}
    madlan_status['status_calc'] = {}

    if madlan_status['status_scrape']['success'] == False:
        return madlan_status

    for city in city_dict.items():
        print(f"madlan_main:{city[0]}")
        madlan_status['status_clean'][city[0]] = main_madlan_clean(city[0])
        madlan_status['status_rank'][city[0]] = main_madlan_ranking(city[0])
        madlan_status['status_calc'][city[0]] = main_madlan_calc(city)
    return madlan_status


def clean_old_ads(headers_delete):
    try:
        to_delete = check_availability(headers=headers_delete)
        if len(to_delete) >= 1:
            delete_records_by_item_ids(item_ids=to_delete, db_name='nextroof_db')
            delete_records_by_item_ids(item_ids=to_delete, db_name='nadlan_db')
            delete_records_by_item_ids(item_ids=to_delete, db_name='nextroof_db', host_name='13.50.98.191')
    except Exception as e:
        print(e)