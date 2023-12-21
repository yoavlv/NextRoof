from .madlan_scrape import madlan_scrape
from .madlan_clean import main_madlan_clean
from .madlan_rank import main_madlan_ranking
from .madlan_calc import main_madlan_calc
import logging
logging.basicConfig(level=logging.WARNING)
def madlan_main(city_dict):
    madlan_status = {}
    madlan_status['status_scrape'] = madlan_scrape()
    madlan_status['status_clean'] = {}
    madlan_status['status_rank'] = {}
    madlan_status['status_calc'] = {}
    for city in city_dict.items():
        print(f"madlan_main:{city[0]}")
        madlan_status['status_clean'][city[0]] = main_madlan_clean(city[0])
        madlan_status['status_rank'][city[0]] = main_madlan_ranking(city[0])
        madlan_status['status_calc'][city[0]] = main_madlan_calc(city)
    return madlan_status