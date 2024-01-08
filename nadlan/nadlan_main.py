from .nadlan_scrape import run_nadlan_scrape
from .nadlan_clean import run_nadlan_clean
from .nadlan_rank import main_nadlan_rank
import time


def nadlan_main(city_dict, num_of_pages=20, maintenance=False, rank = False):
    nadlan_status = {}
    print("nadlan_main_START")
    nadlan_status['status_scrape'] = run_nadlan_scrape(num_of_pages)
    nadlan_status['status_clean'] = run_nadlan_clean(maintenance=maintenance)
    nadlan_status['status_rank'] = {}
    if rank:
        print("start ranking...........")
        for city in city_dict.keys():
            print(f"ranking nadlan: {city}")
            start_time = time.time()
            nadlan_status['status_rank'][city] = main_nadlan_rank(city)
            end_time = time.time()
            print(f"finish: main_nadlan_rank for {city}, Runtime: {end_time - start_time:.1f} seconds")

    print("nadlan_main_FINISH")
    return nadlan_status