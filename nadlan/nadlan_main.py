from .nadlan_scrape import run_nadlan_scrape
from .nadlan_clean import run_nadlan_clean
from .nadlan_rank import main_nadlan_rank


def nadlan_main(city_dict , num_of_pages=20 ,maintenance= False):
    nadlan_status = {}
    print("nadlan_main_START")
    nadlan_status['status_scrape'] = run_nadlan_scrape(num_of_pages)
    print(f"finish : run_nadlan_scrape")
    nadlan_status['status_clean'] = run_nadlan_clean(maintenance = maintenance)
    nadlan_status['status_rank'] = {}
    for city in city_dict.keys():
        print(f"ranking nadlan: {city}")
        nadlan_status['status_rank'][city] = main_nadlan_rank(city)
    print("nadlan_main_FINISH")
    return nadlan_status
