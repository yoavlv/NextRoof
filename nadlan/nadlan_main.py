from .nadlan_scrape import run_nadlan_scrape
from .nadlan_clean import run_nadlan_clean
from .nadlan_rank import main_nadlan_rank
from typing import Dict, Any
import threading

def nadlan_main(city_dict: Dict[int, str], params: Dict[str, Any]) -> Dict[str, Any]:
    nadlan_status = {'status_scrape': None, 'status_clean': {}, 'status_rank': {}}
    print("nadlan_main_START")

    # Scrape data
    nadlan_status['status_scrape'] = run_nadlan_scrape(city_dict, params['num_of_pages'])

    # Define a thread-safe operation for updating status
    def update_status(status_key: str, city: str, result: Any) -> None:
        with threading.Lock():
            nadlan_status[status_key][city] = result

    # Clean data
    def clean_data(city_id: int, city: str) -> None:
        result = run_nadlan_clean(city_id=city_id, city=city)
        update_status('status_clean', city, result)

    # Run clean or rank tasks in threads
    def run_task_in_threads(task: Any, task_name: str):
        print(f"Starting {task_name}...")
        threads = [threading.Thread(target=task, args=(city_id, city)) for city_id, city in city_dict.items()]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()


    run_task_in_threads(clean_data, "cleaning")

    if params['rank']:
        rank_status = main_nadlan_rank()
        nadlan_status['status_rank'] = rank_status

    print("(nadlan_main) FINISH")
    return nadlan_status
