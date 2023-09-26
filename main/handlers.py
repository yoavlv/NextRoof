import sys
sys.path.append('C:/Users/yoavl/NextRoof/Scraping')
sys.path.append('C:/Users/yoavl/NextRoof/Algorithms')
sys.path.append('C:/Users/yoavl/NextRoof/sql')
from sql.push import add_new_deals_madlan ,add_new_deals_yad2 ,add_new_deals_nadlan
from Algorithms.calc_results import calc_results
from Scraping.madlan import run_madlan
from Scraping.nadlan_scrape import run_nadlan
from Scraping.yad2 import run_yad2
from dataProcess import CleanData
from Clean.data_cleaning_nadlan import run_nadlan_clean
import pandas as pd
from monitor import monitor_data , find_errors
import traceback
def nadlan_handler():
    run_nadlan(5, pages=50)
    run_nadlan_clean()
    add_new_deals_nadlan()

def madlan_handler():
    run_madlan()
    data_cleaning('madlan')
    calc_results(madlan=True)
    add_new_deals_madlan()
def yad2_handler():
    run_yad2()
    if monitor_data['yad2']['New_data'] ==0:
        print("yad2 empty")
        return
    data_cleaning('yad2')
    calc_results(yad2=True)
    add_new_deals_yad2()

def data_cleaning(name):
    try:
        df = pd.read_csv(f'C:/Users/yoavl/NextRoof/Data/{name}_data_p.csv', index_col=0)
        data_instance = CleanData(df, name)
        data_instance.saveDataFrame(f'C:/Users/yoavl/NextRoof/Data/{name}_data_clean_p.csv')

        monitor_data['Clean'][name]['Total_size'] = data_instance.df.shape
        monitor_data['Clean'][name]['status'] = 'Success'
    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        print(error_message)
        monitor_data['Clean'][name]['status'] = 'Fail'
        monitor_data['Clean'][name]['error'] = e