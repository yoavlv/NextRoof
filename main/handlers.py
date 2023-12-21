# import sys
# sys.path.append('C:/Users/yoavl/NextRoof/Scraping')
# sys.path.append('C:/Users/yoavl/NextRoof/Algorithms')
# sys.path.append('C:/Users/yoavl/NextRoof/sql')
# from Algorithms.calc_results import calc_results
# from Scraping.nadlan_scrape import run_nadlan
# from Scraping.yad2 import run_yad2
# from dataProcess import CleanData
# import pandas as pd
# from monitor import monitor_data , find_errors
# import traceback



city_dict = {
    'אשדוד': 1100,
    'באר שבע': 1200,
    'בני ברק': 1300,
    'בת ים': 1400,
    'גבעתיים': 1500,
    # 'הרצלייה': 1600,
    'חולון': 1700,
    'חיפה': 1800,
    'ירושלים': 1900,
    'כפר סבא': 2000,
    'נתניה': 2100,
    'פתח תקווה': 2200,
    'ראשון לציון': 2300,
    'רמת גן': 2400,
    'רמת השרון': 2500,
    'רעננה': 2600,
    'תל אביב-יפו': 2700
}


# def nadlan_handler():
#     run_nadlan(5, pages=50)
#     run_nadlan_clean()
#     add_new_deals_nadlan()
#
# def madlan_handler():
#     # run_madlan()
#     data_cleaning('madlan')
#     calc_results(madlan=True)
#     add_new_deals_madlan()
# def yad2_handler():
#     run_yad2()
#     if monitor_data['yad2']['New_data'] ==0:
#         print("yad2 empty")
#         return
#     data_cleaning('yad2')
#     calc_results(yad2=True)
#     add_new_deals_yad2()
#
#
# def data_cleaning(name):
#     try:
#         df = pd.read_csv(f'C:/Users/yoavl/NextRoof/Data/{name}_data_p.csv', index_col=0)
#         df_list = []
#         for city in df['City'].unique():
#             print(city)
#             data_instance = CleanData(df, name,city)
#             df_list.append(data_instance.get)
#
#         merge_df = pd.concat(df_list, ignore_index=True)
#         path = f'C:/Users/yoavl/NextRoof/Data/{name}_data_clean_p.csv'
#         merge_df.pd.to_csv()(path)
#
#         monitor_data['Clean'][name]['Total_size'] = merge_df.shape
#         monitor_data['Clean'][name]['status'] = 'Success'
#     except Exception as e:
#         error_message = f"{e}\n{traceback.format_exc()}"
#         print(error_message)
#         monitor_data['Clean'][name]['status'] = 'Fail'
#         monitor_data['Clean'][name]['error'] = e


