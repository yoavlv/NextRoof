from dataProcess import CleanData
import pandas as pd


df_madlan = pd.read_csv('C:/Users/yoavl/NextRoof/Data/madlan_data.csv')
madlan = CleanData(df_madlan, 'madlan')
madlan.saveDataFrame('Data/test1.csv')
print(madlan.df.shape)
df_yad2 = pd.read_csv('C:/Users/yoavl/NextRoof/Data/yad_2_data.csv')
yad2 = CleanData(df_yad2, 'yad2')
yad2.saveDataFrame('Data/test2.csv')


