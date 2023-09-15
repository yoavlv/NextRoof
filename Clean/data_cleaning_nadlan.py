#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
import bs4
from bs4 import BeautifulSoup
import csv
import numpy as np
from datetime import datetime
from datetime import timedelta
import math
import re


# In[5]:


df = pd.read_csv("../Data/Nadlan.csv")
print(df.shape)
def fill_missing_addresses(df):
    df2 = pd.read_csv("../Data/Addresses.csv")
    
    df1_gov = pd.read_csv("../Data/Real_Estate_TLV_GOVMAPS_1.csv", index_col=0)
    df2_gov = pd.read_csv("../Data/Real_Estate_TLV_GOVMAPS_2.csv", index_col=0)
    df_gov = pd.merge(df1_gov, df2_gov, how='outer')
    df_gov[['Gush', 'Helka', 'Tat']] = df_gov['GUSHHELKATAT'].str.split('-|/', n=2, expand=True).astype(np.int32)
    
    df[['Gush', 'Helka', 'Tat']] = df['GUSH'].str.split('-|/', n=2, expand=True).astype(np.int32)
    df = df.drop(columns='GUSH', axis=1)
    missing_addresses = df.loc[df['DISPLAYADRESS'].isna(), ['Gush', 'Helka']]
    print(missing_addresses.shape[0])
    
    # first method 
    for index, row in missing_addresses.iterrows():
        gush = row['Gush']
        helka = row['Helka']
        
        match = df.loc[(df['Gush'] == gush) & (df['Helka'] == helka), 'DISPLAYADRESS']
        
        if not match.empty:
            df.loc[index, 'DISPLAYADRESS'] = match.values[0]
       
    
    missing_addresses = df.loc[df['DISPLAYADRESS'].isna(), ['Gush', 'Helka']]
    print(missing_addresses.shape[0])
    
    # second method 
    for index, row in missing_addresses.iterrows():
        gush = row['Gush']
        helka = row['Helka']
        
        match = df2.loc[(df2['ms_gush'] == gush) & (df2['ms_chelka'] == helka), 't_ktovet_melea']
        match2 = df_gov.loc[(df_gov['Gush'] == gush) & (df_gov['Helka'] == helka), 'ADDRESS']

        if not match.empty:
            df.loc[index, 'DISPLAYADRESS'] = match.values[0]
        
        elif not match2.empty:
            df.loc[index, 'DISPLAYADRESS'] = match2.values[0]
            
    missing_addresses = df.loc[df['DISPLAYADRESS'].isna(), ['Gush', 'Helka']]
    print(missing_addresses.shape[0])
    return df

# df = fill_missing_addresses(df)
# print(f"shape: {df.shape}")



# In[13]:


def rename_cols_update_data_types(df):
    df.drop(columns=["FULLADRESS", "PROJECTNAME", 'KEYVALUE', 'POLYGON_ID', 'TYPE'], axis=1 , inplace =True)
    df = df.dropna(subset=['DEALAMOUNT', 'DEALNATURE','DISPLAYADRESS']).reset_index(drop=True)
    types = ["nan","מיני פנטהאוז","מגורים","בית בודד","דופלקס","קוטג' חד משפחתי","קוטג' דו משפחתי",'מלונאות','חנות','קרקע למגורים','קבוצת רכישה - קרקע מגורים','None','אופציה','קבוצת רכישה - קרקע מסחרי','חניה','מסחרי + מגורים','דירת נופש','דיור מוגן','קומבינציה','מבנים חקלאיים','תעשיה','מסחרי + משרדים','בניני ציבור','חלוקה/יחוד דירות','מחסנים','אחר','בית אבות','עסק',"קוטג' טורי",'ניוד זכויות בניה','משרד','ללא תיכנון','מלונאות ונופש','משרדים + מגורים','מלאכה',]

    df = df[~df['DEALNATUREDESCRIPTION'].isin(types)].reset_index(drop=True)
    
    df['Home_number'] = pd.to_numeric(df['DISPLAYADRESS'].str.extract('([0-9]+)', expand=False), errors='coerce').astype(np.int32)
    df['Street'] = df['DISPLAYADRESS'].str.replace('\d+', '', regex=True).str.strip()

    df = df.drop(columns='DISPLAYADRESS', axis=1)
    
    df['NEWPROJECTTEXT'] = df['NEWPROJECTTEXT'].fillna(0).astype(int)

    df.rename(columns={'DEALAMOUNT': 'Price', 'DEALNATURE': 'Size', 'DEALNATUREDESCRIPTION': 'Type',
                       'ASSETROOMNUM': 'Rooms', 'NEWPROJECTTEXT': 'New', 'BUILDINGFLOORS': 'Floors',
                       'BUILDINGYEAR': 'Build_year', 'YEARBUILT': 'Rebuilt', 'DEALDATE': 'Date'},inplace=True)
    df['Size'] = df['Size'].fillna('0').astype(np.int32)
    df['Price'] = df['Price'].str.replace(',', '').astype(np.int32)
    df['Build_year'] = df['Build_year'].fillna('0').astype(np.int32)
    df['Rebuilt'] = df['Rebuilt'].fillna('0').astype(np.int32)
    
    df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y').dt.strftime('%d.%m.%Y')
    df['Year'] = pd.to_datetime(df['Date']).dt.strftime('%Y').astype(np.int32)
    
    return df

df = rename_cols_update_data_types(df)
print(df.shape)


# In[14]:


floors = {-1:'מרתף', 0:'קרקע', 1:'ראשונה', 2:'שניה', 3:'שלישית', 4:'רביעית', 5:'חמישית', 6:'שישית', 7:'שביעית', 8:'שמינית', 9:'תשיעית', 10:'עשירית', 11:'אחת עשרה', 12:'שתים עשרה', 13:'שלוש עשרה', 14:'ארבע עשרה', 15:'חמש עשרה', 16:'שש עשרה', 17:'שבע עשרה', 18:'שמונה עשרה', 19:'תשע עשרה', 20:'עשרים', 21:'עשרים ואחת', 22:'עשרים ושתיים', 23:'עשרים ושלוש', 24:'עשרים וארבע', 25:'עשרים וחמש', 26:'עשרים ושש', 27:'עשרים ושבע', 28:'עשרים ושמונה', 29:'עשרים ותשע', 30:'שלושיים', 31:'שלושיים ואחת', 32:'שלושיים ושתיים', 33:'שלושים ושלוש', 34:'שלושים וארבע', 35:'שלושים וחמש', 36:'שלושים ושש', 37:'שלושים ושבע', 38:'שלושים ושמונה', 39:'שלושים ותשע', 40:'ארבעים'}


def floor_to_numeric(df, floors):
    floor_dict = {value: key for key, value in floors.items()}
    for floor, num in floor_dict.items():
        df.loc[df['FLOORNO'] == floor, 'FLOORNO'] = num
    df = df[df['FLOORNO'].isin(list(floor_dict.values()))].reset_index(drop=True)
    df.rename(columns = {'FLOORNO':'Floor'}, inplace = True)
    return df

df = floor_to_numeric(df,floors)
print(df.shape)


# In[15]:


# df_b = pd.read_csv("Data/Buildings.csv")
df_a = pd.read_csv("../Data/Addresses.csv")

def edit_addresses(df):
    df = df.dropna(subset=['ms_gush','ms_chelka']).reset_index(drop=True)
    df.rename(columns = {'ms_gush':'Gush'}, inplace = True)
    df['Gush'] = df['Gush'].astype(np.int32)
    
    df.rename(columns = {'ms_chelka':'Helka'}, inplace = True)
    df['Helka'] = df['Helka'].astype(np.int32)
    
    df.rename(columns = {'t_rechov':'Street'}, inplace = True)
    df.rename(columns = {'ms_bayit':'Home_number'}, inplace = True)

    return df


df_a = edit_addresses(df_a)


# In[16]:


def megre_df(df1,df2):
    df1 = df1.dropna(subset=['Home_number','Street']).reset_index(drop=True)
    df2 = df2.dropna(subset=['Home_number','Street']).reset_index(drop=True)
    
    df1['Gush'] = df1['Gush'].astype(str)
    df1['Helka'] = df1['Helka'].astype(str)
    df1['Home_number'] = df1['Home_number'].astype(str)


    df2['Gush'] = df2['Gush'].astype(str)
    df2['Helka'] = df2['Helka'].astype(str)
    df2['Home_number'] = df2['Home_number'].astype(str)
    
    df1['m'] = df1['Gush'] + df1['Helka'] + df1['Home_number']
    df2['m'] = df2['Gush'] + df2['Helka'] + df2['Home_number']
    
    df2 = df2.drop(columns=['Street','Home_number','Gush','Helka'], axis=1)
    df2.drop_duplicates(subset =['m'], inplace =True)
    merged = pd.merge(df1, df2, on=['m'],how='left')
    
    cols = ['m','t_ktovet_melea','t_bayit_veknisa','knisa','k_status_hesder','k_rechov','lon','lat','id_ktovet']
    merged = merged.drop(columns=cols, axis=1)
    
    merged.rename(columns = {'x':'Long'}, inplace = True)
    merged.rename(columns = {'y':'Lat'}, inplace = True)

    return merged

df_m = megre_df(df,df_a)


# In[17]:


def calc_distance_from_the_see_TLV(X_coordinate, Y_coordinate):
    north_x = 180471
    north_y = 672391
    south_x = 177333
    south_y = 663016
    
    m = (south_y - north_y) / (south_x - north_x)   
    b = north_y - (m * north_x)
    
    numerator = abs(m * X_coordinate - Y_coordinate + b)
    denominator = math.sqrt(m**2 + 1)
    return numerator / denominator


def distance_from_sea_tlv(df):
    df = df.dropna(subset=['Lat', 'Long']).reset_index(drop=True)
    latitudes = df['Long'].astype(float)
    longitudes = df['Lat'].astype(float)
    distances = [calc_distance_from_the_see_TLV(lat, lon) for lat, lon in zip(latitudes, longitudes)]
    df['Distance_sea'] = distances
    df['Distance_sea'] = df['Distance_sea'].astype(np.int32)
    return df

def calc_distance_from_train_station(df):
    stations = [(179820.47, 662424.54), (180619, 664469.56), (181101.44, 665688.78), (181710.96, 667877.05)]
    df['Train'] = [int(min(abs(station[0]-row['Long']) + abs(station[1]-row['Lat']) for station in stations)) for _, row in df.iterrows()]
    return df
    


# df_m = distance_from_sea_tlv(df_m)
# df_m = calc_distance_from_train_station(df_m)
# print(df_m.shape)


# In[18]:


def add_neighborhood_column(df):
    def add_neighborhoods():
        df = pd.read_csv("../Data/Real_Estate_TLV_GOVMAPS_1.csv", index_col=0)
        neighborhoods = {}
        for index, row in df.iterrows():
            neighborhood = row['NEIGHBORHOOD']
            street = row['STREENNAME']
            if neighborhood not in neighborhoods:
                neighborhoods[neighborhood] = set()
            neighborhoods[neighborhood].add(street)
        return neighborhoods
    
    neighborhoods = add_neighborhoods()
    
    df['Neighborhood'] = np.nan
    for index, row in df.iterrows():
        street = row['Street']
        
        for neighborhood, streets in neighborhoods.items():
            # Check if the street is in the set of streets for the neighborhood
            if street in streets:
                # If it is, assign the neighborhood to the 'neighborhood' column for this row
                df.loc[index, 'Neighborhood'] = neighborhood
                break # Break out of the inner loop once we find a match
                
    df['Neighborhood'] = df['Neighborhood'].str.replace('-',' ')
    t = "'"
    df['Neighborhood'] = df['Neighborhood'].str.replace(t,"")
    df['Neighborhood'] = df['Neighborhood'].str.replace('נוה','נווה')
    df = df.dropna(subset=['Neighborhood']).reset_index(drop=True)
    return df

def add_missing_floors(df):
    df['Floors'] = df['Floors'].where(df['Floors'].notna(), np.nan)
    max_floors_by_group = df.groupby(['Street', 'Home_number'])['Floors'].transform('max')
    df['Floors'] = df['Floors'].combine_first(max_floors_by_group)
    df['Floors'].fillna(df['Floor'], inplace=True)
    df['Floors'] = np.where(df['Floor'] > df['Floors'], df['Floor'], df['Floors'])
    df['Floors'] = df['Floors'].astype(np.int32) 
    return df

def fill_missing_type(df):
    df['UniqueId'] = df['UniqueId'].where(df['Type'].notna(), np.nan)
    most_frequent_type_by_group = df.groupby(['UniqueId'])['Type'].apply(lambda x: x.mode().iat[0])
    df['Type'] = df['Type'].combine_first(most_frequent_type_by_group)
    return df.dropna(subset=['Type'])


def fill_rooms(df):
    df['Rooms'] = df.apply(lambda row: round(row['Size'] / 30) if pd.isna(row['Rooms']) else row['Rooms'], axis=1)
    df['Rooms'] = df['Rooms'].astype(np.int32) 
    return df


def fill_missing_neighborhood(df):
    def euclidean_distance(lat1, lon1, lat2, lon2):
        dx = (lon2 - lon1)**2
        dy = (lat2 - lat1)**2
        distance = np.sqrt(dx + dy)
        return distance

    # Find rows with missing 'Neighborhood' values
    missing_neighborhood = df[df['Neighborhood'].isna()]
    not_missing_neighborhood = df.dropna(subset=['Neighborhood'])

    for idx, row in missing_neighborhood.iterrows():
        lat = row['Lat']
        lon = row['Long']
        distances = euclidean_distance(lat, lon, not_missing_neighborhood['Lat'], not_missing_neighborhood['Long'])
        closest_index = distances.idxmin()
        df.at[idx, 'Neighborhood'] = df.at[closest_index, 'Neighborhood']

    return df

df_m = add_neighborhood_column(df_m)
df_m = fill_missing_neighborhood(df_m)
df_m = fill_rooms(df_m)
df_m = add_missing_floors(df_m)
df_m = fill_missing_type(df_m)
print(df_m.shape)


# In[19]:


def clean_outliers(df):
    df['PPM'] = (df["Price"] / df['Size']).astype(np.int32)
    columns = ['PPM']
    for col in columns:
        q1, q3 = np.percentile(df[col], [25, 75])
        iqr = q3 - q1
        lower_bound = q1 -(1.5 * iqr) 
        upper_bound = q3 +(1.5 * iqr)
        df = df[(df[col] > lower_bound) & (df[col] < upper_bound)]
            
    df.drop(columns = ['PPM'], inplace = True)
    return df

def street_and_neighborhood_rank(df ,column):
    streets_rank = {}
    years = df['Year'].unique()
    
    for year in years:
        df_by_year = df[df['Year'] == year]
        streets_rank[year] = {}
        
        for street in df_by_year[column].unique():
            df_by_street = df_by_year[df_by_year[column] == street]  # Filter df by selected column    
            street_year_rank = df_by_street['Price'].sum() / df_by_street['Size'].sum()
        
            streets_rank[year][street] = int(street_year_rank)
    
  
    new_column_name = column+'_rank'
        
    df[new_column_name] = np.nan
    
    for index, row in df.iterrows():
        year = row['Year']
        street = row[column]
        
        df.loc[index, new_column_name] = streets_rank[year][street]
        
    df[new_column_name] = df[new_column_name].astype(np.int32)

    return df

def change_by_years(df):
    years = df['Year'].unique()
    today = df[df['Year'] == 2023]
    avg_today = today['Price'].sum() / today['Size'].sum()
    change = {}
    for year in years:
        df_year = df[df['Year'] == year]

        avg_year = df_year['Price'].sum() / df_year['Size'].sum()

        change[year] = avg_today / avg_year 
    return change


def helka_rank(df):
    df = df.drop_duplicates(subset=['Price', 'Date'])
    df = df[(df['Year'] < 2024)]
            
    helka_rank = {}
    
    df['Gush_Helka'] = df['Gush'] + '' + df['Helka']
    df.loc[:, 'Helka_rank'] = np.nan
    gush_helka = df['Gush_Helka'].unique()
    
    change_p = change_by_years(df)
    
    for gh in gush_helka:
        df_gush_helka = df[df['Gush_Helka'] == gh]
        max_year = df_gush_helka.loc[:,'Build_year'].max()
        result_df = df_gush_helka[(df_gush_helka['Year'] >= max_year) & (df_gush_helka['Year'] < 2024)]
        result_df.loc[:, 'P_price'] = result_df['Year'].apply(lambda x: change_p[x]) * result_df['Price']
        result_df.loc[:, 'P_price'] = result_df['P_price'].astype(np.int32)

        rank = result_df.loc[:, 'P_price'].sum() / result_df.loc[:, 'Size'].sum()
        helka_rank[gh] = rank
        
            
    df['Helka_rank'] = df['Gush_Helka'].map(helka_rank)
    df = df.dropna(subset=['Helka_rank'])    
    df.loc[:, 'Helka_rank'] = df['Helka_rank'].astype(np.int32)
    df.drop(columns = ['Gush_Helka'], inplace = True)
    
    return df

df_m = add_neighborhood_column(df_m)
df_m = street_and_neighborhood_rank(df_m,'Street')
df_m = street_and_neighborhood_rank(df_m,'Neighborhood')
df_m = clean_outliers(df_m)
df_m = street_and_neighborhood_rank(df_m,'Gush')
# df_m = street_and_neighborhood_rank(df_m,'Helka')
df_m = helka_rank(df_m)
df_m['New'].fillna(0, inplace=True)
df_m['New'] = df_m['New'].astype(np.int32) 


# In[20]:


df_m.to_csv('Data/Nadlan_clean.csv') 


# In[7]:


def append_to_file():
    file_path = "../status.txt"
    message = "clean_nadlan pass-success"

    try:
        # Open the file in append mode
        with open(file_path, "a") as file:
            # Append the message on a new line
            file.write("\n" + message)

        print("Message appended to the file successfully!")
    except Exception as e:
        print("An error occurred while appending to the file:", str(e))

# Call the function to append to the file
append_to_file()

print(df_m.shape)


# In[ ]:




