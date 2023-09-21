import pandas as pd
from utils.base import  df_helper ,create_building_year_dict , create_street_neighborhood_dict , update_neighborhood_street_token , check_for_match
# from bs4 import BeautifulSoup
from utils.location import convert_coordinates , get_long_lat_tuples ,find_avg_cords_by_street
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import re
import csv

class CleanData():
    def __init__(self, df , name='NoName' , test = False):
        self.df = df
        self.name = name
        self.shape = df.shape
        self.setUp_cols()
        if self.name == 'madlan':
            self.madlanSetUp()

        elif self.name == 'yad2' :
            self.yad2SetUp()

        if test == False:
            self.mainSetUp()

    def __str__(self):
        return f'name:{self.name} {self.df.shape}'

    def showDataFrame(self):
        return self.df

    def saveDataFrame(self,path):
        self.df.to_csv(path)
        print(f"df saved {path}")

    def setUp_cols(self):
        self.df = self.df[self.df['City'].str.contains('תל אביב')]
        cols = ['Gush', 'Helka','Tat']
        for col in cols:
            if col not in list(self.df.columns):
                self.df.loc[:, col] = np.nan


    def madlanSetUp(self):
        self.df = self.df.rename(columns={'BuildingClass': 'Asset_type'})
        self.df = self.df.dropna(subset=['Size'])
        self.df = self.df[self.df['Asset_type'].apply(lambda x: x in ['flat', 'gardenapartment', 'roofflat', 'building', 'studio'])]
    def yad2SetUp(self):
        self.df['Parking'] = self.df['Parking'].replace('ללא', "0").fillna("0").astype(np.int32)
        self.algo_accuracy()
        self.drop_rows_by_keyword()
        try:
            self.df = self.df.drop(['level_0', 'index'], axis=1)
        except:
            pass
        self.df = self.df.drop(columns=['Balconies', 'Parking', 'Balconies', 'Immediate'
            , 'Ac', 'Furniture', 'On_pillars', 'Elevator', 'Storeroom', 'Shelter',
                              'Text', 'Date_of_entry', 'Images', 'Handicapped', 'Floors_text', 'Agency', 'Garden_size'
            , 'Date_added', 'City_code', 'City'], axis=1)
        self.convert_price_str_to_int()
    def mainSetUp(self):
        print('add_gush_helka_tat')
        print(self.df.shape)
        self.add_gush_helka_tat()
        print('fix_build_year')
        print(self.df.shape)
        self.fix_build_year()
        print('fix_floors')
        print(self.df.shape)

        self.fix_floors()
        print('update_asset_conditon')
        self.update_asset_conditon()
        print('add_year')
        self.add_year()
        print('update_neighborhood_street_token')
        self.update_neighborhood_street_token()
        print('street_and_neighborhood_rank_Neighborhood')
        self.street_and_neighborhood_rank('Neighborhood')
        print('street_and_neighborhood_rank_Street')
        self.street_and_neighborhood_rank('Street')
        print('street_and_neighborhood_rank_Gush')
        self.street_and_neighborhood_rank('Gush')
        print('parcel_rank')
        self.parcel_rank()

    def drop_rows_by_keyword(self):
        keywords = ['פנטאהוס', 'בדמי מפתח', 'דמי מפתח', 'משרד', 'לא למגורים', 'סכסוך',
                    'משרדים', 'אולם', 'מפתח', 'לשימור', 'דרוש שיפוץ', 'דרושה שיפוץ', 'להריסה', 'דירת פאר', 'מחולקת']
        self.df['Text'] =  self.df['Text'].astype(str)

        conditions = [ self.df['Text'].str.contains(keyword, case=False) for keyword in keywords]
        drop_mask = pd.concat(conditions, axis=1).any(axis=1)
        self.df =  self.df[~drop_mask]



    def fix_floors(self):
        nadlan = df_helper(['Floors', 'Street', 'Home_number'])
        self.df['Floors'] = np.nan

        max_floors = nadlan.groupby(['Street', 'Home_number'])['Floors'].max().reset_index()
        max_floors_dict = {(row['Street'], row['Home_number']): row['Floors'] for _, row in max_floors.iterrows()}

        for idx, row in self.df.iterrows():
            if pd.isna(row['Floors']):
                match = max_floors_dict.get((row['Street'], row['Home_number']), np.nan)
                if not pd.isna(match):
                    self.df.at[idx, 'Floors'] = match

        street_avg_floors = nadlan.groupby('Street')['Floors'].mean().round().reset_index()
        street_avg_floors_dict = {row['Street']: row['Floors'] for _, row in street_avg_floors.iterrows()}

        for idx, row in self.df[self.df['Floors'].isna()].iterrows():
            match = street_avg_floors_dict.get(row['Street'], np.nan)
            if not pd.isna(match):
                self.df.at[idx, 'Floors'] = match

        self.df.loc[self.df['Floor'] > self.df['Floors'], 'Floors'] = self.df['Floor']
        self.df['Floors'] = self.df['Floors'].fillna(self.df['Floor'])

    def parcel_rank(self):
        count = 0
        self.df['Helka_rank'] = np.nan

        self.df = self.df.dropna(subset=['Gush', 'Helka']).reset_index(drop=True)
        self.df['Gush'] = self.df['Gush'].astype(np.int32)
        self.df['Helka'] = self.df['Helka'].astype(np.int32)

        df_nadlan = df_helper(['Gush', 'Helka', 'Helka_rank'])
        df_nadlan['Gush_Helka'] = df_nadlan['Gush'].astype(str) + df_nadlan['Helka'].astype(str)
        column = 'Gush_Helka'
        self.df[column] = self.df['Gush'].astype(str) + self.df['Helka'].astype(str)
        self.df[column] = self.df[column].str.replace('.', '', regex=False)

        for index, row in self.df.iterrows():
            gush_helka = row['Gush_Helka']
            match = df_nadlan.loc[(df_nadlan['Gush_Helka'] == gush_helka), 'Helka_rank']

            if match.empty:
                match = check_for_match(df_nadlan, gush_helka)

            if not match.empty:
                self.df.at[index, 'Helka_rank'] = match.values[0]

            if match.empty:
                count += 1

        self.df['Helka_rank'] = self.df['Helka_rank'].fillna(df_nadlan['Helka_rank'].mean()).astype(np.int32)

    def street_and_neighborhood_rank(self, column):
        self.df = update_neighborhood_street_token(self.df)
        self.df = self.df.dropna(subset=['Neighborhood', 'Street']).reset_index(drop=True)
        df_nadlan = df_helper(['Year', 'Size', 'Price','Neighborhood', 'Street','Gush'])

        rank = df_nadlan.groupby(['Year', column]) \
            .apply(lambda x: np.nan if x['Size'].sum() == 0 else x['Price'].sum() / x['Size'].sum()) \
            .unstack().to_dict(orient="index")

        new_column_name = column + '_rank'
        self.df[new_column_name] = np.nan

        for index, row in self.df.iterrows():
            year = row['Year']
            item = row[column]
            found_street = False

            for nadlan_street in df_nadlan[column].unique():
                if item == nadlan_street:
                    found_street = True
                    item = nadlan_street
                    break

            if not found_street and found_street != False:
                for nadlan_street in df_nadlan[column].unique():
                    if item in nadlan_street:
                        item = nadlan_street
                        found_street = True
                        break

                    check_parts = nadlan_street.split(' ')
                    if item in check_parts:
                        item = nadlan_street
                        found_street = True
                        break

            if found_street:
                for potential_year in [year, year - 1, year - 2, year - 3]:
                    try:
                        self.df.at[index, new_column_name] = rank[potential_year][item]
                        break
                    except KeyError:
                        continue
            else:
                pass

        mean = self.df[new_column_name].mean()
        self.df[new_column_name] = self.df[new_column_name].fillna(mean)
        self.df[new_column_name] = self.df[new_column_name].astype(np.int32)

    def convert_price_str_to_int(self):
        self.df['Price'] = self.df['Price'].str.replace('לא צוין מחיר', '0').replace('[₪,]', '', regex=True).astype(np.int32)
    def add_year(self):
        current_year = datetime.now().year
        self.df['Year'] = current_year
        self.df.assign(profit='NAN')
        self.df['Year'].astype(np.int32)
    def edit_parking(self):
        # Not in use
        if 'Parking' not in self.df.columns:
            self.df['Parking'] = np.nan

        self.df['Parking'] = self.df['Parking'].str.replace('ללא', '0')
        self.df['Parking'] = self.df['Parking'].astype(np.int32)
    def fix_build_year(self):
        self.df = self.df.dropna(subset=['Street'])

        if 'Build_year' not in self.df.columns:
            self.df['Build_year'] = np.nan

        nadlan_dict = create_building_year_dict()
        self.df.set_index(['Street', 'Home_number'], inplace=True)
        self.df['Build_year'].fillna(nadlan_dict, inplace=True)
        self.df.reset_index(inplace=True)

        # For the ones still missing, use the average build year by street
        street_buildings_avg = self.df.groupby('Street')['Build_year'].transform('mean')
        self.df['Build_year'].fillna(street_buildings_avg, inplace=True)

        self.df['Build_year'].fillna(self.df['Build_year'].mean(), inplace=True)
        self.df['Build_year'] = self.df['Build_year'].astype(np.int32)
    def complete_neighborhood_column(self):
        neighborhoods = create_street_neighborhood_dict()
        for index, row in self.df.iterrows():
            street = row['Street']

            for neighborhood, streets in neighborhoods.items():
                if street in streets:
                    self.df.at[index, 'Neighborhood'] = neighborhood
                    break

    def complete_long_lat_columns(self):
        avg_cords = find_avg_cords_by_street()
        for index, row in self.df.iterrows():
            long = row['Long']
            lat = row['Lat']
            street = row['Street']

            try:
                cords = avg_cords[street]
                if long == 0.0 or lat == 0.0:
                    self.df.at[index, 'Long'] = cords[1]
                    self.df.at[index, 'Lat'] = cords[0]
            except:
                pass

        self.df.rename(columns={'Lat': 'Temp'}, inplace=True)
        self.df.rename(columns={'Long': 'Lat'}, inplace=True)
        self.df.rename(columns={'Temp': 'Long'}, inplace=True)

        self.df = self.df.loc[(self.df['Lat'] != 0.0) & (self.df['Long'] != 0.0)]

        self.df.loc[:, 'Long'] = self.df['Long'].astype(np.int32)
        self.df.loc[:, 'Lat'] = self.df['Lat'].astype(np.int32)


    def convert_lat_long(self):
        new_cord = convert_coordinates(get_long_lat_tuples(self.df))
        for index, row in self.df.iterrows():
            try:
                self.df.at[index, 'Lat'] = new_cord[index][0]
                self.df.at[index, 'Long'] = new_cord[index][1]
            except:
                pass
    def update_neighborhood_street_token(self):
        self.df['Street'] = self.df['Street'].str.strip().str.replace("'", "")
        self.df['Neighborhood'] = self.df['Neighborhood'].str.strip()

        replacements_street = {
            "כרמייה": "כרמיה"
        }

        replacements_neighborhood = {
            "'": "",
            "-": " ",
            "נווה אביבים": "נווה אביבים וסביבתה",
            "לב תל אביב, לב העיר צפון": "הצפון הישן החלק הצפוני",
            "נוה": "נווה",
            "גני צהלה, רמות צהלה": "גני צהלה",
            "נווה אליעזר וכפר שלם מזרח": "כפר שלם מזרח נווה אליעזר",
            "גני שרונה, קרית הממשלה": "גני שרונה",
            "הצפון הישן   דרום": "הצפון החדש החלק הדרומי",
            "מכללת תל אביב יפו, דקר": "דקר"
        }

        for old, new in replacements_street.items():
            self.df['Street'] = self.df['Street'].str.replace(old, new)

        for old, new in replacements_neighborhood.items():
            self.df['Neighborhood'] = self.df['Neighborhood'].str.replace(old, new)

    def update_asset_conditon(self):
        '''
        My index:
        1- new from builder
        2 - new
        3 - good
        4 - after reinovtions
        5 - need reinovtions
        '''
        try:
            self.df.rename(columns={'Asset_classification': 'New'}, inplace=True)
            self.df.loc[self.df['New'] == 2, 'New'] = 4
            self.df.loc[self.df['New'] == 6, 'New'] = 2
            self.df['New'] = self.df['New'].apply(lambda x: 1 if x in [1, 2] else 0)

        except:
            self.df['Condition'] = self.df['Condition'].apply(lambda x: 1 if x == 'new' else 0)
            self.df.rename(columns={'Condition': 'New'}, inplace=True)

        self.df['New'] = self.df['New'].astype(np.int32)

    def add_gush_helka_tat(self, city='תל אביב'):

        df1_gov = pd.read_csv("C:/Users/yoavl/NextRoof/Data/Real_Estate_TLV_GOVMAPS_1.csv", index_col=0)
        df2_gov = pd.read_csv("C:/Users/yoavl/NextRoof/Data/Real_Estate_TLV_GOVMAPS_2.csv", index_col=0)
        df_gov = pd.merge(df1_gov, df2_gov, how='outer')
        df_gov[['Gush', 'Helka', 'Tat']] = df_gov['GUSHHELKATAT'].str.split('-|/', n=2, expand=True).astype(np.int32)
        df_gov['Home_number'] = df_gov['ADDRESS'].str.extract('(\d+)').astype(np.int32)

        # Level 1: Government Maps
        for index, row in self.df.iterrows():
            if pd.notna(row['Home_number']) and pd.isna(row['Gush']):
                home_number = row['Home_number']
                street = row['Street']

                matches = df_gov.loc[
                    (df_gov['STREENNAME'].str.contains(street)) & (df_gov['Home_number'] == home_number), ['Gush',
                                                                                                           'Helka',
                                                                                                           'Tat']]

                if not matches.empty:
                    self.df.loc[index, 'Gush'] = matches['Gush'].values[0]
                    self.df.loc[index, 'Helka'] = matches['Helka'].values[0]
                    self.df.loc[index, 'Tat'] = matches['Tat'].values[0]

        # Level 2: Nadlan Database
        df_nadlan = pd.read_csv("C:/Users/yoavl/NextRoof/Data/Nadlan.csv")
        df_nadlan[['Gush', 'Helka', 'Tat']] = df_nadlan['GUSH'].str.split('-|/', n=2, expand=True).astype(np.int32)
        df_nadlan = df_nadlan.drop(columns='GUSH', axis=1)
        df_nadlan[['Street', 'Home_number']] = df_nadlan['DISPLAYADRESS'].str.extract('(.+)\s+(\d+)')
        df_nadlan.dropna(subset=['Home_number'], inplace=True)
        df_nadlan['Home_number'] = df_nadlan['Home_number'].astype(np.int32)

        for index, row in self.df.iterrows():
            if pd.notna(row['Home_number']) and pd.isna(row['Gush']):
                home_number = row['Home_number']
                street = row['Street']

                matches = df_nadlan.loc[
                    (df_nadlan['Street'].str.contains(street)) & (df_nadlan['Home_number'] == home_number), ['Gush',
                                                                                                             'Helka',
                                                                                                             'Tat']]

                if not matches.empty:
                    self.df.loc[index, 'Gush'] = matches['Gush'].values[0]
                    self.df.loc[index, 'Helka'] = matches['Helka'].values[0]
                    self.df.loc[index, 'Tat'] = matches['Tat'].values[0]

        # Level 3: Addresses
        df_address = pd.read_csv("C:/Users/yoavl/NextRoof/Data/Addresses.csv")
        for index, row in self.df.iterrows():
            if pd.notna(row['Home_number']):
                home_number = row['Home_number']
                street = row['Street']

                gush = df_address.loc[
                    (df_address['ms_bayit'] == home_number) & (df_address['t_rechov'].str.contains(street)), 'ms_gush']
                helka = df_address.loc[(df_address['ms_bayit'] == home_number) & (
                    df_address['t_rechov'].str.contains(street)), 'ms_chelka']

                if not gush.empty:
                    self.df.at[index, 'Gush'] = gush.iloc[0]
                if not helka.empty:
                    self.df.at[index, 'Helka'] = helka.iloc[0]

                if gush.empty and helka.empty:
                    street_parts = street.split(' ')
                    last_part = street_parts[-1]
                    matching_streets = df_address[df_address['t_rechov'].str.contains(last_part)]
                    if not matching_streets.empty:
                        gush = matching_streets.loc[matching_streets['ms_bayit'] == home_number, 'ms_gush']
                        helka = matching_streets.loc[matching_streets['ms_bayit'] == home_number, 'ms_chelka']
                        if not gush.empty:
                            self.df.at[index, 'Gush'] = gush.iloc[0]
                        if not helka.empty:
                            self.df.at[index, 'Helka'] = helka.iloc[0]

        self.df = self.df.dropna(subset=['Gush', 'Helka'])
        self.df.loc[:, 'Gush'] = self.df['Gush'].astype(np.int32)
        self.df.loc[:, 'Helka'] = self.df['Helka'].astype(np.int32)


    def algo_accuracy(self , startValue = 20):
        self.df['Accuracy'] = startValue
        cols_to_check = ['Neighborhood', 'Home_number', 'Long', 'Lat', 'Floor', 'Floors']
        cols_with_nonzero = ['Size', 'Rooms']

        for col in cols_to_check:
            self.df['Accuracy'] +=  self.df[col].notna().astype(np.int32) * 10

        for col in cols_with_nonzero:
            self.df['Accuracy'] += (( self.df[col].notna()) & ( self.df[col] != 0)).astype(np.int32) * 10


    def count_na(self):
        for col in self.df.columns:
            na = self.df[self.df[col].isna()]
            print(col)
            print(na.shape[0])

