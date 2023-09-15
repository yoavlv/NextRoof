import time
import requests
import pandas as pd
import math
import numpy as np


def get_gush_chelka_api(address):
    headers = {
        'sec-ch-ua': '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
        'auth_data': '{"api_token":"03387185-da2d-4cb4-a3c6-2292a3df7915","user_token":"","domain":"www.gov.il","token":"qMZlz1IwqfKc6WhO6qet3OHGjLusa/tBvtVdRBurehytCsrtnVhcI81jhg/2pDc39ChRLpK7C+mzq4rg5GjqdHfTQJANTYMPAmVWrfMwL4O2AMtdLCq+QEt/inww791B+vWYq7Z0PiAq8lUPSBuB1//YGsM/BvB9daJoQwguhya+2NxIm2JK2gfMp+RnSHzGdn4LFWLuLw3lHHYzhGT8Q8+3vzRbF/vSl79P7qNAoLxGPApgGa80OA==","user_id":178592,"isAdmin":false,"expires":"2023-08-15T21:20:19.0844919+03:00"}',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
        'Content-Type': 'application/json; charset=UTF-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': 'https://www.gov.il/',
        'sec-ch-ua-platform': '"Windows"',
    }

    json_data = {
        'type': 0,
        'address': address,
    }
    try:
        response = requests.post('https://ags.govmap.gov.il/Api/Controllers/GovmapApi/SearchAndLocate', headers=headers, json=json_data)
        print(response)
    except Exception as e:
        print(e)
        return False
    if response.status_code == 200:
        data = response.json()
        if data['status'] != 0:

            return response
        if 'data' in data and len(data['data']) > 0 and 'Values' in data['data'][0]:
            return [int(value) for value in data['data'][0]['Values']]

    return False


def create_street_neighborhood_dict():
    df = pd.read_csv('C:/Users/yoavl/NextRoof/Data/Real_Estate_TLV_GOVMAPS_1.csv',index_col=0)
    neighborhoods = {}
    for index, row in df.iterrows():
        neighborhood = row['NEIGHBORHOOD']
        street = row['STREENNAME']
        if neighborhood not in neighborhoods:
            neighborhoods[neighborhood] = set()
        neighborhoods[neighborhood].add(street)
    return neighborhoods

def create_building_year_dict():
    df = pd.read_csv('C:/Users/yoavl/NextRoof/Data/Nadlan_clean.csv',index_col=0)
    df_sub = df.dropna(subset=['Floors','Street','Build_year'])
    max_build_years = df_sub.groupby(['Street', 'Home_number'])['Build_year'].max()
    return max_build_years.to_dict()

def calc_distance_from_the_see_TLV(X_coordinate, Y_coordinate):
    north_x = 180471
    north_y = 672391
    south_x = 177333
    south_y = 663016

    m = (south_y - north_y) / (south_x - north_x)
    b = north_y - (m * north_x)

    numerator = abs(m * X_coordinate - Y_coordinate + b)
    denominator = math.sqrt(m ** 2 + 1)
    return numerator / denominator

def calc_distance_from_train_station(df):
    stations = [(179820.47,662424.54), (180619,664469.56), (181101.44,665688.78), (181710.96,667877.05)]
    distances = []

    for index, row in df.iterrows():
        apartment_coords = (row['Lat'], row['Long'])

        station_distances = []
        for station_coords in stations:
            station_distance = abs(station_coords[0] - apartment_coords[1]) + abs(station_coords[1] - apartment_coords[0])
            station_distances.append(station_distance)

        # Find the minimum distance
        min_distance = min(station_distances)
        distances.append(min_distance)

    df['Train'] = distances
    df['Train'] = df['Train'].astype(np.int32)
    return df

def distance_from_sea_tlv(df):
    '''
    Need to fix long ? lat ?
    '''
    df = df.dropna(subset=['Lat', 'Long']).reset_index(drop=True)
    latitudes = df['Long'].astype(float)
    longitudes = df['Lat'].astype(float)
    distances = [calc_distance_from_the_see_TLV(lat, lon) for lat, lon in zip(latitudes, longitudes)]
    df['Distance_sea'] = distances
    df['Distance_sea'] = df['Distance_sea'].astype(np.int32)
    return df

def update_neighborhood_street_token(df):
    df['Street'] = df['Street'].str.strip().str.replace("'", "")
    df['Neighborhood'] = df['Neighborhood'].str.strip()

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
        df['Street'] = df['Street'].str.replace(old, new)

    for old, new in replacements_neighborhood.items():
        df['Neighborhood'] = df['Neighborhood'].str.replace(old, new)
    return df

def check_for_match(df_nadlan, parcel):
    parcel = int(parcel)
    parcel_offsets = [parcel - 1, parcel + 1, parcel + 2, parcel - 2, parcel + 3, parcel - 3, parcel + 4, parcel - 4]
    for parcel_offset in parcel_offsets:
        match = df_nadlan.loc[(df_nadlan['Gush_Helka'] == str(parcel_offset)), 'Helka_rank']
        if not match.empty:
            return match
    return match


def df_helper(cols = None):
    nadlan = pd.read_csv("C:/Users/yoavl/NextRoof/Data/Nadlan_clean.csv", index_col=0)
    if isinstance(cols, list):
        nadlan_df = nadlan[cols]
        nadlan_df = nadlan_df.dropna(subset=cols).reset_index(drop=True)
        return nadlan_df

    return nadlan

