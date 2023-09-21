import time
import requests
import pandas as pd
import math
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
def convert_coordinates(lat_long_list):
    '''
    This function use to convert UTM coordinates to ITM coordinates using outside website with selenium
    '''
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
    driver.get("https://zvikabenhaim.appspot.com/software/ITM/")

    # locate all the input fields and button
    lat_input = driver.find_element(By.XPATH, '//*[@id="lat"]')
    lon_input = driver.find_element(By.XPATH, '//*[@id="long"]')
    convert_button = driver.find_element(By.XPATH, '//form[1]/table/tbody/tr[3]/td/input')
    east_span = driver.find_element(By.XPATH, '//*[@id="itm_east"]')
    north_span = driver.find_element(By.XPATH, '//*[@id="itm_north"]')
    coverted_list = []
    for cords in lat_long_list:
        east = cords[0]
        north = cords[1]
        if cords[0] != 0.0 and cords[1] != 0.0:
            lat_input.clear()
            lon_input.clear()

            lat_input.send_keys(cords[0])
            lon_input.send_keys(cords[1])

            convert_button.click()

            east = int(east_span.text)
            north = int(north_span.text)

        coverted_list.append((east, north))

    driver.quit()
    return coverted_list

def get_long_lat_tuples(df):
    long_lat_tuples = []
    for index, row in df.iterrows():
        lat = row['Lat']
        long = row['Long']
        try:
            lat = float(lat)
            long = float(long)

            if long <= 0 or lat <= 0 or np.isnan(lat) or np.isnan(long):
                lat = 0.0
                long = 0.0
        except:
            lat = 0.0
            long = 0.0

        long_lat_tuples.append((lat, long))
    return long_lat_tuples

def find_avg_cords_by_street():
    df = pd.read_csv('C:/Users/yoavl/NextRoof/Data/Nadlan_clean.csv', index_col=0)
    df = df.dropna(subset=['Long', 'Lat', 'Street']).reset_index(drop=True)
    df['Long'] = df['Long'].astype(int)
    df['Lat'] = df['Lat'].astype(int)

    # create unique streets list
    streets = df['Street'].unique()

    street_cords_avg = {}
    for street in streets:
        street_df = df[df['Street'] == street]

        lat_mean = street_df['Lat'].mean()
        long_mean = street_df['Long'].mean()

        street_cords_avg[street] = (long_mean, lat_mean)

    return street_cords_avg
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