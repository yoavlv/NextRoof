import math
import numpy as np
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
    df['Train'] = [int(min(abs(station[0] - row['Long']) + abs(station[1] - row['Lat']) for station in stations)) for
                   _, row in df.iterrows()]
    return df


def main_distance(df):
    df = distance_from_sea_tlv(df)
    df = calc_distance_from_train_station(df)
    return df