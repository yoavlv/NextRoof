import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.model_selection import train_test_split
import joblib
from datetime import datetime


def data_prep(start_year=2005, end_year=2024, min_price=800000, max_price=10000000, yad2=False, madlan=False,
              scaler='StandardScaler', accuracy=False):
    '''
    This function is a setup function for all the algorithms.
    It prepares the data for the machine learning/deep learning models:
    1. Load the dataset
    2. Feature engineering
    3. Clean outliers
    4. Drop non-numeric data
    5. Use PCA if needed
    6. Convert columns to numeric if needed
    7. Split the data
    8. Fit and transform the data
    '''
    df = None
    item_id_col = None
    Activate = False
    print("Prep")
    if yad2 == True:
        Activate = True
        # df = pd.read_csv("../Data/Yad2_clean.csv")
        df = pd.read_csv("../Data/test2.csv")

        #         df = df[df['Asset_type'] == 'דירה']
        if accuracy:
            df = df[df['Accuracy'] >= accuracy]

        item_id_col = df['Item_id']
    if madlan == True:
        Activate = True
        df = pd.read_csv("../Data/test1.csv")
        # df = pd.read_csv("../Data/madlan_data_clean.csv")
        item_id_col = df['Item_id']
        df.dropna(subset=["Rooms", "Floor", "Size", "Price", "Build_year", "Floors",
                          "Year", 'Neighborhood_rank', 'Street_rank', 'Gush_rank', 'Helka_rank', 'New'])

    if not Activate:
        df = pd.read_csv("../Data/Nadlan_clean.csv", index_col=0)

    df = df.drop_duplicates()

    #     df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y')
    #     df['Year'] = df['Date'].dt.strftime('%Y')
    df['Year'] = df['Year'].astype(np.int32)
    df['Size'] = df['Size'].astype(np.int32)

    df = df[(df['Year'] >= start_year) & (df['Year'] < end_year)]
    df = df[(df['Price'] > min_price) & (df['Price'] < max_price)]
    df = df[(df['Size'] < 400) & (df['Size'] > 25)]
    df = df[df['Build_year'] > 1910]

    current_year = datetime.now().year
    df['Age'] = current_year - df['Build_year']

    df = df.dropna(subset=['Long', 'Lat', 'Rooms', 'Floor', 'Floors', 'Gush_rank', 'Helka_rank','Size'])

    df['Floors'] = df['Floors'].astype(float).astype(np.int32)
    df['Floor'] = df['Floor'].astype(float).astype(np.int32)
    df['Lat'] = df['Lat'].astype(np.int32)
    df['Long'] = df['Long'].astype(np.int32)
    df = df[~df['Rooms'].astype(str).str.contains('<function')]
    df['Rooms'] = df['Rooms'].astype(float).astype(int)

    def drop_columns(df, cols):
        for col in cols:
            try:
                df.drop(columns=[col], inplace=True)
            except:
                pass  # print(f"col: '{col}' not found")

        return df

    columns = ['Home_number', 'Rebuilt', 'Gush', 'Helka', 'Tat', 'Percentage_Change', 'Predicted_Price', 'Neighborhood',
               'street_id', 'Item_id', 'Street_id']

    df = drop_columns(df, columns)

    non_numeric_cols = list(df.select_dtypes(exclude=['number']).columns)
    na_cols = list(df.columns[df.isna().any()])
    cols_to_drop = list(set(non_numeric_cols) | set(na_cols))

    df.drop(cols_to_drop, axis=1, inplace=True)  # 'Salary'
    #     df = df.reindex(columns=["Rooms", "Floor", "Size", "Price","Build_year", "Floors", "Long", "Lat",
    #                              "Year", "Distance_sea", "Train",'Age','Neighborhood_rank','Street_rank','Gush_rank','Helka_rank'])

    df = df.reindex(columns=["Rooms", "Floor", "Size", "Price", "Build_year", "Floors",
                             "Year", 'Age', 'Neighborhood_rank', 'Street_rank', 'Gush_rank', 'Helka_rank', 'New'])

    if Activate:
        df.dropna(inplace=True)
        return df, item_id_col

    y = df['Price']
    X = df.drop('Price', axis=1)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42)
    if scaler == 'StandardScaler':
        scaler = StandardScaler()

    else:
        scaler = MinMaxScaler()

    X_train_scaled = scaler.fit(X_train)
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    joblib.dump(scaler, 'scaler.pkl')

    return X_train_scaled, X_test_scaled, y_train, y_test, X_train, X_test


