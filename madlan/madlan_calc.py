import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.metrics import r2_score, mean_absolute_error
from .sql_reader_madlan import read_from_madlan_rank, read_model_scaler_from_db
from .sql_save_madlan import add_new_deals_madlan_predict
import traceback
from utils.utils_sql import DatabaseManager

def data_prep_madlan(df,city_id, start_year=2005, end_year=2025, min_price=800000, max_price=10000000):
    item_id = df['item_id']

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(subset=['size','price','build_year'], inplace=True)
    df['last_update'] = pd.to_datetime(df['last_update'])
    df['year'] = df['last_update'].dt.year.astype(np.int32)
    df.loc[:, 'size'] = df['size'].astype(float).astype(np.int32)
    df.loc[:, 'price'] = df['price'].astype(float).astype(np.int32)
    df.loc[:, 'build_year'] = df['build_year'].astype(float).astype(np.int32)
    df['condition'] = df['condition'].apply(lambda x: 1 if x == 'new' else 0).astype(np.int32)
    df.rename(columns={'condition': 'new'}, inplace=True)

    current_year = datetime.now().year
    df = df.query(
        "@start_year <= year < @end_year and @min_price < price < @max_price and 25 < size < 400 and build_year > 1910")
    df = df.copy()
    df.loc[:, 'age'] = current_year - df.loc[:, 'build_year']

    cols = ["rooms", "floor", "size", "price", "build_year", "floors",
                             "year", "age", "gush_rank", "street_rank", "helka_rank", "new"
            ]
    df = df.dropna(subset=cols)

    df.loc[:, 'floors'] = df['floors'].astype(float).astype(np.int32)
    df.loc[:, 'floor'] = df['floor'].astype(float).astype(np.int32)
    df.loc[:, 'rooms'] = df['rooms'].astype(float).astype(int)

    df = df.reindex(columns=cols)

    model = read_model_scaler_from_db(city_id, model=True)

    data = predict_data_madlan(df, model, item_id,city_id)
    return data


def predict_data_madlan(df, model, item_id, city_id):
    """
    Make predictions using the input model and compute r2_score and MAE.
    :param df: Input DataFrame
    :param model: Trained model for prediction
    :param item_id: List of item IDs
    :return: DataFrame with predictions and associated metrics
    """
    if df.empty:
        print(f"DataFrame is empty, skipping prediction.  (city_id {city_id})")
        return None

    y = df['price']
    X = df.drop('price', axis=1)

    try:
        scaler = read_model_scaler_from_db(city_id, scaler=True)
        if scaler is None:
            print(f"Scaler not found. Skipping scaling and prediction. (city_id {city_id})")
            return None

        X_scaled = scaler.transform(X)
        y_pred = model.predict(X_scaled)
    except Exception as e:
        print(f"Error in prediction or scaling: {e}  (city_id {city_id})")
        return

    df["predicted"] = y_pred.astype(np.int32)
    df['difference'] = df["price"] - df["predicted"]

    mae = mean_absolute_error(y, y_pred)
    r2 = r2_score(y, y_pred)
    print(f'r2_score: {r2} , mae: {mae}')

    df['item_id'] = item_id
    data = {
        'df': df.sort_values(by="difference"),
        'r2': r2,
        'mae': mae,
    }
    return data


def main_madlan_calc(city, city_id):
    status = {}
    try:
        df = read_from_madlan_rank(city_id)
        if df.empty:
            status.update({
                'success': False,
                'error': 'No data available for prediction.'
            })
            return status

        data_calc = data_prep_madlan(df, city_id)

        if data_calc is None:
            status.update({
                'success': False,
                'error': "Data preparation or prediction failed."
            })
            return status

        data = {}
        data['new_rows'] = 'Empty DataFrame'
        data['updated_rows'] = 'Empty DataFrame'
        if not data_calc['df'].empty:
            df = data_calc['df']
            # data = add_new_deals_madlan_predict(data_calc['df'])
            # data2 = add_new_deals_madlan_predict(data_calc['df'], '13.50.98.191')
            db_manager = DatabaseManager('nextroof_db', 'localhost', 'madlan_predict')
            df = df[['item_id','price','predicted','difference']]
            success, new_rows, updated_rows = db_manager.insert_dataframe(df,'item_id')

            status.update({
                'success': success,
                'new_rows': new_rows,
                'updated_rows': updated_rows,
                'r2': data_calc.get('r2'),
                'mae': data_calc.get('mae')
            })

    except Exception as e:
        error_message = f"*****{city}*****\n{e}\n{traceback.format_exc()}"
        status.update({
            'success': False,
            'error': error_message
        })

    return status