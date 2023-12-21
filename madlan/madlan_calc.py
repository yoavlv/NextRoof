import pandas as pd
import numpy as np
from datetime import datetime
import joblib
from sklearn.metrics import r2_score, mean_absolute_error
from .sql_reader_madlan import read_from_madlan_rank
from .sql_save_madlan import add_new_deals_madlan_predict
import traceback

def data_prep_madlan(df,city_id, start_year=2005, end_year=2024, min_price=800000, max_price=10000000):
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

    df = df.dropna(subset=['long', 'lat', 'rooms', 'floor', 'floors', 'helka_rank', 'size'])

    df.loc[:, 'floors'] = df['floors'].astype(float).astype(np.int32)
    df.loc[:, 'floor'] = df['floor'].astype(float).astype(np.int32)
    df.loc[:, 'rooms'] = df['rooms'].astype(float).astype(int)

    df = df.reindex(columns=["rooms", "floor", "size", "price", "build_year", "floors",
                             "year", "age", "neighborhood_rank", "street_rank", "helka_rank", "new"])
    models = joblib.load(f'C:/Users/yoavl/NextRoof/Algorithms/models/{city_id}_saved_models.pkl')
    model = models['stacking']

    data = predict_data_madlan(df, model, item_id,city_id)
    return data


def predict_data_madlan(df, model, item_id,city_id):
    """
    Make predictions using the input model and compute r2_score and MAE.
    :param df: Input DataFrame
    :param model: Trained model for prediction
    :param item_id: List of item IDs
    :return: DataFrame with predictions and associated metrics
    """
    y = df['price']
    X = df.drop('price', axis=1)

    try:
        scaler = joblib.load(f'C:/Users/yoavl/NextRoof/Algorithms/models/{city_id}_scaler.pkl')
        X_scaled = scaler.transform(X)
        y_pred = model.predict(X_scaled)
    except Exception as e:
        print(f"Error in prediction or scaling: {e}")
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


def main_madlan_calc(city_obj):
    city_name = city_obj[0]
    city_id = city_obj[1]
    status = {}

    try:
        df = read_from_madlan_rank(city_name)
        data_calc = data_prep_madlan(df, city_id)
        data = add_new_deals_madlan_predict(data_calc['df'])

        status.update({
            'success': True,
            'new_rows': data.get('new_rows'),
            'updated_rows': data.get('updated_rows'),
            'r2': data_calc.get('r2'),
            'mae': data_calc.get('mae')
        })

    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        status.update({
            'success': False,
            'error': error_message
        })

    return status