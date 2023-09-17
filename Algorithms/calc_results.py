import numpy as np
import pandas as pd
import joblib
from sklearn.metrics import r2_score, mean_absolute_error
from setUp import data_prep
from monitor import monitor_data

def final_data(df, algo_data):
    """
    Merge input dataframe with algo_data based on 'Item_id', remove duplicates, and sort by 'Difference'.
    :param df: Input DataFrame
    :param algo_data: Algorithm DataFrame
    :return: Merged and sorted DataFrame
    """
    merged_df = pd.merge(df, algo_data, on='Item_id', how='inner')
    merged_df.index += 1
    merged_df.drop_duplicates(subset='Item_id', inplace=True)
    return merged_df.sort_values(by="Difference")


def predict_data(df, model, item_id):
    """
    Make predictions using the input model and compute r2_score and MAE.
    :param df: Input DataFrame
    :param model: Trained model for prediction
    :param item_id: List of item IDs
    :return: DataFrame with predictions and associated metrics
    """
    y = df['Price']
    X = df.drop('Price', axis=1)

    try:
        scaler = joblib.load('scaler.pkl')
        X_scaled = scaler.transform(X)
        y_pred = model.predict(X_scaled)
    except Exception as e:
        print(f"Error in prediction or scaling: {e}")
        return

    df["Predicted"] = y_pred.astype(np.int32)
    df['Difference'] = df["Price"] - df["Predicted"]

    mae = mean_absolute_error(y, y_pred)
    r2 = r2_score(y, y_pred)
    print(f'r2_score: {r2} , mae: {mae}')

    df['Item_id'] = item_id
    data = {
        'df': df.sort_values(by="Difference"),
        'r2':r2,
        'mae':mae,
    }
    return data

def save_data(df, path):
    """
    Save DataFrame to a CSV file.
    :param df: DataFrame to be saved
    :param path: Destination path for saving
    """
    try:
        df.to_csv(path)
        print(f"Data saved at {path}")
    except Exception as e:
        print(f"Error saving data: {e}")


def calc_results(yad2 = False, madlan =False):
    name = 'yad2'
    if madlan == True:
        name = 'madlan'

    try:
        models = joblib.load('saved_models.pkl')
        model = models['stacking']
        yad2_df, item_id = data_prep(yad2=yad2 ,madlan=madlan, accuracy=0, min_price=1200000, max_price=6000000)
        data = predict_data(yad2_df, model, item_id)
        save_data(data['df'], f"C:/Users/yoavl/NextRoof/Data/{name}_predict")
        monitor_data['algo'][name]['r2'] = data['r2']
        monitor_data['algo'][name]['mae'] = data['mae']
        monitor_data['algo'][name]['status'] = 'Success'
        print(monitor_data)
    except Exception as e:
        monitor_data['algo'][name]['error'] = e
        monitor_data['algo'][name]['status'] = 'Fail'

