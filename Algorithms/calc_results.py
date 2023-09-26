import numpy as np
import pandas as pd
import joblib
from sklearn.metrics import r2_score, mean_absolute_error
from setUp import data_prep
from monitor import monitor_data
import traceback

def final_data(df, algo_data , name=None):
    """
    Merge input dataframe with algo_data based on 'Item_id', remove duplicates, and sort by 'Difference'.
    Avoid introducing duplicate columns during the merge.
    :param df: Input DataFrame
    :param algo_data: Algorithm DataFrame
    :return: Merged and sorted DataFrame
    """
    # Identify overlapping columns between df and algo_data excluding 'Item_id'
    overlapping_cols = [col for col in df.columns if col in algo_data.columns and col != 'Item_id']


    # Rename overlapping columns in algo_data with a suffix
    suffix = "_x"
    algo_data_renamed = algo_data.rename(columns={col: col + suffix for col in overlapping_cols})

    # Merge df with the renamed algo_data
    merged_df = pd.merge(df, algo_data_renamed, on='Item_id', how='inner')
    if name == 'yad2':
        images = pd.read_csv("C:/Users/yoavl/NextRoof/Data/yad_2_data.csv", index_col=0)
        merged_df = pd.merge(merged_df, images[['Item_id', 'Images']], on='Item_id', how='left')


    # Update the index
    merged_df.index += 1

    # Drop duplicates based on 'Item_id'
    merged_df.drop_duplicates(subset='Item_id', inplace=True)

    # Drop the renamed columns if they exist in the merged dataframe
    for col in overlapping_cols:
        if col + suffix in merged_df.columns:
            merged_df.drop(columns=col + suffix, inplace=True)

    # Return the merged dataframe sorted by 'Difference'
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
        scaler = joblib.load('C:/Users/yoavl/NextRoof/Algorithms/scaler.pkl')
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

    if madlan == True:
        name = 'madlan'
        raw_data = pd.read_csv("C:/Users/yoavl/NextRoof/Data/madlan_data_clean_p.csv" , index_col=0)
    else:
        name = 'yad2'
        raw_data = pd.read_csv("C:/Users/yoavl/NextRoof/Data/yad2_data_clean_p.csv", index_col=0)

    try:
        models = joblib.load('C:/Users/yoavl/NextRoof/Algorithms/saved_models.pkl')
        model = models['stacking']
        yad2_df, item_id = data_prep(yad2=yad2 ,madlan=madlan, accuracy=0, min_price=1200000, max_price=6000000)
        data = predict_data(yad2_df, model, item_id)

        data['df'] = final_data(raw_data,data['df'] , name)
        save_data(data['df'], f"C:/Users/yoavl/NextRoof/Data/{name}_predict_p.csv")
        monitor_data['algo'][name]['r2'] = data['r2']
        monitor_data['algo'][name]['mae'] = data['mae']
        monitor_data['algo'][name]['status'] = 'Success'
    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        print(error_message)
        monitor_data['algo'][name]['error'] = e
        monitor_data['algo'][name]['status'] = 'Fail'

# calc_results(madlan=True)
# calc_results(yad2=True)