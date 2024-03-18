from utils.utils_sql import DatabaseManager, sql_script
from nadlan.sql_reader_nadlan import read_from_view
from dateutil.relativedelta import relativedelta
import traceback
import pandas as pd
import datetime
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures

CURRENT_DATE = datetime.datetime.now()
CURRENT_YEAR = datetime.datetime.now().year
THREE_MONTHS_EARLIER = CURRENT_DATE - relativedelta(months=2)


def calc_and_predict_polynomial_change(df, col_name, current_year, degree=2):
    X = df['year'].values.reshape(-1, 1)
    y = df[col_name].values
    poly_features = PolynomialFeatures(degree=degree, include_bias=True)
    X_poly = poly_features.fit_transform(X)
    model = LinearRegression()
    model.fit(X_poly, y)

    new_year_poly = poly_features.transform([[current_year]])
    predicted_rank = model.predict(new_year_poly)
    return int(predicted_rank[0])


def create_rank_df(df, group_cols, rank_col):
    current_year = datetime.datetime.now().year
    rank_data = {'year': current_year, }
    for col in group_cols:
        rank_data[col] = []
    rank_data[rank_col] = []

    for key, temp_df in df.groupby(group_cols):
        if temp_df.shape[0] > 3:
            rank_res = calc_and_predict_polynomial_change(temp_df, rank_col, current_year)
            if rank_res >= 3000:
                if isinstance(key, tuple):
                    for idx, col_name in enumerate(group_cols):
                        rank_data[col_name].append(key[idx])
                else:
                    rank_data[group_cols[0]].append(key)

                rank_data[rank_col].append(rank_res)

    rank_df = pd.DataFrame(rank_data)
    return rank_df


def main_nadlan_rank():
    success = sql_script('nadlan.sql')
    nadlan_rank_status = {"success": True, "new_rows": 0, "updated_rows": 0, "errors": [], 'nadlan_sql_script': success}
    try:
        df = read_from_view('street_rank_view')
        street_rank_df = create_rank_df(df, ['city_id', 'street_id'], 'street_rank')
        db_manager = DatabaseManager(table_name='street_rank', db_name='nextroof_db')
        _, _, _ = db_manager.insert_dataframe_batch(street_rank_df, batch_size=int(street_rank_df.shape[0]),
                                                                             replace=True, pk_columns=['city_id','street_id'])

        df = read_from_view('gush_rank_view')
        gush_rank_df = create_rank_df(df, ['gush'], 'gush_rank')
        db_manager = DatabaseManager(table_name='gush_rank', db_name='nextroof_db')
        _, _, _ = db_manager.insert_dataframe_batch(gush_rank_df, batch_size=int(gush_rank_df.shape[0]),
                                                                             replace=True, pk_columns='gush')

        df = read_from_view('helka_rank_view')
        gush_helka_rank_df = create_rank_df(df, ['gush', 'helka'], 'helka_rank')
        db_manager = DatabaseManager(table_name='helka_rank', db_name='nextroof_db')
        _, _, _ = db_manager.insert_dataframe_batch(gush_helka_rank_df, batch_size=int(gush_helka_rank_df.shape[0]),
                                                                             replace=True, pk_columns=['gush','helka'])
        nadlan_rank_status['success'] = True

    except Exception as e:
        error_message = f":{e}\n{traceback.format_exc()}"
        nadlan_rank_status['success'] = False
        nadlan_rank_status['error'] = error_message

    print("main_nadlan_rank_FINISH")
    return nadlan_rank_status

