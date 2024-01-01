import pandas as pd
import numpy as np
from dev import get_db_engine
import joblib
import pickle
import traceback
from sqlalchemy import text

def read_addr_table(city):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM addr_cache WHERE LENGTH(neighborhood) > 3 and city LIKE %s"
    params = ('%' + city + '%',)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn,params=params)
    return df


def read_from_madlan_raw(city=None, item_id=False):
    engine = get_db_engine()
    if city:
        query = "SELECT * FROM madlan_raw WHERE city LIKE %s"
        params = ('%' + city + '%',)
    elif item_id:
        query = """
                SELECT item_id FROM madlan_raw 
                WHERE EXTRACT(DAY FROM NOW() - last_update) > 7
                """
        params = None
    else:
        query = "SELECT * FROM madlan_raw"
        params = None

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params=params)
    if item_id:
        return df['item_id'].tolist()

    df.replace({'NaN': np.nan, '': np.nan}, inplace=True)

    return df

def read_from_madlan_rank(city , items_id = False):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM madlan_rank WHERE city LIKE %s"
    params = ('%' + city + '%',)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn,params=params)

    df.replace({'NaN': np.nan, 'None': np.nan}, inplace=True)
    df = df.dropna(subset=['helka_rank', 'street_rank', 'neighborhood_rank'])
    df.loc[:, 'helka_rank'] = df['helka_rank'].astype(float).astype(np.int32)
    df.loc[:, 'street_rank'] = df['street_rank'].astype(float).astype(np.int32)
    df.loc[:, 'neighborhood_rank'] = df['neighborhood_rank'].astype(float).astype(np.int32)

    return df

def read_from_madlan_clean(city):
    engine = get_db_engine(db_name='nadlan_db')
    query = "SELECT * FROM madlan_clean WHERE city LIKE %s"
    params = ('%' + city + '%',)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn,params=params)

    df.replace({'NaN': np.nan, 'None': np.nan}, inplace=True)
    df = df.dropna(subset=['gush'])
    df.loc[:, 'gush'] = df['gush'].astype(float).astype(np.int32)
    df.loc[:, 'helka'] = df['helka'].astype(float).astype(np.int32)

    return df


def read_model_scaler_from_db(city_id, model=False, scaler= False):
    engine = get_db_engine(db_name='nextroof_db')
    with engine.connect() as conn:
        if model:
            query = text("SELECT model_data FROM ml_models WHERE city_code = :city_id AND model_name = 'stacking'")
        if scaler:
            query = text("SELECT model_scaler FROM ml_models WHERE city_code = :city_id AND model_name = 'stacking'")
        data = conn.execute(query, {'city_id': city_id}).fetchone()[0]
        return pickle.loads(data)


def delete_records_by_item_ids(item_ids, db_name='nextroof_db',host_name='localhost'):
    engine = get_db_engine(db_name=db_name, host_name=host_name)
    tables = ['madlan_raw', 'madlan_clean'] if db_name == 'nadlan_db' else ['madlan_rank', 'madlan_predict']

    with engine.connect() as connection:
        trans = connection.begin()
        try:
            for table in tables:
                query = text(f"""
                    DELETE FROM {table}
                    WHERE item_id IN (SELECT unnest(:item_ids))
                """)
                connection.execute(query, {'item_ids': item_ids})

            trans.commit()
            print(f'delete_records_by_item_ids (deleted): {item_ids}')
        except Exception as e:
            trans.rollback()
            print(f"An error occurred: {e}")