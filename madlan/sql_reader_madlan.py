import pandas as pd
import numpy as np
from dev import get_db_engine
import pickle
from sqlalchemy import text

def read_from_rank_table(table_name):
    engine = get_db_engine(db_name='nextroof_db')
    query = f"select * from {table_name};"
    df = pd.read_sql_query(query, engine)
    return df
def read_addr_table(city_id):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM addr_cache WHERE LENGTH(neighborhood) > 3 and city_id = %s"
    params = (city_id,)  # Parameters as a tuple
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params=params)
    return df


def read_from_madlan_raw(city_id=None, item_id=False):
    engine = get_db_engine(db_name='nextroof_db')

    if city_id is not None:
        query = "SELECT * FROM madlan_raw WHERE city_id = %s"
        params = (city_id,)

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

    df = df.replace([None, 'NaN', np.nan, ''], np.nan)

    return df

def read_from_madlan_rank(city_id):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM madlan_clean WHERE city_id = %s"
    params = (int(city_id),)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn,params=params)

        conn.close()

    df.replace({'NaN': np.nan, 'None': np.nan}, inplace=True)
    df = df.dropna(subset=['helka_rank', 'street_rank','gush_rank','floor','floors','build_year'])
    df.loc[:, 'helka_rank'] = df['helka_rank'].astype(float).astype(np.int32)
    df.loc[:, 'street_rank'] = df['street_rank'].astype(float).astype(np.int32)

    return df


def read_from_madlan_clean(city_id):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM madlan_clean WHERE city_id = %s"
    params = (city_id,)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

    df = df.replace([None, 'NaN', np.nan, ''], np.nan)
    df = df.dropna(subset=['gush'])
    df.loc[:, 'gush'] = df['gush'].astype(float).astype(np.int32)
    df.loc[:, 'helka'] = df['helka'].astype(float).astype(np.int32)

    return df


def read_model_scaler_from_db(city_id, model=False, scaler=False):
    engine = get_db_engine(db_name='nextroof_db')
    with engine.connect() as conn:
        if model:
            query = text("SELECT model_data FROM ml_models WHERE city_id = :city_id AND model_name = 'stacking'")
        elif scaler:
            query = text("SELECT model_scaler FROM ml_models WHERE city_id = :city_id AND model_name = 'stacking'")

        result = conn.execute(query, {'city_id': city_id}).fetchone()
        if result is None:
            return None  # Return None if no data found
        data = result[0]

    return pickle.loads(data)

def delete_records_by_item_ids(item_ids, db_name='nextroof_db',host_name='localhost'):
    engine = get_db_engine(db_name=db_name, host_name=host_name)
    # tables = ['madlan_raw', 'madlan_clean'] if db_name == 'nadlan_db' else ['madlan_rank', 'madlan_predict']
    tables = ['madlan_raw', 'madlan_clean', 'madlan_rank','madlan_predict']
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