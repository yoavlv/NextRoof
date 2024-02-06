import pandas as pd
import numpy as np

from sqlalchemy import text
import sqlalchemy

try:
    from ..dev import get_db_connection
except:
    get_db_engine = None

def read_from_population(population_size=10000):
    engine = get_db_engine(db_name='nextroof_db')
    city_dict = {}

    try:
        with engine.connect() as conn:
            query = text("select city_id, city_name from population where total > :size order by total desc;")
            result = conn.execute(query, {'size': population_size}).fetchall()

            for row in result:
                city_id, city_name = row
                city_dict[city_name] = city_id

    except sqlalchemy.exc.SQLAlchemyError as e:
        # Handle the exception here (e.g., log the error, raise a custom exception)
        print(f"Error during database operation: {e}")
        city_dict = {}  # Return an empty dictionary when an error occurs

    return city_dict

def read_raw_data_table_by_gush(gush, num_of_rows=1):
    engine = get_db_engine()
    with engine.connect() as conn:
        query = text("SELECT fulladress FROM nadlan_raw WHERE gush = :gush ORDER BY dealdate DESC LIMIT :num_of_rows")
        result = conn.execute(query, {'gush': gush, 'num_of_rows': num_of_rows}).fetchall()
        fulladress_list = [row[0] for row in result]
    return fulladress_list[0] if fulladress_list else np.nan

def read_raw_data_table(num_of_rows=100, city=None):
    engine = get_db_engine()
    try:
        if city:
            query = "SELECT * FROM nadlan_raw WHERE city = %s ORDER BY created_at DESC LIMIT %s"
            df = pd.read_sql_query(query, engine, params=(city, num_of_rows))
        else:
            query = "SELECT * FROM nadlan_raw ORDER BY created_at DESC LIMIT %s"
            df = pd.read_sql_query(query, engine, params=(num_of_rows,))
        df_clean = read_key_from_nadlan_clean(get_db_engine(db_name='nextroof_db'))
        keys_to_remove = df_clean['key'].unique()
        df = df[~df['keyvalue'].isin(keys_to_remove)]
        return df

    except Exception as e:
        print(f"Error: {e}")
        return None


def read_key_from_nadlan_clean(engine):
    return pd.read_sql_query("SELECT key FROM nadlan_clean", engine)


def read_from_nadlan_rank(city):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM nadlan_rank WHERE city LIKE %s"
    df = pd.read_sql_query(query, engine, params=(city,))
    df.replace({'NaN': np.nan, 'None': np.nan}, inplace=True)
    df = df.dropna(subset=['gush'])
    df.loc[:, 'gush'] = df['gush'].astype(float).astype(np.int32)
    df.loc[:, 'helka'] = df['helka'].astype(float).astype(np.int32)
    return df

def read_from_nadlan_rank_find_floor(city, street, home_number):
    home_number = str(home_number).strip()
    city = str(city).strip()
    street = str(street).strip()
    engine = get_db_engine(db_name='nextroof_db')
    query = text(f"SELECT floors FROM nadlan_rank WHERE city = '{city}' AND street = '{street}' AND home_number = '{home_number}' ORDER BY floors DESC LIMIT 1;")
    with engine.connect() as connection:
        result = connection.execute(query)
        row = result.fetchone()
        return row[0] if row else np.nan

def read_from_nadlan_clean(city):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM nadlan_clean WHERE city = %s"
    df = pd.read_sql_query(query, engine, params=(city,))
    df.replace({'NaN': np.nan, 'None': np.nan}, inplace=True)
    df = df.dropna(subset=['gush'])
    df.loc[:, 'gush'] = df['gush'].astype(float).astype(np.int32)
    df.loc[:, 'helka'] = df['helka'].astype(float).astype(np.int32)
    return df

def distinct_city_list(table_name):
    engine = get_db_engine(db_name='nextroof_db')
    query = text(f"SELECT DISTINCT(city) FROM {table_name}")

    with engine.connect() as connection:
        result = connection.execute(query)
        city_list = [row[0] for row in result]
    return list(city_list)