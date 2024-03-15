import pandas as pd
import numpy as np
from sqlalchemy import text, exc
from dev import get_db_engine
from sqlalchemy.exc import SQLAlchemyError


def read_floors_to_dict():
    engine = get_db_engine(db_name='nextroof_db')
    floors_dict = {}

    try:
        with engine.connect() as conn:
            query = text("SELECT floor_name , floor_num FROM floors;")
            result = conn.execute(query).fetchall()
            for row in result:
                city_id, city_name = row
                floors_dict[city_id] = city_name

    except SQLAlchemyError as e:
        raise SQLAlchemyError(f"Error during database operation: {e}")

    return floors_dict
def fetch_all_cache_data_by_city(city_id):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM addr_cache WHERE city_id = %s"
    df = pd.read_sql_query(query, engine, params=(city_id,))
    df["street_id"] = df["street_id"].astype(np.int32)
    return df

def read_from_population(city_id_list=None, population_size=10000):
    engine = get_db_engine(db_name='nextroof_db')
    city_dict = {}

    try:
        with engine.connect() as conn:
            if city_id_list:
                query = text("SELECT city_id, city_name FROM population WHERE city_id IN :city_id_list;")
                result = conn.execute(query, {'city_id_list': tuple(city_id_list)}).fetchall()
            else:
                query = text("SELECT city_id, city_name FROM population WHERE total > :size ORDER BY total DESC;")
                result = conn.execute(query, {'size': population_size}).fetchall()

            for row in result:
                city_id, city_name = row
                city_dict[city_id] = city_name

    except SQLAlchemyError as e:
        raise SQLAlchemyError(f"Error during database operation: {e}")

    return city_dict

def read_raw_data_table(num_of_rows=10000, city_id = None):
    engine = get_db_engine(db_name='nextroof_db')
    try:
        if city_id:
            query = "SELECT * FROM nadlan_raw WHERE city_id = %s ORDER BY created_at DESC LIMIT %s"
            df = pd.read_sql_query(query, engine, params=(city_id, num_of_rows))
        else:
            query = "SELECT * FROM nadlan_raw ORDER BY created_at DESC LIMIT %s"
            df = pd.read_sql_query(query, engine, params=(num_of_rows,))

        # Remove data that alreay exist
        df_clean = read_key_from_nadlan_clean(get_db_engine(db_name='nextroof_db'))
        keys_to_remove = df_clean['key'].unique()
        df = df[~df['key'].isin(keys_to_remove)]
        return df

    except Exception as e:
        print(f"Error: {e}")
        return None


def read_key_from_nadlan_clean(engine):
    return pd.read_sql_query("SELECT key FROM nadlan_clean", engine)


def read_from_nadlan_rank(city_id):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM nadlan_rank WHERE city_id = %s order by year desc"
    df = pd.read_sql_query(query, engine, params=(city_id,))
    df = df.replace([None, 'NaN', ''], np.nan)
    df = df.dropna(subset=['gush'])
    df.loc[:, 'gush'] = df['gush'].astype(float).astype(np.int32)
    df.loc[:, 'helka'] = df['helka'].astype(float).astype(np.int32)

    return df

def read_from_nadlan_rank_find_floor(city_id, street_id, home_number):
    home_number = str(home_number).strip()
    engine = get_db_engine(db_name='nextroof_db')
    query = text(f"SELECT floors FROM nadlan_rank WHERE city_id = '{city_id}' AND street_id = '{street_id}' AND home_number = '{home_number}' ORDER BY floors DESC LIMIT 1;")
    with engine.connect() as connection:
        result = connection.execute(query)
        row = result.fetchone()
        return row[0] if row else np.nan

def read_from_address_addr_cache(city_id, street_id, home_number):
    home_number = str(home_number).strip()
    engine = get_db_engine(db_name='nextroof_db')
    query = text("""
        SELECT * FROM addr_cache 
        WHERE city_id = :city_id AND street_id = :street_id AND home_number = :home_number 
        ORDER BY date DESC 
        LIMIT 1;
    """)

    with engine.connect() as connection:
        result = connection.execute(query, {'city_id': int(city_id), 'street_id': int(street_id),'home_number': int(home_number)})
        row = result.fetchone()

    if row:
        row_dict = {column: value for column, value in row._mapping.items()}
        return row_dict
    else:
        return None
def read_from_nadlan_clean(city_id):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM nadlan_clean WHERE city_id = %s"
    df = pd.read_sql_query(query, engine, params=(city_id,))
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

def read_from_streets_table(city_id, reverse=False):
    engine = get_db_engine(db_name='nextroof_db')
    df = pd.read_sql_query("SELECT * FROM streets WHERE city_id = %s", engine, params=(int(city_id),))
    if reverse:
        return reverse_street(df)
    return df

def read_from_cities_table():
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT city_code, city_name FROM cities"
    df = pd.read_sql_query(query, engine)
    df.rename(columns={'city_code': 'city_id'}, inplace=True)
    df.rename(columns={'city_name': 'city'}, inplace=True)
    df['city'] = df['city'].str.strip()
    return df

def reverse_street(df):
    new_rows = []
    for index, row in df.iterrows():
        st = row['street']
        st_split = st.split(' ')

        if len(st_split) == 2:
            new_st = st_split[1] + ' ' + st_split[0]
            new_row = {
                'city_id': row['city_id'],
                'city': row['city'],
                'street_id': row['street_id'],
                'street': new_st,
            }
            new_rows.append(new_row)
    new_df = pd.DataFrame(new_rows)
    result_df = pd.concat([df, new_df], ignore_index=True)
    return result_df