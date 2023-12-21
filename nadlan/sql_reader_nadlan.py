import pandas as pd
import numpy as np
from dev import get_db_engine

def read_raw_data_table(num_of_rows=100, city=None):
    engine = get_db_engine()
    try:
        if city:
            query = "SELECT * FROM nadlan_raw WHERE city = %s ORDER BY created_at DESC LIMIT %s"
            df = pd.read_sql_query(query, engine, params=(city, num_of_rows))
        else:
            query = "SELECT * FROM nadlan_raw ORDER BY created_at DESC LIMIT %s"
            df = pd.read_sql_query(query, engine, params=(num_of_rows,))

        df.columns = [col.upper() for col in df.columns]
        df_clean = read_key_from_nadlan_clean(get_db_engine(db_name='nextroof_db'))
        keys_to_remove = df_clean['key'].unique()
        df = df[~df['KEYVALUE'].isin(keys_to_remove)]

        for col in df.columns:
            df[col] = df[col].replace('NaN', np.nan).replace('', np.nan)  # Replace 'NaN' strings with np.nan

        df = convert_data_types(df)

        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

def convert_data_types(df):
    float_columns = ['ASSETROOMNUM', 'DEALNATURE', 'NEWPROJECTTEXT', 'BUILDINGYEAR', 'YEARBUILT', 'BUILDINGFLOORS', 'TYPE']
    for col in float_columns:
        if col in df.columns:
            df[col] = df[col].astype(float)
    return df

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

def read_from_nadlan_clean(city):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM nadlan_clean WHERE city = %s"
    df = pd.read_sql_query(query, engine, params=(city,))
    df.replace({'NaN': np.nan, 'None': np.nan}, inplace=True)
    df = df.dropna(subset=['gush'])
    df.loc[:, 'gush'] = df['gush'].astype(float).astype(np.int32)
    df.loc[:, 'helka'] = df['helka'].astype(float).astype(np.int32)
    return df