import pandas as pd
import numpy as np
from dev import get_db_engine

def read_addr_table(city):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM addr_cache WHERE LENGTH(neighborhood) > 3 and city LIKE %s"
    params = ('%' + city + '%',)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn,params=params)
    return df

def read_from_madlan_raw(city=None):
    engine = get_db_engine()
    if city:
        query = "SELECT * FROM madlan_raw WHERE city LIKE %s"
        params = ('%' + city + '%',)
    else:
        query = "SELECT * FROM madlan_raw"
        params = None  # No parameters needed

    with engine.connect() as conn:
        if params:
            df = pd.read_sql_query(query, conn, params=params)
        else:
            df = pd.read_sql_query(query, conn)

    for col in df.columns:
        df[col] = df[col].replace('NaN', np.nan).replace('', np.nan)
    return df

def read_from_madlan_rank(city):
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