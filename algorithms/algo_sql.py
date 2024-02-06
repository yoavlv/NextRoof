import pandas as pd
import numpy as np
from dev import get_db_engine


def read_from_nadlan_rank(city):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM nadlan_rank WHERE city = %s"
    df = pd.read_sql_query(query, engine, params=(city,))
    df.replace({'NaN': np.nan, 'None': np.nan}, inplace=True)
    df = df.dropna(subset=['gush'])
    df.loc[:, 'gush'] = df['gush'].astype(float).astype(np.int32)
    df.loc[:, 'helka'] = df['helka'].astype(float).astype(np.int32)
    return df