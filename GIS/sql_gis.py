import pandas as pd
import numpy as np
from dev import get_db_engine
from sqlalchemy import text
from dev import get_db_connection


def save_into_db_city_rank(df):
    conflict_count = 0
    new_row_count = 0
    conn = get_db_connection(db_name='nextroof_db')

    try:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                # Convert numpy.int64 to Python int
                city_id = int(row['city_id'])
                year = int(row['year'])
                rank = int(row['rank'])

                sql = """
                    INSERT INTO city_rank (city_id, year, rank)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (city_id, year) DO UPDATE
                    SET rank = EXCLUDED.rank;
                """
                cursor.execute(sql, (city_id, year, rank))

                if cursor.rowcount > 0:
                    new_row_count += 1
                else:
                    conflict_count += 1

            conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()


def read_cities_to_dict():
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT city_code , city_name FROM cities"
    df = pd.read_sql_query(query, engine)
    city_dict = dict(zip(list(df['city_name']), list(df['city_code'])))
    return city_dict

def read_distinct_cities_id():
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT distinct(city_id) as city_id_distinct FROM deals_2"
    df = pd.read_sql_query(query, engine)
    return list(df['city_id_distinct'])
def read_city_from_deals(city_id):
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM deals_2 WHERE city_id = %s"
    df = pd.read_sql_query(query, engine, params=(city_id,))
    return df