from typing import List, Tuple, Union, Any
from dev import get_db_connection
import psycopg2
from psycopg2 import sql, extras
import numpy as np
import pandas as pd
from psycopg2.extensions import register_adapter, AsIs
import os
def sql_script(sql_file_name: str)-> bool:
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        script_dir = os.path.dirname(__file__)
        full_path = os.path.join(script_dir, sql_file_name)
        with open(full_path, 'r') as sql_file:
            sql_script = sql_file.read()

        cur.execute(sql_script)
        conn.commit()
        cur.close()
        conn.close()
        print(f"SQL file executed successfully. {sql_file_name}")
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error {sql_file_name}: {error}")
        return False
    finally:
        conn.close()


class DatabaseManager:
    def __init__(self, table_name: str, db_name: str = 'nextroof_db', host_name: str = 'nextroof-rds.cboisuqgg7m3.eu-north-1.rds.amazonaws.com') -> None:
        self.db_name: str = db_name
        self.host_name: str = host_name
        self.table_name: str = table_name
        self.conn = self.get_db_connection()
        self.success: bool = True

    def get_db_connection(self) -> psycopg2.extensions.connection:
        return get_db_connection(self.db_name, self.host_name)

    def preprocess_values(self, row: Tuple[Any, ...]) -> Tuple[Any, ...]:
        return tuple((None if (value == '' or value == '<NA>' or value == 'NaN' or pd.isna(value)) else value) for value in row)

    def close_connection(self) -> None:
        if self.conn is not None:
            self.conn.close()

    def prepare_insert_query(self, columns: List[str], pk_columns: List[str], replace: bool = True) -> sql.Composed:
        columns_sql = sql.SQL(', ').join(map(sql.Identifier, columns))
        placeholders_sql = sql.SQL(', ').join(sql.Placeholder() * len(columns))

        pk_columns_sql = sql.SQL(', ').join(map(sql.Identifier, pk_columns))
        if replace:
            conflict_sql = sql.SQL("ON CONFLICT ({}) DO UPDATE SET ").format(pk_columns_sql)
            updates = sql.SQL(', ').join(
                [sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col)) for col in columns if col not in pk_columns])
            conflict_sql += updates
        else:
            conflict_sql = sql.SQL("ON CONFLICT ({}) DO NOTHING").format(pk_columns_sql)

        return sql.SQL("""
            INSERT INTO {table} ({columns})
            VALUES ({values})
            {conflict_sql}
        """).format(
            table=sql.Identifier(self.table_name),
            columns=columns_sql,
            values=placeholders_sql,
            conflict_sql=conflict_sql
        )

    def prepare_insert_query_no_pk(self, columns: List[str]) -> sql.Composed:
        columns_sql = sql.SQL(', ').join(map(sql.Identifier, columns))
        placeholders_sql = sql.SQL(', ').join(sql.Placeholder() * len(columns))

        return sql.SQL("""
            INSERT INTO {table} ({columns})
            VALUES ({values})
        """).format(
            table=sql.Identifier(self.table_name),
            columns=columns_sql,
            values=placeholders_sql,
        )

    def check_for_existence(self, pk_columns: List[str], pk_values: Tuple[Any, ...]) -> bool:
        condition_sql = sql.SQL(" AND ").join(
            [sql.SQL("{} = %s").format(sql.Identifier(col)) for col in pk_columns])
        query = sql.SQL("SELECT EXISTS(SELECT 1 FROM {} WHERE {})").format(
            sql.Identifier(self.table_name), condition_sql)

        pk_values_python = tuple(value.item() if isinstance(value, np.generic) else value for value in pk_values)

        with self.conn.cursor() as cursor:
            cursor.execute(query, pk_values_python)
            exists_before_insert = cursor.fetchone()[0] > 0
            return exists_before_insert

    def insert_record(self, row: Tuple[Any, ...], columns: List[str], pk_columns: Union[List[str], None] = None, replace: bool = True) -> None:
        processed_record = self.preprocess_values(row)

        try:
            if pk_columns:
                insert_query = self.prepare_insert_query(columns, pk_columns, replace)
            else:
                insert_query = self.prepare_insert_query_no_pk(columns)

            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, processed_record)
                self.conn.commit()
        except psycopg2.Error as e:
            self.success = False
            print(f"Error processing row: {e}")

    def fetch_existing_pks(self, pk_columns: List[str]) -> List[Tuple[Any, ...]]:
        pk_columns_sql = ', '.join([f'"{col}"' for col in pk_columns])
        query = f'SELECT {pk_columns_sql} FROM "{self.table_name}"'
        with self.conn.cursor() as cur:
            cur.execute(query)
            existing_pks = cur.fetchall()
        return existing_pks

    def count_pk_conflicts(self, df: pd.DataFrame, pk_columns: List[str]) -> int:
        existing_pks = self.fetch_existing_pks(pk_columns)
        df_pks = set(tuple(row[col] for col in pk_columns) for _, row in df.iterrows())

        conflict_count = len(df_pks.intersection(existing_pks))
        return conflict_count

    def insert_dataframe_batch(self, df: pd.DataFrame, pk_columns: Union[List[str], None, str] = None,
                               columns: Union[List[str], None] = None, batch_size: int = 1000, replace: bool = False) -> \
    Tuple[bool, int, int]:
        psycopg2.extensions.register_adapter(np.int32, psycopg2._psycopg.AsIs)
        psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
        if batch_size <= 0:
            raise ValueError(f"batch_size must be a positive integer {self.table_name}")

        df = self.replace_nan_value_df(df)
        if pk_columns is not None:
            pk_columns = pk_columns.split() if isinstance(pk_columns, str) else pk_columns
        if columns is None:
            columns = df.columns.tolist()

        if not isinstance(columns, list):
            raise ValueError('Type of columns should be list')


        tuples = [tuple(x) for x in df.to_numpy()]
        potential_conflicts = self.count_pk_conflicts(df, pk_columns) if pk_columns else 0
        rows_processed = len(tuples)
        rows_conflicted = potential_conflicts
        rows_inserted = rows_processed - rows_conflicted

        on_conflict_clause = ""
        if pk_columns:
            pk_columns_sql = ", ".join([f"\"{col}\"" for col in pk_columns])
            if replace:
                update_columns = [col for col in columns if col not in pk_columns]
                updates = ", ".join([f"\"{col}\" = EXCLUDED.\"{col}\"" for col in update_columns]) if update_columns else ""
                on_conflict_clause = f"ON CONFLICT ({pk_columns_sql}) DO UPDATE SET {updates}" if updates else f"ON CONFLICT ({pk_columns_sql}) DO NOTHING"
            else:
                on_conflict_clause = f"ON CONFLICT ({pk_columns_sql}) DO NOTHING"

        try:
            with self.conn.cursor() as cur:
                columns_sql = ", ".join([f"\"{col}\"" for col in columns])
                placeholders = ", ".join(['%s'] * len(columns))
                insert_query = f"INSERT INTO \"{self.table_name}\" ({columns_sql}) VALUES ({placeholders}) {on_conflict_clause}"

                for batch in [tuples[i:i + batch_size] for i in range(0, len(tuples), batch_size)]:
                    extras.execute_batch(cur, insert_query, batch)

                self.conn.commit()

        except Exception as e:
            print(f"Error inserting data into table {self.table_name}: {e}")
            self.conn.rollback()
            return False, 0, 0

        print(f"Finished inserting data into table {self.table_name}.")
        print(f"Total rows processed: {rows_processed}\nNew rows inserted: {rows_inserted}")
        if replace == True:
            print(f"Rows updated (replace=True): {rows_conflicted}")
        else:
            print(f"Rows conflicted (replace=False): {rows_conflicted}")

        return True, rows_inserted, rows_conflicted

    def insert_record_from_df(self, row: pd.Series, columns: List[str], pk_columns: List[str], replace: bool = True) -> None:
        try:
            insert_query = self.prepare_insert_query(columns, pk_columns, replace)
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, tuple(row))
                self.conn.commit()
        except psycopg2.Error as e:
            self.success = False
            print(f"Error processing row: {e}")

    def insert_dataframe(self, df: pd.DataFrame, pk_columns: Union[str, List[str]], replace: bool = True) -> Tuple[bool, int, int]:
        potential_conflicts = self.count_pk_conflicts(df, pk_columns)

        total_row = int(df.shape[0])
        rows_conflicted = potential_conflicts
        rows_inserted = total_row - rows_conflicted

        pk_columns = pk_columns.split() if isinstance(pk_columns, str) else pk_columns
        df = self.replace_nan_value_df(df)

        for index, row in df.iterrows():
            self.insert_record_from_df(row, df.columns.tolist(), pk_columns, replace)

        return True, rows_inserted, rows_conflicted

    def replace_nan_value_df(self,df: pd.DataFrame)-> pd.DataFrame:
        return df.replace(['None', 'NaN', np.nan, '','NaT','<NA>'], None)