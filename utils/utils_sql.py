import psycopg2
from psycopg2 import sql
from dev import get_db_connection
import numpy as np

class DatabaseManager:
    def __init__(self, table_name, db_name='nextroof_db', host_name='localhost'):
        self.db_name = db_name
        self.host_name = host_name
        self.table_name = table_name
        self.conn = self.get_db_connection()
        self.success = True
        self.new_row_count = 0
        self.conflict_count = 0

    def get_db_connection(self):
        return get_db_connection(self.db_name, self.host_name)

    def preprocess_values(self, row):
        # Convert numpy types to Python types
        return tuple((None if (value == '' or value == 'NaN') else
                      value) for value in row)

    def close_connection(self):
        if self.conn is not None:
            self.conn.close()

    def prepare_insert_query(self, columns, pk_columns, action='update'):
        columns_sql = sql.SQL(', ').join(map(sql.Identifier, columns))
        placeholders_sql = sql.SQL(', ').join(sql.Placeholder() * len(columns))

        pk_columns_sql = sql.SQL(', ').join(map(sql.Identifier, pk_columns))
        if action == 'update':
            conflict_sql = sql.SQL("ON CONFLICT ({}) DO UPDATE SET ").format(pk_columns_sql)
            updates = sql.SQL(', ').join(
                [sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col)) for col in columns if
                 col not in pk_columns])
            conflict_sql = conflict_sql + updates
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

    def prepare_insert_query_no_pk(self, columns):
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

    def check_for_existence(self, pk_columns, pk_values):
        condition_sql = sql.SQL(" AND ").join(
            [sql.SQL("{} = %s").format(sql.Identifier(col)) for col in pk_columns])
        query = sql.SQL("SELECT EXISTS(SELECT 1 FROM {} WHERE {})").format(
            sql.Identifier(self.table_name), condition_sql)

        # Explicitly convert pk_values to native Python types
        pk_values_python = tuple(value.item() if isinstance(value, np.generic) else value for value in pk_values)

        with self.conn.cursor() as cursor:
            cursor.execute(query, pk_values_python)  # Use the converted tuple here
            exists_before_insert = cursor.fetchone()[0] > 0
            return exists_before_insert

    def insert_record(self, row, columns, pk_columns=None, action='update'):
        processed_record = self.preprocess_values(row)

        if pk_columns:
            pk_values = tuple(row[col] for col in pk_columns)
            exists = self.check_for_existence(pk_columns, pk_values)

            if not exists:
                self.new_row_count += 1
            else:
                self.conflict_count += 1

        try:
            if pk_columns:
                insert_query = self.prepare_insert_query(columns, pk_columns, action)
            else:
                insert_query = self.prepare_insert_query_no_pk(columns)

            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, processed_record)
                self.conn.commit()
        except psycopg2.Error as e:
            self.success = False
            print(f"Error processing row: {e}")

    def insert_record_from_df(self, row, columns, pk_columns, action='update'):
        pk_values = tuple(row[col] for col in pk_columns)

        # Need to optimize this
        exists = self.check_for_existence(pk_columns, pk_values)

        if not exists:
            self.new_row_count += 1
        else:
            self.conflict_count += 1

        try:
            insert_query = self.prepare_insert_query(columns, pk_columns, action)
            with self.conn.cursor() as cursor:
                cursor.execute(insert_query, row)
                self.conn.commit()
        except psycopg2.Error as e:
            self.success = False
            print(f"Error processing row: {e}")

    def insert_dataframe(self, df, pk_columns, action='update'):
        pk_columns = pk_columns.split() if isinstance(pk_columns, str) else pk_columns
        df = df.replace(['None','NaN', np.nan, ''], None)

        for index, row in df.iterrows():
            self.insert_record_from_df(row, df.columns, pk_columns, action)

        return self.success, self.new_row_count, self.conflict_count

    def __str__(self):
        return f"DatabaseManager Table: {self.table_name} status- Success: {self.success}, New rows added: {self.new_row_count}, Conflicts: {self.conflict_count}"

# class DatabaseManagerReader:
#     def __init__(self, db_name, host_name, table_name):
#         self.db_name = db_name
#         self.host_name = host_name
#         self.table_name = table_name
#         self.conn = self.get_db_connection()
#         self.success = True
#         self.new_row_count = 0
#         self.conflict_count = 0
#
#     def get_db_connection(self):
#         return get_db_connection(self.db_name, self.host_name)
#
#     def read_to_data_frame(self,selected_columns,):
#         condition_sql = sql.SQL(" AND ").join(
#             [sql.SQL("{} = %s").format(sql.Identifier(col)) for col in pk_columns])
#         query = sql.SQL("SELECT EXISTS(SELECT 1 FROM {} WHERE {})").format(
#             sql.Identifier(self.table_name), condition_sql)
