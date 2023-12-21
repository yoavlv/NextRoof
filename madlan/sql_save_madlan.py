import numpy as np
import pandas as pd
from dev import get_db_connection
def add_new_deals_madlan_raw(df):
    new_row_count = 0
    updated_row_count = 0
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                record = (
                    row['Item_id'], row['Lat'], row['Long'], row['City'], row['Home_number'], row['Street'],
                    row['Rooms'], row['Neighborhood'], row['Floor'], row['Build_year'], row['Size'],
                    row['Price'], row['Condition'], row['Last_update'], row['Agency'], row['Asset_type'],
                    row['Images']
                )
                cursor.execute("""
                    INSERT INTO madlan_raw (
                        item_id, lat, long, city, home_number, street, rooms, neighborhood, floor,
                        build_year, size, price, condition, last_update, agency, asset_type, images
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (item_id)
                    DO UPDATE SET
                        lat = EXCLUDED.lat,
                        long = EXCLUDED.long,
                        city = EXCLUDED.city,
                        home_number = EXCLUDED.home_number,
                        street = EXCLUDED.street,
                        rooms = EXCLUDED.rooms,
                        neighborhood = EXCLUDED.neighborhood,
                        floor = EXCLUDED.floor,
                        build_year = EXCLUDED.build_year,
                        size = EXCLUDED.size,
                        price = EXCLUDED.price,
                        condition = EXCLUDED.condition,
                        last_update = EXCLUDED.last_update,
                        agency = EXCLUDED.agency,
                        asset_type = EXCLUDED.asset_type,
                        images = EXCLUDED.images
                    RETURNING (xmax = 0)
                """, record)

                result = cursor.fetchone()
                if result[0]:
                    new_row_count += 1
                else:
                    updated_row_count += 1

        conn.commit()
        print(f"add_new_deals_madlan_raw:\nNew rows added: {new_row_count}, Rows updated: {updated_row_count}")

    return {
            'new_rows': new_row_count,
            'updated_rows': updated_row_count,
        }

def add_new_deals_madlan_clean(df):
    new_row_count = 0
    updated_row_count = 0
    df = df.dropna(subset=['street'])
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                record = (
                    row['item_id'], row['lat'], row['long'], row['city'], row['home_number'], row['street'],
                    row['rooms'], row['neighborhood'], row['floor'], row['build_year'], row['size'], row['price'],
                    row['condition'], row['last_update'], row['agency'], row['asset_type'], row['images'],
                    row['gush'], row['helka'], row['x'], row['y'], row['zip'], row['addr_key']
                )
                cursor.execute("""
                    INSERT INTO madlan_clean(
                        item_id, lat, long, city, home_number, street, rooms, neighborhood, floor, build_year, size, price, 
                        condition, last_update, agency, asset_type, images, gush, helka, x, y, zip, addr_key
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (item_id) DO UPDATE SET
                        lat = EXCLUDED.lat,
                        long = EXCLUDED.long,
                        city = EXCLUDED.city,
                        home_number = EXCLUDED.home_number,
                        street = EXCLUDED.street,
                        rooms = EXCLUDED.rooms,
                        neighborhood = EXCLUDED.neighborhood,
                        floor = EXCLUDED.floor,
                        build_year = EXCLUDED.build_year,
                        size = EXCLUDED.size,
                        price = EXCLUDED.price,
                        condition = EXCLUDED.condition,
                        last_update = EXCLUDED.last_update,
                        agency = EXCLUDED.agency,
                        asset_type = EXCLUDED.asset_type,
                        images = EXCLUDED.images,
                        gush = EXCLUDED.gush,
                        helka = EXCLUDED.helka,
                        x = EXCLUDED.x,
                        y = EXCLUDED.y,
                        zip = EXCLUDED.zip,
                        addr_key = EXCLUDED.addr_key
                    RETURNING (xmax = 0)
                """, record)

                result = cursor.fetchone()
                if result[0]:
                    new_row_count += 1
                else:
                    updated_row_count += 1

        conn.commit()
        print(f"add_new_deals_madlan_clean:\nNew rows added: {new_row_count}, Rows updated: {updated_row_count}")
    return {
            'new_rows': new_row_count,
            'updated_rows': updated_row_count,
        }

def add_new_deals_madlan_rank(df):
    new_row_count = 0
    updated_row_count = 0
    def strip_string(x):
        return x.strip() if isinstance(x, str) else x

    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].map(strip_string)

    for col in ['size', 'floors', 'floor', 'price', 'rooms']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col].replace([np.inf, -np.inf], np.nan, inplace=True)
        df.dropna(subset=[col], inplace=True)
        if col in ['size','price']:
            df[col] = df[col].astype(float).astype(np.int32)
        else:
            df[col] = df[col].astype(float)

    conn = get_db_connection(db_name='nextroof_db')
    with conn:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                record = (
                    row['item_id'], row['lat'], row['long'], row['city'], row['home_number'], row['street'],
                    row['rooms'], row['neighborhood'], row['floor'], row['build_year'], row['size'], row['price'],
                    row['condition'], row['last_update'], row['agency'], row['asset_type'], row['images'],
                    row['gush'], row['helka'], row['x'], row['y'], row['zip'], row['addr_key'],
                    row['helka_rank'], row['street_rank'], row['neighborhood_rank'], row['floors']
                )
                cursor.execute("""
                    INSERT INTO madlan_rank (
                        item_id, lat, long, city, home_number, street, rooms, neighborhood, floor,
                        build_year, size, price, condition, last_update, agency, asset_type, images,
                        gush, helka, x, y, zip, addr_key, helka_rank, street_rank,
                        neighborhood_rank, floors
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (item_id)
                    DO UPDATE SET
                        lat = EXCLUDED.lat,
                        long = EXCLUDED.long,
                        city = EXCLUDED.city,
                        home_number = EXCLUDED.home_number,
                        street = EXCLUDED.street,
                        rooms = EXCLUDED.rooms,
                        neighborhood = EXCLUDED.neighborhood,
                        floor = EXCLUDED.floor,
                        build_year = EXCLUDED.build_year,
                        size = EXCLUDED.size,
                        price = EXCLUDED.price,
                        condition = EXCLUDED.condition,
                        last_update = EXCLUDED.last_update,
                        agency = EXCLUDED.agency,
                        asset_type = EXCLUDED.asset_type,
                        images = EXCLUDED.images,
                        gush = EXCLUDED.gush,
                        helka = EXCLUDED.helka,
                        x = EXCLUDED.x,
                        y = EXCLUDED.y,
                        zip = EXCLUDED.zip,
                        addr_key = EXCLUDED.addr_key,
                        helka_rank = EXCLUDED.helka_rank,
                        street_rank = EXCLUDED.street_rank,
                        neighborhood_rank = EXCLUDED.neighborhood_rank,
                        floors = EXCLUDED.floors
                    RETURNING (xmax = 0)
                """, record)

                result = cursor.fetchone()
                if result[0]:
                    new_row_count += 1
                else:
                    updated_row_count += 1

        conn.commit()
        print(f"add_new_deals_madlan_rank:\nNew rows added: {new_row_count}, Rows updated: {updated_row_count}")
    return {
            'new_rows': new_row_count,
            'updated_rows': updated_row_count,
        }

def add_new_deals_madlan_predict(df):
    new_row_count = 0
    updated_row_count = 0
    conn = get_db_connection(db_name='nextroof_db')

    with conn:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                record = (row['item_id'], row['price'], row['predicted'], row['difference'])
                cursor.execute("""
                    INSERT INTO madlan_predict (item_id, price, predicted, difference) 
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (item_id) DO UPDATE SET
                        price = EXCLUDED.price,
                        predicted = EXCLUDED.predicted,
                        difference = EXCLUDED.difference
                    RETURNING (xmax = 0)
                """, record)

                result = cursor.fetchone()
                if result[0]:
                    new_row_count += 1
                else:
                    updated_row_count += 1
        conn.commit()
        print(f"add_new_deals_madlan_predict:\nNew rows added: {new_row_count}, Rows updated: {updated_row_count}")
    return {
                'new_rows': new_row_count,
                'updated_rows': updated_row_count,
            }
