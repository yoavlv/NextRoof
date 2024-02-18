from dev import get_db_connection
from tqdm import tqdm
import psycopg2

def add_new_deals_nadlan_raw(df):
    conn = get_db_connection(db_name='nadlan_db')
    data = {
        'new_rows': 0,
        'conflict_rows': 0,
    }
    if conn is not None:
        try:
            cur = conn.cursor()
            total_rows = 0
            inserted_rows = 0

            for index, row in df.iterrows():
                query = """
                    INSERT INTO nadlan_raw (
                        dealdate, fulladress, displayadress, gush, 
                        dealnaturedescription, assetroomnum, floorno, 
                        dealnature, dealamount, newprojecttext, 
                        projectname, buildingyear, yearbuilt, 
                        buildingfloors, keyvalue, type, 
                        polygon_id, city_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    ) ON CONFLICT (keyvalue) DO NOTHING;
                """

                row_values = (
                    row['dealdate'], row['fulladress'], row['displayadress'], row['gush'],
                    row['dealnaturedescription'], row['assetroomnum'], row['floorno'],
                    row['dealnature'], row['dealamount'], row['newprojecttext'],
                    row['projectname'], row['buildingyear'], row['yearbuilt'],
                    row['buildingfloors'], row['keyvalue'], row['type'],
                    row['polygon_id'], row['city_id']
                )

                cur.execute(query, row_values)
                inserted_rows += cur.rowcount
                total_rows += 1

            conn.commit()
            conflict_count = total_rows - inserted_rows
            data = {
                'new_rows': inserted_rows,
                'conflict_rows': conflict_count,
            }

        except Exception as e:
            print(f"An error occurred (add_new_deals_nadlan_raw): {e}")
            conn.rollback()
        finally:
            conn.close()
    return data

import numpy as np
def add_new_deals_nadlan_clean(df, host_name='localhost'):
    conflict_count = 0
    new_row_count = 0
    df = df.dropna(subset=['street'])
    conn = get_db_connection(db_name='nextroof_db', host_name=host_name)
    df['city_id'] = df['city_id'].astype(np.int32)
    df['street_id'] = df['street_id'].astype(np.int32)

    columns = [
        'date', 'type', 'rooms', 'floor', 'size', 'price',
        'new', 'build_year', 'rebuilt', 'floors', 'key',
        'city', 'year', 'gush', 'helka', 'tat',
        'home_number', 'street', 'neighborhood', 'lat', 'long',
        'x', 'y', 'zip', 'city_id', 'street_id'
    ]

    with conn:
        with conn.cursor() as cursor:
            for index, row in df.iterrows():
                record = tuple(row[col] for col in columns)
                try:
                    cursor.execute("""
                        INSERT INTO nadlan_clean (
                            Date, Type, Rooms, Floor, Size, Price, New, Build_year, Rebuilt, Floors, Key,
                            City, Year, Gush, Helka, Tat, Home_number, Street, Neighborhood, Lat, Long, X, Y, Zip, City_id, Street_id
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (Key)
                        DO UPDATE SET
                            Date = EXCLUDED.Date, Type = EXCLUDED.Type, Rooms = EXCLUDED.Rooms,
                            Floor = EXCLUDED.Floor, Size = EXCLUDED.Size, Price = EXCLUDED.Price,
                            New = EXCLUDED.New, Build_year = EXCLUDED.Build_year, Rebuilt = EXCLUDED.Rebuilt,
                            Floors = EXCLUDED.Floors, City = EXCLUDED.City, Year = EXCLUDED.Year,
                            Gush = EXCLUDED.Gush, Helka = EXCLUDED.Helka, Tat = EXCLUDED.Tat,
                            Home_number = EXCLUDED.Home_number, Street = EXCLUDED.Street,
                            Neighborhood = EXCLUDED.Neighborhood, Lat = EXCLUDED.Lat, Long = EXCLUDED.Long,
                            X = EXCLUDED.X, Y = EXCLUDED.Y, Zip = EXCLUDED.Zip,
                            City_id = EXCLUDED.City_id, Street_id = EXCLUDED.Street_id                    
                    """, record)
                    if cursor.rowcount > 0:
                        new_row_count += 1
                    else:
                        conflict_count += 1
                except psycopg2.errors.NumericValueOutOfRange as e:
                    print(f"Error processing row {index}: {e}")
                    for col, value in zip(columns, record):
                        print(f"Column: {col}, Value: {value}")
                    # Break or continue based on your preference
                    # break

        conn.commit()
        print(f"(add_new_deals_nadlan_clean)\nNew rows added: {new_row_count}, Conflicts updated: {conflict_count}")
        data = {
            'new_rows': new_row_count,
            'conflict_rows': conflict_count,
        }
        return data


def add_new_deals_nadlan_rank(df, host_name='localhost'):
    new_row_count = 0
    updated_row_count = 0
    conn = get_db_connection(db_name='nextroof_db', host_name=host_name)
    try:
        with conn.cursor() as cursor:
            for _, row in tqdm(df.iterrows(), total=len(df), desc="Add_new_deals_nadlan_rank"):
                record = (
                    row['date'], row['type'], row['rooms'], row['floor'], row['size'], row['price'],
                    row['new'], row['build_year'], row['rebuilt'], row['floors'], row['key'],
                    row['city'], row['year'], row['gush'], row['helka'], row['tat'],
                    row['home_number'], row['street'], row['neighborhood'], row['lat'], row['long'],
                    row['x'], row['y'], row['zip'], row['city_id'], row['street_id'],
                    row['gush_rank'], row['street_rank'], row['helka_rank']
                )

                # Save the current row count before the operation
                before_rowcount = cursor.rowcount

                cursor.execute("""
                    INSERT INTO nadlan_rank (
                        date, type, rooms, floor, size, price, new, build_year, rebuilt, floors, key,
                        city, year, gush, helka, tat, home_number, street, neighborhood, lat, long, x, y, zip,
                        city_id, street_id, gush_rank, street_rank, helka_rank
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (key) DO UPDATE SET
                        date = EXCLUDED.date,
                        type = EXCLUDED.type,
                        rooms = EXCLUDED.rooms,
                        floor = EXCLUDED.floor,
                        size = EXCLUDED.size,
                        price = EXCLUDED.price,
                        new = EXCLUDED.new,
                        build_year = EXCLUDED.build_year,
                        rebuilt = EXCLUDED.rebuilt,
                        floors = EXCLUDED.floors,
                        city = EXCLUDED.city,
                        year = EXCLUDED.year,
                        gush = EXCLUDED.gush,
                        helka = EXCLUDED.helka,
                        tat = EXCLUDED.tat,
                        home_number = EXCLUDED.home_number,
                        street = EXCLUDED.street,
                        neighborhood = EXCLUDED.neighborhood,
                        lat = EXCLUDED.lat,
                        long = EXCLUDED.long,
                        x = EXCLUDED.x,
                        y = EXCLUDED.y,
                        zip = EXCLUDED.zip,
                        city_id = EXCLUDED.city_id,
                        street_id = EXCLUDED.street_id,
                        gush_rank = EXCLUDED.gush_rank,
                        street_rank = EXCLUDED.street_rank,
                        helka_rank = EXCLUDED.helka_rank
                    """, record)

                # Check if the row was updated by comparing the rowcount before and after
                if cursor.rowcount > before_rowcount:
                    updated_row_count += 1
                else:
                    new_row_count += 1

        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

    print(f"add_new_deals_nadlan_rank\nNew rows added: {new_row_count}, Rows updated: {updated_row_count}, host: {host_name}")
    return {
        'new_rows': new_row_count,
        'updated_rows': updated_row_count,
    }

def add_new_deals_nadlan_clean_neighborhood_complete(df, host_name='localhost'):
    conflict_count = 0
    new_row_count = 0
    df = df.dropna(subset=['street'])
    conn = get_db_connection(db_name='nextroof_db', host_name=host_name)

    with conn:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                record = (
                    row['date'], row['type'], row['rooms'], row['floor'], row['size'], row['price'],
                    row['new'], row['build_year'], row['rebuilt'], row['floors'], row['key'],
                    row['city'], row['year'], row['gush'], row['helka'], row['tat'],
                    row['home_number'], row['street'], row['neighborhood'], row['lat'], row['long'],
                    row['x'], row['y'], row['zip']
                )
                cursor.execute("""
                    INSERT INTO nadlan_clean (
                        date, type, rooms, floor, size, price, new, build_year, rebuilt, floors, key,
                        city, year, gush, helka, tat, home_number, street, neighborhood, lat, long, x, y, zip
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (key)
                    DO UPDATE SET
                        date = EXCLUDED.date, type = EXCLUDED.type, rooms = EXCLUDED.rooms,
                        floor = EXCLUDED.floor, size = EXCLUDED.size, price = EXCLUDED.price,
                        new = EXCLUDED.new, build_year = EXCLUDED.build_year, rebuilt = EXCLUDED.rebuilt,
                        floors = EXCLUDED.floors, city = EXCLUDED.city, year = EXCLUDED.year,
                        gush = EXCLUDED.gush, helka = EXCLUDED.helka, tat = EXCLUDED.tat,
                        home_number = EXCLUDED.home_number, street = EXCLUDED.street,
                        neighborhood = EXCLUDED.neighborhood, lat = EXCLUDED.lat, long = EXCLUDED.long,
                        x = EXCLUDED.x, y = EXCLUDED.y, zip = EXCLUDED.zip
                """, record)

                if cursor.rowcount == 0:
                    conflict_count += 1
                else:
                    new_row_count += 1

        conn.commit()
        print(f"add_new_deals_nadlan_clean_neighborhood_complete\nNew rows added: {new_row_count}, Conflicts encountered and updated: {conflict_count}")
