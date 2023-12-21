from dev import get_db_connection

def add_new_deals_nadlan_raw(df):
    conflict_count = 0
    new_row_count = 0

    conn = get_db_connection()

    try:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                record = tuple(row)
                cursor.execute("""
                    INSERT INTO nadlan_raw (
                        dealdate, fulladress, displayadress, gush, dealnaturedescription,
                        assetroomnum, floorno, dealnature, dealamount, newprojecttext,
                        projectname, buildingyear, yearbuilt, buildingfloors, keyvalue,
                        type, polygon_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (keyvalue) DO NOTHING
                """, record)

                # Check if the last query resulted in an insert
                if cursor.rowcount > 0:
                    new_row_count += 1
                else:
                    conflict_count += 1

            conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

    print(f"New rows added: {new_row_count}")
    print(f"Conflicts encountered: {conflict_count}")
    data = {
        'new_rows': new_row_count,
        'conflict_rows': conflict_count,
    }
    return data


def add_new_deals_nadlan_clean(df):
    conflict_count = 0
    new_row_count = 0
    df = df.dropna(subset=['Street'])
    conn = get_db_connection(db_name='nextroof_db')
    with conn:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                record = (
                    row['Date'], row['Type'], row['Rooms'], row['Floor'], row['Size'], row['Price'],
                    row['New'], row['Build_year'], row['Rebuilt'], row['Floors'], row['Key'],
                    row['City'], row['Year'], row['Gush'], row['Helka'], row['Tat'],
                    row['Home_number'], row['Street'], row['Neighborhood'], row['Lat'], row['Long'],
                    row['X'], row['Y'], row['Zip'],row['Addr_key']
                )
                cursor.execute("""
                    INSERT INTO nadlan_clean (
                        Date, Type, Rooms, Floor, Size, Price, New, Build_year, Rebuilt, Floors, Key,
                        City, Year, Gush, Helka, Tat, Home_number, Street, Neighborhood, Lat, Long, X, Y, Zip , Addr_key
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (Key)
                    DO NOTHING
                """, record)

                if cursor.rowcount > 0:
                    new_row_count += 1
                else:
                    conflict_count += 1

        conn.commit()
        print(f"add_new_deals_nadlan_clean\nNew rows added: {new_row_count}, Conflicts encountered: {conflict_count}")
        data = {
            'new_rows' :new_row_count,
            'conflict_rows':conflict_count,
        }
        return data
def add_new_deals_nadlan_rank(df):
    new_row_count = 0
    updated_row_count = 0
    conn = get_db_connection(db_name='nextroof_db')

    try:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                record = (
                    row['date'], row['type'], row['rooms'], row['floor'], row['size'], row['price'],
                    row['new'], row['build_year'], row['rebuilt'], row['floors'], row['key'],
                    row['city'], row['year'], row['gush'], row['helka'], row['tat'],
                    row['home_number'], row['street'], row['neighborhood'], row['lat'], row['long'],
                    row['x'], row['y'], row['zip'], row['neighborhood_rank'], row['street_rank'], row['helka_rank']
                )
                cursor.execute("""
                    INSERT INTO nadlan_rank (
                        date, type, rooms, floor, size, price, new, build_year, rebuilt, floors, key,
                        city, year, gush, helka, tat, home_number, street, neighborhood, lat, long, x, y, zip,
                        neighborhood_rank, street_rank, helka_rank
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (key)
                    DO UPDATE SET
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
                        neighborhood_rank = EXCLUDED.neighborhood_rank,
                        street_rank = EXCLUDED.street_rank,
                        helka_rank = EXCLUDED.helka_rank
                    RETURNING (xmax = 0)
                """, record)

                result = cursor.fetchone()
                if result[0]:
                    new_row_count += 1
                else:
                    updated_row_count += 1

        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

    print(f"add_new_deals_nadlan_rank\nNew rows added: {new_row_count}, Rows updated: {updated_row_count}")
    data = {
        'new_rows': new_row_count,
        'updated_rows': updated_row_count,
    }
    return data

