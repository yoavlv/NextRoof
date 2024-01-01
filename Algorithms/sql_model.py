from dev import get_db_connection

def insert_ml_model(model_name, city_code, model_data, model_scaler, scores, params ,hostname='localhost'):
    try:
        conn = get_db_connection(db_name='nextroof_db',host_name=hostname)
        with conn:
            with conn.cursor() as cursor:
                insert_sql = """
                    INSERT INTO ml_models (model_name, city_code, model_data, model_scaler, scores, params)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (city_code) DO UPDATE
                    SET model_data = EXCLUDED.model_data,
                        model_scaler = EXCLUDED.model_scaler,
                        scores = EXCLUDED.scores,
                        params = EXCLUDED.params
                """
                cursor.execute(insert_sql, (model_name, city_code, model_data, model_scaler, scores, params))

        print("insert_ml_model: Data inserted or replaced successfully")

    except Exception as e:
        print(f"insert_ml_model: An error occurred: {e}")
