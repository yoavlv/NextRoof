import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.model_selection import train_test_split
import pickle
from algorithms.model import data_prep , get_stacking
from conf import PROD
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import StackingRegressor
from algorithms.model_data import best_params

@pytest.mark.skipif(not PROD, reason="Need DB connection")
@pytest.fixture
def input_data():
    from dev import get_db_engine
    engine = get_db_engine(db_name='nextroof_db')
    query = "SELECT * FROM nadlan_rank LIMIT 1000"
    df = pd.read_sql_query(query, engine)
    return df

@pytest.mark.skipif(not PROD, reason="Need DB connection")
@pytest.mark.parametrize('scaler', ['StandardScaler', 'MinMaxScaler'])
def test_data_prep(input_data, scaler):

    X_train_scaled, X_test_scaled, y_train, y_test, X_train ,scaler_bytes = data_prep(input_data, scaler=scaler)
    expected_columns = ["rooms", "floor", "size", "build_year", "floors","year", 'age', 'neighborhood_rank', 'street_rank', 'helka_rank', 'new']

    assert X_train_scaled.shape[1] == len(expected_columns)
    assert X_test_scaled.shape[1] == len(expected_columns)
    assert isinstance(scaler_bytes, bytes)



def test_get_stacking():
    model = get_stacking(best_params)
    assert isinstance(model, StackingRegressor)
    assert len(model.estimators) >= 4
    assert isinstance(model.final_estimator, LinearRegression)
    assert model.cv == 5
