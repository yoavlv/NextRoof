from numpy import mean ,std
import numpy as np
from sklearn.model_selection import cross_val_score , cross_validate ,RepeatedKFold ,GridSearchCV
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.ensemble import StackingRegressor ,RandomForestRegressor , GradientBoostingRegressor
from sklearn.metrics import r2_score ,mean_absolute_error
from catboost import CatBoostRegressor
import joblib
from Algorithms.model_data import params, best_params, models_list, lean_params
from Algorithms.model_plots import result_plot , plot_model_scores
from monitor import monitor_data
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from datetime import datetime
import pandas as pd
from .algo_sql import read_from_nadlan_rank
import traceback
def get_models_with_best_params(best_params):
    models = dict()
    models['CatBoostRegressor'] = CatBoostRegressor(**best_params['CatBoostRegressor'], logging_level='Silent')
    models['XGBRegressor'] = XGBRegressor(**best_params['XGBRegressor'])
    models['RandomForestRegressor'] = RandomForestRegressor(**best_params['RandomForestRegressor'])
    models['LinearRegression'] = LinearRegression(**best_params['LinearRegression'])
    models['GradientBoostingRegressor'] = GradientBoostingRegressor(**best_params['GradientBoostingRegressor'])
    models['stacking'] = get_stacking(best_params)
    return models


def get_stacking(params):
    level0 = []
    level0.append(('CatBoostRegressor', CatBoostRegressor(**params['CatBoostRegressor'], logging_level='Silent')))
    level0.append(('RandomForestRegressor', RandomForestRegressor(**params['RandomForestRegressor'])))
    level0.append(('XGBRegressor', XGBRegressor(**params['XGBRegressor'])))
    level0.append(('LinearRegression', LinearRegression(**params['LinearRegression'])))
    level0.append(('GradientBoostingRegressor', GradientBoostingRegressor(**params['GradientBoostingRegressor'])))
     # define meta-learner model
    level1 = LinearRegression(**params['LinearRegression'])
    # define the stacking ensemble
    model = StackingRegressor(estimators=level0, final_estimator=level1, cv=5)
    return model

def find_best_params(models, X_train_selected, y_train):
    best_params = {}
    for name, model in models.items():
        print("Tuning", name)
        clf = GridSearchCV(model, params[name], cv=10, n_jobs=-1)
        clf.fit(X_train_selected, y_train)
        best_params[name] = clf.best_params_
        print("Best parameters:", best_params[name])
    return best_params


def evaluate_model(model, X_train_scaled, X_test_scaled, y_train, y_test):
    model_train = model.fit(X_train_scaled, y_train)
    cv = RepeatedKFold(n_splits=10, n_repeats=3, random_state=42)
    results_score = cross_val_score(model_train, X_test_scaled, y_test, scoring='neg_mean_absolute_error', cv=cv,n_jobs=-1)
    y_pred = model_train.predict(X_test_scaled)
    r2 = r2_score(y_test, y_pred)
    mae_score = mean_absolute_error(y_test, y_pred)
    return {
        'accuracy': {'mean': mean(results_score), 'std': std(results_score)},
        'r2_score': r2,
        'mae_score': mae_score,
        'results_score': results_score,
        'y_pred': y_pred,
        'model': model_train
    }


def data_prep(df,city_code,start_year=2005, end_year=2024, min_price=800000, max_price=10000000, yad2=False, madlan=False,
              scaler='StandardScaler', accuracy=False):
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['year'] = df['year'].astype(np.int32)
    df['size'] = df['size'].astype(np.int32)

    df = df[(df['year'] >= start_year) & (df['year'] < end_year)]
    df = df[(df['price'] > min_price) & (df['price'] < max_price)]
    df = df[(df['size'] < 400) & (df['size'] > 25)]
    df = df[df['build_year'] > 1910]

    current_year = datetime.now().year
    df['age'] = current_year - df['build_year']

    df = df.dropna(subset=['long', 'lat', 'rooms', 'floor', 'floors', 'helka_rank','size'])

    df['floors'] = df['floors'].astype(float).astype(np.int32)
    df['floor'] = df['floor'].astype(float).astype(np.int32)
    df['rooms'] = df['rooms'].astype(float).astype(int)


    df = df.reindex(columns=["rooms", "floor", "size", "price", "build_year", "floors",
                             "year", 'age', 'neighborhood_rank', 'street_rank', 'helka_rank', 'new'])

    y = df['price']
    X = df.drop('price', axis=1)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42)
    if scaler == 'StandardScaler':
        scaler = StandardScaler()
    else:
        scaler = MinMaxScaler()

    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    joblib.dump(scaler, f'C:/Users/yoavl/NextRoof/Algorithms/models/{city_code}_scaler.pkl')

    return X_train_scaled, X_test_scaled, y_train, y_test, X_train, X_test_scaled

def init_model(city_obj):
    status = {}
    city = city_obj[0]
    city_code = city_obj[1]
    try:
        df = read_from_nadlan_rank(city)
        print(df.shape)
        X_train_scaled,  X_test_scaled  ,y_train, y_test , X_train, X_test = data_prep(df,city_code)
        # best_params = find_best_params(models_list , X_train_scaled ,y_train )
        models = get_models_with_best_params(lean_params) # best_params  / lean_params

        scores = {}
        saved_models = {}
        for name, model in models.items():
            score = evaluate_model(model, X_train_scaled, X_test_scaled, y_train, y_test)
            scores[name] = score
            saved_models[name] = model

        joblib.dump(saved_models, f'C:/Users/yoavl/NextRoof/Algorithms/models/{city_code}_saved_models.pkl')

        plot_model_scores(scores)
        result_plot(scores)
        status['r2'] = scores['stacking']['r2_score']
        status['mae'] = scores['stacking']['mae_score']
        status['success'] = True
        print(f"city {city} , r2_score: {scores['stacking']['r2_score']} , mae_score: {scores['stacking']['mae_score']}")

    except Exception as e:
        error_message = f"{e}\n{traceback.format_exc()}"
        status['success'] = False
        status['error'] = error_message
    return status



def train_model_main(city_dict):
    status = {}
    for item in city_dict.items():
        data = init_model(city_obj=item)
        status[item[0]] = data
