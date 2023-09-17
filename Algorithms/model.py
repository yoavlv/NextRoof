from numpy import mean ,std
import numpy as np
from sklearn.model_selection import cross_val_score , cross_validate ,RepeatedKFold ,GridSearchCV
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.pipeline import Pipeline
from sklearn.ensemble import StackingRegressor ,RandomForestRegressor , GradientBoostingRegressor
from sklearn.metrics import r2_score ,mean_absolute_error
from sklearn.neural_network import MLPRegressor
from catboost import CatBoostRegressor
from xgboost import XGBRegressor
import pandas as pd
import csv
import joblib
from Algorithms.model_data import params, best_params, models_list, lean_params
from Algorithms.setUp import data_prep
from Algorithms.model_plots import result_plot , plot_model_scores
from monitor import monitor_data

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
def init_model():
    try:
        X_train_scaled,  X_test_scaled  ,y_train, y_test , X_train, X_test = data_prep(start_year = 2003 , min_price = 1200000 ,max_price =7000000 )
        # best_params = find_best_params(models_list , X_train_scaled ,y_train )
        models = get_models_with_best_params(lean_params) # best_params  / lean_params

        # evaluate the models and store results
        scores = {}
        saved_models = {}
        for name, model in models.items():
            score = evaluate_model(model, X_train_scaled, X_test_scaled, y_train, y_test)
            scores[name] = score
            saved_models[name] = model

        joblib.dump(saved_models, 'saved_models.pkl')

        plot_model_scores(scores)
        result_plot(scores)

        monitor_data['Clean']['nadlan']['r2'] = scores['stacking']['r2_score']
        monitor_data['Clean']['nadlan']['mae'] = scores['stacking']['mae_score']
        monitor_data['Clean']['nadlan']['status'] = 'Success'

    except Exception as e:
        monitor_data['Clean']['nadlan']['error'] = e
        monitor_data['Clean']['nadlan']['status'] = 'Fail'
