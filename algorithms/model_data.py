from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from catboost import CatBoostRegressor
from xgboost import XGBRegressor


models_list = {
    'CatBoostRegressor': CatBoostRegressor(logging_level='Silent'),
    'XGBRegressor': XGBRegressor(),
    'RandomForestRegressor': RandomForestRegressor(),
    'LinearRegression': LinearRegression(),
    'GradientBoostingRegressor': GradientBoostingRegressor(),
}

best_params = {
    'RandomForestRegressor': {'bootstrap': True, 'max_depth': 15, 'max_features': 'sqrt', 'min_samples_leaf': 2, 'min_samples_split': 2, 'n_estimators': 200},
    'CatBoostRegressor': {'depth': 10, 'iterations': 1500, 'l2_leaf_reg': 9, 'learning_rate': 0.05},
    'XGBRegressor': {'learning_rate': 0.07, 'max_depth': 5, 'n_estimators': 150},
    'LinearRegression': {'fit_intercept': True, 'positive': True},
    'GradientBoostingRegressor': {'n_estimators': 100, 'learning_rate': 0.1, 'max_depth': 3},
}

params = {
    'CatBoostRegressor': {'iterations': [1000, 1200],'learning_rate': [0.01,0.05],'depth': [8,10],'l2_leaf_reg': [3,9]},
    'RandomForestRegressor': {'bootstrap': [True, False],'max_depth': [8, 10],'max_features': ['log2', 'sqrt'],'min_samples_leaf': [1, 2, 4],'n_estimators': [100, 150, 200],'min_samples_split': [2, 5, 10]},
    'XGBRegressor': {'learning_rate': [0.07, 0.1, 0.15],'max_depth': [5, 7, 10],'n_estimators': [150, 200, 250],'max_leaves':[2,3,4]},
    'LinearRegression': {'fit_intercept': [True, False], 'positive': [True, False]},
    'GradientBoostingRegressor': {'n_estimators': [100,200,300], 'learning_rate': [0.1,0.2,0.3], 'max_depth': [3,5,7]},
    'LGBMRegressor': {'boosting_type': ['gbdt'], 'num_leaves': [10, 20, 30], 'learning_rate': [0.1, 0.2, 0.3], 'n_estimators': [100, 200, 300]}
}

lean_params = {
    'RandomForestRegressor': {'bootstrap': True, 'max_depth': 3, 'max_features': 'sqrt', 'min_samples_leaf': 2, 'min_samples_split': 2, 'n_estimators': 100},
    'CatBoostRegressor': {'depth': 3, 'iterations': 500, 'l2_leaf_reg': 3, 'learning_rate': 0.05},
    'XGBRegressor': {'learning_rate': 0.07, 'max_depth': 3, 'n_estimators': 50},
    'LinearRegression': {'fit_intercept': True, 'positive': True},
    'GradientBoostingRegressor': {'n_estimators': 50, 'learning_rate': 0.1, 'max_depth': 3},
}