from .sql_save_nadlan import add_new_deals_nadlan_rank
from utils.utils_sql import DatabaseManager
from .sql_reader_nadlan import read_from_nadlan_clean
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import traceback

CURRENT_DATE = datetime.datetime.now()
THREE_MONTHS_EARLIER = CURRENT_DATE - relativedelta(months=3)


def create_rank_for_area(df, column):
    '''
    This function need to get -all- the city data to create new ranking for the city
    '''
    rank_dict = {}
    years = df['year'].unique()
    column = str(column)
    for year in years:
        df_by_year = df[df.loc[:, 'year'] == year]

        rank_group_price = df_by_year.groupby(column)['price'].sum().reset_index()
        rank_group_size = df_by_year.groupby(column)['size'].sum().reset_index()

        rank_group_size.loc[:, 'size'] = rank_group_size.loc[:, 'size'].astype(np.int64)
        rank_group_price.loc[:, 'price'] = rank_group_price.loc[:, 'price'].astype(np.int64)

        rank_group = rank_group_price.merge(rank_group_size, on=column)
        rank_group.loc[:, 'rank'] = rank_group.loc[:, 'price'] / rank_group.loc[:, 'size']

        rank_group.loc[:, 'rank'] = rank_group.loc[:, 'rank'].astype(np.int32)
        rank_group = rank_group.sort_values(by='rank', ascending=False)

        rank_dict[year] = rank_group

    new_column_name = column + '_rank'

    if column == 'street_id':
        new_column_name = 'street_rank'

    df[new_column_name] = np.nan

    for index, row in df.iterrows():
        year = row['year']
        street_id = row[column]
        temp_df = rank_dict[year]

        match = temp_df[temp_df[column] == street_id]['rank']
        if not match.empty:
            df.loc[index, new_column_name] = match.iloc[0]
        else:
            df.loc[index, new_column_name] = np.nan
    df = df.dropna(subset=[new_column_name])
    df.loc[:, new_column_name] = df.loc[:, new_column_name].astype(np.int32)
    return df


def change_by_years(df):
    years = df['year'].unique()
    today = df[df['year'] == THREE_MONTHS_EARLIER.year]
    avg_today = today['price'].sum() / today['size'].sum()
    change = {}
    for year in years:
        df_year = df[df['year'] == year]
        avg_year = df_year['price'].sum() / df_year['size'].sum()
        change[year] = avg_today / avg_year

    return change


def create_parcel_rank(df):
    '''
    This function need to get -all- the city data to create new ranking for the city
    '''
    #     df = df.drop_duplicates(subset=['Price', 'Date'])
    # Note : if the max year in the df != today year the calc will not work...
    df = df.copy()
    df = df[(df['year'] <= THREE_MONTHS_EARLIER.year)]

    parcel_rank = {}

    df.loc[:,'gush_helka'] = df.apply(lambda row: str(row['gush']) + str(row['helka']), axis=1)

    df.loc[:, 'helka_rank'] = None
    gush_helka = df['gush_helka'].unique()

    change_p = change_by_years(df)

    for gh in gush_helka:
        df_gush_helka = df[df['gush_helka'] == gh]
        max_year = df_gush_helka.loc[:, 'build_year'].max()
        result_df = df_gush_helka[(df_gush_helka['year'] >= max_year) & (df_gush_helka['year'] < 2024)].copy()

        result_df['p_price'] = result_df['year'].apply(lambda x: change_p[x]) * result_df['price']
        result_df.replace([np.inf, -np.inf], np.nan, inplace=True)
        result_df.dropna(subset=['p_price'], inplace=True)

        result_df['p_price'] = result_df['p_price'].astype(np.int32)

        denominator = result_df['size'].sum()
        if denominator != 0:
            rank = result_df['p_price'].sum() / denominator
        else:
            rank = np.nan
        parcel_rank[gh] = rank

    df.loc[:, 'helka_rank'] = df.loc[:, 'gush_helka'].map(parcel_rank)
    df = df.dropna(subset=['helka_rank'])
    df.loc[:, 'helka_rank'] = df.loc[:, 'helka_rank'].astype(np.int64)
    df.drop(columns=['gush_helka'], inplace=True)

    return df

def main_nadlan_rank(city, city_id):
    nadlan_rank_status = {}
    try:
        df = read_from_nadlan_clean(city_id)
        df = create_rank_for_area(df, 'gush')
        df = create_rank_for_area(df, 'street_id')
        df = create_parcel_rank(df)
        db_manager = DatabaseManager('nextroof_db', 'localhost', 'nadlan_rank')
        success, new_rows, updated_rows = db_manager.insert_dataframe(df, 'key')

        # data2 = add_new_deals_nadlan_rank(df, '13.50.98.191')
        nadlan_rank_status['success'] = success
        nadlan_rank_status['new_rows'] = new_rows
        nadlan_rank_status['updated_rows'] = updated_rows

    except Exception as e:
        error_message = f"{city}:{e}\n{traceback.format_exc()}"
        nadlan_rank_status['success'] = False
        nadlan_rank_status['error'] = error_message

    print("main_nadlan_rank_FINISH")
    return nadlan_rank_status

