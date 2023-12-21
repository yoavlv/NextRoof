# import datetime
# import numpy as np
# from sql.sql_reader import read_clean_data_table
# from monitor import monitor_data
# import psycopg2


import sys

print("Python vehhhrsion:", sys.version)

def create_street_and_neighborhood_rank(df, column):
    '''
    This function need to get -all- the city data to create new ranking for the city
    '''
    rank_dict = {}
    years = df['Year'].unique()

    for year in years:
        df_by_year = df[df.loc[:, 'Year'] == year]

        rank_group_price = df_by_year.groupby(column)['Price'].sum().reset_index()
        rank_group_size = df_by_year.groupby(column)['Size'].sum().reset_index()

        rank_group_size.loc[:, 'Size'] = rank_group_size.loc[:, 'Size'].astype(np.int64)
        rank_group_price.loc[:, 'Price'] = rank_group_price.loc[:, 'Price'].astype(np.int64)

        rank_group = rank_group_price.merge(rank_group_size, on=column)
        rank_group.loc[:, 'Rank'] = rank_group.loc[:, 'Price'] / rank_group.loc[:, 'Size']

        rank_group.loc[:, 'Rank'] = rank_group.loc[:, 'Rank'].astype(np.int32)
        rank_group = rank_group.sort_values(by='Rank', ascending=False)

        rank_dict[year] = rank_group

    new_column_name = column + '_rank'

    df[new_column_name] = np.nan

    for index, row in df.iterrows():
        year = row['Year']
        street = row[column]
        temp_df = rank_dict[year]

        match = temp_df[temp_df[column] == street]['Rank']
        df.loc[index, new_column_name] = match.iloc[0]

    df[new_column_name] = df[new_column_name].astype(np.int32)

    return df

def change_by_years(df):
    years = df['Year'].unique()
    today = df[df['Year'] == datetime.datetime.now().year]
    avg_today = today['Price'].sum() / today['Size'].sum()
    change = {}
    for year in years:
        df_year = df[df['Year'] == year]

        avg_year = df_year['Price'].sum() / df_year['Size'].sum()

        change[year] = avg_today / avg_year
    return change




def create_parcel_rank(df):
    '''
    This function need to get -all- the city data to create new ranking for the city
    '''
    #     df = df.drop_duplicates(subset=['Price', 'Date'])
    # Note : if the max year in the df != today year the calc will not work...

    df = df[(df['Year'] <= datetime.datetime.now().year)]

    parcel_rank = {}

    df.loc[:, 'Gush_Helka'] = df['Gush'] + '' + df['Helka']
    df.loc[:, 'Helka_rank'] = np.nan
    gush_helka = df['Gush_Helka'].unique()

    change_p = change_by_years(df)

    for gh in gush_helka:
        df_gush_helka = df[df['Gush_Helka'] == gh]
        max_year = df_gush_helka.loc[:, 'Build_year'].max()
        result_df = df_gush_helka[(df_gush_helka['Year'] >= max_year) & (df_gush_helka['Year'] < 2024)].copy()
        result_df['P_price'] = result_df['Year'].apply(lambda x: change_p[x]) * result_df['Price']
        result_df['P_price'] = result_df['P_price'].astype(np.int32)

        denominator = result_df['Size'].sum()
        if denominator != 0:
            rank = result_df['P_price'].sum() / denominator
        else:
            rank = np.nan
        parcel_rank[gh] = rank

    df.loc[:, 'Helka_rank'] = df.loc[:, 'Gush_Helka'].map(parcel_rank)
    df = df.dropna(subset=['Helka_rank'])
    df.loc[:, 'Helka_rank'] = df.loc[:, 'Helka_rank'].astype(np.int64)
    df.drop(columns=['Gush_Helka'], inplace=True)

    return df


# def parcel_rank_general(df):
#     count = 0
#     df['Helka_rank'] = np.nan
#     df = df.dropna(subset=['Gush', 'Helka']).reset_index(drop=True)
#     df['Gush'] = df['Gush'].astype(np.int32)
#     df['Helka'] = df['Helka'].astype(np.int32)
#     df_nadlan = table_to_df('nadlan',['gush', 'helka', 'helka_rank'])
#     df_nadlan['gush_helka'] = df_nadlan['gush'].astype(str) + df_nadlan['helka'].astype(str)
#     column = 'Gush_Helka'
#     df[column] = df['Gush'].astype(str) + df['Helka'].astype(str)
#     df[column] = df[column].str.replace('.', '', regex=False)
#
#     for index, row in df.iterrows():
#         gush_helka = row['Gush_Helka']
#         match = df_nadlan.loc[(df_nadlan['gush_helka'] == gush_helka), 'helka_rank']
#
#         if match.empty:
#             match = check_for_match(df_nadlan, gush_helka)
#
#         if not match.empty:
#             df.at[index, 'Helka_rank'] = match.values[0]
#
#         if match.empty:
#             count += 1
#
#         df['Helka_rank'] = df['Helka_rank'].fillna(df_nadlan['helka_rank'].mean()).astype(np.int32)
#
#     df = df.drop(columns='Gush_Helka')
#
#     return df
#
#
# def neighborhood_and_street_rank_general(df, column):
#     df = df.dropna(subset=['Neighborhood', 'Street']).reset_index(drop=True)
#
#     columns_needed = [
#         'year', 'neighborhood',
#         'street', 'gush', 'street_rank',
#         'neighborhood_rank', 'gush_rank'
#     ]
#     df_nadlan = table_to_df('nadlan', columns_needed)
#
#     new_column_name = column + '_rank'
#     df[new_column_name] = np.nan
#
#     for index, row in df.iterrows():
#         year = int(row['Year'])
#         item_to_rank = row[column]
#         try:
#             item_to_rank = int(row[column])
#         except:
#             pass
#
#         filtered = df_nadlan[df_nadlan[column.lower()] == item_to_rank]
#         sorted_filtered = filtered.sort_values(by='year', ascending=False)
#
#         if not sorted_filtered.empty:
#             df.at[index, new_column_name] = sorted_filtered[new_column_name.lower()].iloc[0]
#         else:
#             print(f"No match found for Index: {index}, Year: {year}, item_to_rank: |{item_to_rank}|, {column}")
#
#
#     col = new_column_name.lower()
#     df_nadlan[col] = df_nadlan[col].astype(np.int32)
#     mean = df_nadlan[col].mean()
#
#     r = df[df[new_column_name].isna()]
#     f = df[df[new_column_name].notna()]
#
#     print(f"col {col}\ntotal na: {r.shape}\ntotal not na: {f.shape}")
#
#     df[new_column_name] = df[new_column_name].fillna(mean)
#     df[new_column_name] = df[new_column_name].astype(np.int32)
#
#     return df

def run_nadlan_ranking():
    try:
        df = read_clean_data_table()
        df =create_street_and_neighborhood_rank(df, 'street')
        df =create_street_and_neighborhood_rank(df, 'neighborhood')
        df =create_street_and_neighborhood_rank(df, 'gush')
        df = create_parcel_rank(df)

        # print(f"final nadlan shape : {df.shape}")
        # monitor_data['Clean']['nadlan']['Total_size'] = df.shape
        # monitor_data['Clean']['nadlan']['status'] = 'Success'

    except Exception as e:
        # error_message = f"{e}\n{traceback.format_exc()}"
        print(f"error :{e}")
        # monitor_data['Clean']['nadlan']['status'] = 'Fail'
        # monitor_data['Clean']['nadlan']['error'] = e

