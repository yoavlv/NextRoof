
import numpy as np

def street_and_neighborhood_rank(df, column):
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
    today = df[df['Year'] == 2023]
    avg_today = today['Price'].sum() / today['Size'].sum()
    change = {}
    for year in years:
        df_year = df[df['Year'] == year]

        avg_year = df_year['Price'].sum() / df_year['Size'].sum()

        change[year] = avg_today / avg_year
    return change


def parcel_rank(df):
    #     df = df.drop_duplicates(subset=['Price', 'Date'])
    df = df[(df['Year'] < 2024)]

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