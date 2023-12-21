import sys
sys.path.append('C:/Users/yoavl/NextRoof/')
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

def check_for_match(df_nadlan, parcel):
    parcel = int(parcel)
    parcel_offsets = [parcel - 1, parcel + 1, parcel + 2, parcel - 2, parcel + 3, parcel - 3, parcel + 4, parcel - 4]
    for parcel_offset in parcel_offsets:
        try:
            match = df_nadlan.loc[(df_nadlan['gush_helka'] == str(parcel_offset)), 'helka_rank']
        except:
            match = df_nadlan.loc[(df_nadlan['Gush_Helka'] == str(parcel_offset)), 'Helka_rank']
        if not match.empty:
            return match
    return None


def strip_columns(df):
    string_cols = df.select_dtypes(include=['object']).columns
    df[string_cols] = df[string_cols].apply(lambda col: col.str.strip())
    return df
