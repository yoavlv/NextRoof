import pytest
import pandas as pd
from nadlan.nadlan_clean import rename_cols_update_data_types , pre_process ,floor_to_numeric , floors
import numpy as np



@pytest.fixture
def input_data():
    # try:
    #     from ..dev import get_db_engine
    #     engine = get_db_engine(db_name='nadlan_db',)
    #     query = "SELECT * FROM nadlan_raw LIMIT 100"
    #     df = pd.read_sql_query(query, engine)
    # except:
    df = pd.read_csv('nadlan_data.csv',index_col=0)
    return df

def test_pre_process(input_data):
    data = {
        'assetroomnum': ['1', '2.5', np.nan],
        'dealnature': ['100', '200.5', ''],
        'newprojecttext': ['1', '', '3.5'],
        'buildingyear': ['1990', '2000', np.nan],
        'yearbuilt': ['1991', '', '2001'],
        'buildingfloors': ['5', '10', ''],
        'type': ['1', '2', '5']
    }
    df = pd.DataFrame(data)
    df = pre_process(df)

    for col in df.columns:
        assert df[col].dtype == float, f"Column {col} is not of type float."


def test_rename_cols_update_data_types(input_data):
    result_df = rename_cols_update_data_types(input_data)
    expected_columns = ['date', 'gush', 'type', 'rooms', 'floor', 'size', 'price', 'new',
       'build_year', 'rebuilt', 'floors', 'key', 'created_at', 'city',
       'home_number', 'street', 'year', 'helka', 'tat']

    # Check that all expected columns are present
    assert all([col in result_df.columns for col in expected_columns]), "Not all expected columns are present."

    # # Check data types and transformations for specific columns
    assert result_df['home_number'].dtype == 'int32', "Home number is not of type int32."
    assert result_df['price'].dtype == 'int32', "Price is not of type int32."
    assert all(result_df['price'] > 10000), "Price col isn't valid."

    assert result_df['date'].dtype == 'datetime64[ns]', "Date is not of type datetime64[ns]."
    assert result_df['rooms'].dtype == float, "Rooms column is not of type float."
    assert result_df['build_year'].dtype == np.int32, "Build year is not of type int32."
    assert result_df['rebuilt'].dtype == np.int32, "Rebuilt year is not of type int32."

    # Verify that unwanted types are removed
    unwanted_types = [
        "nan", "מיני פנטהאוז", "מגורים", "בית בודד", "דופלקס", "קוטג' חד משפחתי", "קוטג' דו משפחתי",
        'מלונאות', 'חנות', 'קרקע למגורים', 'קבוצת רכישה - קרקע מגורים', 'None', 'אופציה',
        'קבוצת רכישה - קרקע מסחרי', 'חניה', 'מסחרי + מגורים', 'דירת נופש', 'דיור מוגן', 'קומבינציה', 'מבנים חקלאיים',
        'תעשיה', 'מסחרי + משרדים', 'בניני ציבור', 'חלוקה/יחוד דירות', 'מחסנים', 'אחר', 'בית אבות', 'עסק',
        "קוטג' טורי", 'ניוד זכויות בניה', 'משרד', 'ללא תיכנון', 'מלונאות ונופש', 'משרדים + מגורים', 'מלאכה'
    ]
    assert not any(result_df['type'].isin(unwanted_types)), "Unwanted types were not removed."

    # Verify date parsing
    assert result_df['date'].dtype == np.dtype('<M8[ns]'), "Date column is not of type datetime64."
    # Verify city, street, and home_number extraction
    assert all(result_df['city'].notnull()), "City extraction failed for some rows."
    assert all(result_df['street'].notnull()), "Street extraction failed for some rows."
    assert all(result_df['home_number'] > 0), "Home number extraction or conversion failed."
    # Verify NaN handling for new, floors, and other fields where NaNs are expected to be filled
    assert all(result_df['new'].notnull()), "New column contains NaN values."


def test_valid_floor_conversion(input_data):
    result_df = rename_cols_update_data_types(input_data)
    result_df = floor_to_numeric(result_df, floors)
    assert result_df['floor'].dtype == np.int32 , "Floor is not of type int32."
    assert all(result_df['floor'].notnull()), "Floor column contains NaN values."

