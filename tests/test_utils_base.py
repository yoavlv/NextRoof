import pytest
from utils.base import find_most_similar_word, add_id_columns
import pandas as pd
from unittest.mock import patch

def test_find_most_similar_word():
    word_list = ["tel-aviv", "tel-aviv yafo", "ramat aviv",'tel aviv']
    target_word = "tel-aviv"
    assert find_most_similar_word(word_list, target_word) == "tel-aviv"

    target_word_no_match = "rishon"
    assert find_most_similar_word(word_list, target_word_no_match) is None
    word_list = ['apple', 'banana', 'cherry']
    assert find_most_similar_word(word_list, None) is None
    assert find_most_similar_word(word_list, b'appl') == 'apple'
    assert find_most_similar_word(word_list, 'banan') == 'banana'
    assert find_most_similar_word(word_list, 'grape') is None


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "city_id": [1],
        "street": ["Main St"],
        "other_column": ["value"]
    })



@pytest.fixture
def mock_read_from_streets_table():
    return pd.DataFrame({
        'street': ['Main Street', 'Second Street', 'Third Street'],
        'street_id': [1, 2, 3]
    })

@pytest.fixture
def mock_read_from_cities_table():
    return pd.DataFrame({
        'city': ['CityA', 'CityB', 'CityC'],
        'city_id': [10, 20, 30]
    })

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'name': ['  some name  ', 'another name'],
        'value': [1, 2]
    })
@patch('nadlan.sql_reader_nadlan.read_from_streets_table')
@patch('nadlan.sql_reader_nadlan.read_from_cities_table')
def test_add_id_columns(mock_read_from_cities, mock_read_from_streets, mock_read_from_streets_table,
                        mock_read_from_cities_table):
    df = pd.DataFrame({
        'city_id': [1],
        'street': ['Main Strt'],  # Intentional typo to test matching
        'value': [100]
    })
    mock_read_from_streets.return_value = mock_read_from_streets_table
    mock_read_from_cities.return_value = mock_read_from_cities_table

    # Test for 'street'
    result_df = add_id_columns(df, 'street_id', 'street')
    assert 'street_id' in result_df.columns
