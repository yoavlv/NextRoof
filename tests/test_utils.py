from nadlan.nadlan_utils import split_address
import pytest

def valid_input_data():
    valid_data = [
        "רחוב הרצל 123, תל אביב",
        "בן גוריון 123, אשדוד",
        "בן 3 123, אשדוד",
        "בן גורי'ון 123, אשדוד",
        "רחוב הרצל 123, תל אביב",
        "בן גוריון 5, אשדוד",

    ]
    return valid_data

def invalid_input_data():
    invalid_data = [
        'Invalid Address Format 1',
        'Invalid Address Format 2',
        'Invalid Address Format 3',
        'Invalid Address Format 4',
        'Invalid Address Format 5',
        'Invalid Address Format 6',
        'Invalid Address Format 7',
        'Invalid Address Format 8',
        'Invalid Address Format 9',
        'Invalid Address Format 10'
    ]
    return invalid_data

def test_split_address_valid(address):
    address = "רחוב הרצל 123, תל אביב"
    result = split_address(address)
    assert result == {'street': 'רחוב הרצל', 'home_number': '123', 'city': 'תל אביב'}

@pytest.mark.parametrize('address', valid_input_data())
def test_split_address_valid(address):
    result = split_address(address)
    assert len(result) == 3, f"The result should have three elements."
    assert isinstance(result, dict), "The result should be a dictionary."
    assert isinstance(result['street'], str), "The 'street' should be a string."
    assert isinstance(int(result['home_number']), int), "The 'home_number' should be an integer."
    assert isinstance(result['city'], str), "The 'city' should be a string."

def test_split_address_valid_len():
    address = "רחוב הרצל 123, תל אביב"
    result = split_address(address)
    assert result == {'street': 'רחוב הרצל', 'home_number': '123', 'city': 'תל אביב'}
@pytest.mark.parametrize('address',invalid_input_data())
def test_split_address_invalid(address):
    with pytest.raises(ValueError, match=r"Invalid address format:.*"):
        split_address(address)

def test_split_address_exception():
    address = "רחוב הרצל, תל אביב"
    with pytest.raises(Exception, match=r"Error parsing address:.*"):
        split_address(address)
