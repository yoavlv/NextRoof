import unittest
import pandas as pd
from unittest.mock import patch, Mock
import sys
sys.path.append('C:/Users/yoavl/NextRoof/utils')
from utils.base import *

class TestFunctions(unittest.TestCase):


    def setUp(self):
        print("SetUP")

    def tearDown(self):
        print("tearDown")
    def test_get_gush_chelka_api(self):
        pass


    @patch('utils.base.pd.read_csv')
    def test_create_building_year_dict(self, mock_read_csv):
        data = {
            'Floors': [1, 2, 3, 4, 5],
            'Street': ['A', 'B', 'B', 'C', 'A'],
            'Home_number': [1, 2, 2, 3, 1],
            'Build_year': [2000, 2001, 2002, 2003, 1999]
        }
        mock_df = pd.DataFrame(data)
        mock_read_csv.return_value = mock_df

        result = create_building_year_dict()

        expected = {
            ('A', 1): 2000,
            ('B', 2): 2002,
            ('C', 3): 2003
        }
        self.assertDictEqual(result, expected)


    def test_update_neighborhood_street_token(self):
        data = {
            'Street': ["'כרמייה  ", 'SomeStreet'],
            'Neighborhood': ["נווה אביבים", "לב תל אביב, לב העיר צפון"]
        }
        input_df = pd.DataFrame(data)

        result_df = update_neighborhood_street_token(input_df)

        # Expected output data
        expected_data = {
            'Street': ["כרמיה", 'SomeStreet'],
            'Neighborhood': ["נווה אביבים וסביבתה", "הצפון הישן החלק הצפוני"]
        }
        expected_df = pd.DataFrame(expected_data)

        pd.testing.assert_frame_equal(result_df, expected_df)


    def test_check_for_match(self):
        data = {
            'Gush_Helka': ["1", "3", "5", "9"],
            'Helka_rank': ["10", "20", "30", "40"]
        }
        df_nadlan = pd.DataFrame(data)

        # Test for a parcel that should match with "3"
        result = check_for_match(df_nadlan, "2")
        self.assertEqual(result.values[0], "10")  # This should match with "3" and hence give us "B"

        # Test for a parcel that should not find a match
        result = check_for_match(df_nadlan, "1")
        self.assertEqual(result.values[0], "20")  # This should match with "3" and hence give us "B"

        # Test for a parcel that should match with "1"
        result = check_for_match(df_nadlan, "0")
        self.assertEqual(result.values[0], "10")

        # Test for a parcel that should match with "9"
        result = check_for_match(df_nadlan, "5")
        self.assertEqual(result.values[0], "20")

if __name__ == "__main__":
    unittest.main()
