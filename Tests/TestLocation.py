import unittest
import pandas as pd
from unittest.mock import patch, Mock
import sys
sys.path.append('C:/Users/yoavl/NextRoof/utils')
from utils.location import *

class TestFunctions(unittest.TestCase):


    def setUp(self):
        print("SetUP")

    def tearDown(self):
        print("tearDown")
    def test_get_gush_chelka_api(self):
        pass

    def test_convert_coordinates(self):
        lat_long_list = [(32, 35)]
        result = convert_coordinates(lat_long_list)
        self.assertEqual(result, [(200131, 656329)])

    def test_get_long_lat_tuples(self):
        sample_data = {
            'Lat': [32.0, np.nan,23,4,-3],
            'Long': [35.0, 4,23,4,5],
        }
        df = pd.DataFrame(sample_data)
        result = get_long_lat_tuples(df)
        print(result)
        self.assertEqual(result, [(32.0,35.0) , (0.0,0.0) , (23,23 ) ,(4,4) ,(0.0,0.0 )])

    @patch('utils.location.pd.read_csv')
    def test_find_avg_cords_by_street(self, mock_read_csv):
        # Mocked DataFrame
        data = {
            'Long': [34, 35, 35, 36],
            'Lat': [32, 31, 31, 33],
            'Street': ['A', 'B', 'B', 'C']
        }
        mock_df = pd.DataFrame(data)

        mock_read_csv.return_value = mock_df
        result = find_avg_cords_by_street()

        expected = {
            'A': (34, 32),
            'B': (35, 31),
            'C': (36, 33)
        }
        self.assertDictEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
