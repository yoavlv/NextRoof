import unittest
import pandas as pd
import numpy as np
from datetime import datetime
from dataProcess import CleanData
from unittest.mock import patch, Mock
import sys
sys.path.append('C:/Users/yoavl/NextRoof')
class TestCleanData(unittest.TestCase):

    def setUp(self ):
        self.name_value = None
        if self._testMethodName == 'test_madlanSetUp':
            self.name_value = 'madlan'

        self.sample_data = pd.DataFrame({
            'BuildingClass': ['flat', 'roofflat', 'studio', 'villa', 'flat'],
            'Street': ['Main St', 'Broadway', 'Park Ave', None, 'Market St'],
            'Home_number': [1, 2, 3, 4, 5],
            'Text': ['Lorem', 'penthhouse amazing', 'Nice', 'Lorem', 'מפתח' ],
            'Price': ['₪5,000,000', '7000000', '900,000', '₪6,5,0,0', '₪555,00'],
            'Condition': ['New', 1, 3, '2', np.nan],
            'Neighborhood': ['נווה שרת', 'רמת החייל', 'צהלה', 'פלורנטין', 'נווה שרת'],
            'Floor': [-1, 4,0, 25, 3],
            'Floors': [2, 4, 5, 23, 3],
            'Size': [55,105,42, 67, 90],
            'Lat': [2, 4, 5, 23, 3],
            'Long':  [2, 4, 5, 23, 3],
            'Build_year': [1900, 2000, 2023, 2050, 2019],
            'Gush': [1, 1, 2, 3, 3],
            'Helka': [1, 2, 4, 2, 7],

        })
        self.clean_data_instance = CleanData(self.sample_data, name=self.name_value, test=True)
    def tearDown(self):
        print(f"\nTearing down after the test: {self._testMethodName}...\n")


    @patch('dataProcess.df_helper')
    def test_fix_floors(self, mock_df_helper):
        # Mocked dataframe for df_helper function
        nadlan_mock = pd.DataFrame({
            'Street': ['Main St', 'Main St', 'Broadway', 'Park Ave', 'Broadway'],
            'Home_number': [1, 1, 2, 3, 2],
            'Floors': [2, 3, 4, 5, 4]
        })

        mock_df_helper.return_value = nadlan_mock

        # Note: You'd need to determine what the expected dataframe looks like after fix_floors.
        # Here, I'm assuming that the Floors column gets updated to certain values as per your logic.
        # Replace this with what you expect the result to be.
        expected_df = self.sample_data.copy()
        expected_df['Floors'] = [3, 4, 5, 25, 3]  # Example of expected updated floors

        # Call the fix_floors method
        self.clean_data_instance.fix_floors()
        expected_df['Floors'] = expected_df['Floors'].astype(float)

        # Compare the dataframe from the instance with the expected dataframe
        pd.testing.assert_frame_equal(self.clean_data_instance.df, expected_df)

    @patch('dataProcess.check_for_match')
    @patch('dataProcess.df_helper')
    def test_parcel_rank(self, mock_df_helper , mock_check_for_match):
        mock_df = pd.DataFrame({
            'Neighborhood': ['n1','n1','n2','n3','n3','n3'],
            'Gush': ['1', '1', '2', '3', '3','3'],
            'Helka': ['1', '2', '2', '2', '6','3'],
            'Helka_rank': [100, 110, 300, 500, 100, 200],
        })

        parcel_mean = int(mock_df['Helka_rank'].mean())
        parcel_rank_result = [100,110,parcel_mean,500,parcel_mean]
        mock_df_helper.return_value = mock_df
        mock_check_for_match.return_value = pd.DataFrame()
        self.clean_data_instance.parcel_rank()
        helka_rank = list(self.clean_data_instance.df['Helka_rank'])
        self.assertEqual(parcel_rank_result, helka_rank)


    def test_setUp_cols(self):
        # The expected columns after running setUp_cols
        self.clean_data_instance.setUp_cols()
        resulting_cols = set(self.clean_data_instance.df.columns)
        for col in ['Gush', 'Helka', 'Tat']:
            self.assertTrue(col in resulting_cols)

    def test_update_asset_conditon(self):
        start_shape = self.clean_data_instance.shape
        self.clean_data_instance.update_asset_conditon()
        unique_asset_condition = self.clean_data_instance.df['New'].unique()
        self.assertTrue(set(unique_asset_condition).issubset([0,1]))
        end_shape = self.clean_data_instance.shape
        self.assertEqual(start_shape,end_shape)


    def test_convert_price_str_to_int(self):
        self.clean_data_instance.convert_price_str_to_int()
        # Check type of Price column
        self.assertTrue(np.issubdtype(self.clean_data_instance.df['Price'].dtype, int))

    def test_add_year(self):
        self.clean_data_instance.add_year()
        current_year = datetime.now().year
        # Check if 'Year' column has been added and all values are the current year
        self.assertTrue('Year' in self.clean_data_instance.df.columns)
        self.assertTrue((self.clean_data_instance.df['Year'] == current_year).all())

    # Continue adding tests for the remaining methods...
    def test_madlanSetUp(self):
        self.clean_data_instance.madlanSetUp()
        unique_asset_types = self.clean_data_instance.df['Asset_type'].unique()
        # Checking if only allowed types are present in the dataframe after filtering
        self.assertTrue(set(unique_asset_types).issubset(['flat', 'gardenapartment', 'roofflat', 'building', 'studio']))

if __name__ == "__main__":
    unittest.main()
