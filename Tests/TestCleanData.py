import unittest
import pandas as pd
import numpy as np
from datetime import datetime
from dataProcess import CleanData

class TestCleanData(unittest.TestCase):

    def setUp(self):
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
            'Lat': [2, 4, 5, 23, 3],
            'Long':  [2, 4, 5, 23, 3],
            'Build_year': [1900, 2000, 2023, 2050, 2019],
            'Gush': [1, 1, 2, 3, 4],
            'Helka': [1, 2, 4, 2, 2],

        })
        self.clean_data_instance = CleanData(self.sample_data, name='madlan')
    def tearDown(self):
        print(f"\nTearing down after the test: {self._testMethodName}...\n")
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
