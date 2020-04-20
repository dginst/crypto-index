import unittest
import sys
import pandas as pd
sys.path.append('c:\\Projects\\dginst\\crypto-index\\')
from utils import data_download

class TestDownload(unittest.TestCase):

    def test_ECB_downloader(self):
        Start_Period = '2020-01-02'
        key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']
        header = ['CURRENCY', 'CURRENCY_DENOM', 'OBS_VALUE', 'TIME_PERIOD']
        test_matrix = [['USD', 'EUR', 1.1193, '1577923200'],
            ['GBP', 'EUR', 0.8482799999999999, '1577923200'],
            ['CAD', 'EUR', 1.4549, '1577923200'],
            ['JPY', 'EUR', 121.75, '1577923200']]
        test_matrix = pd.DataFrame(test_matrix, columns = header)
        ECB_data = data_download.ECB_rates_extractor(key_curr_vector, Start_Period, End_Period = None)
        self.assertTrue(ECB_data.equals(test_matrix)) 


    def test_CW_downloader(self):



if __name__ == "__main__":
    # execute only if run as a script
    unittest.main()
