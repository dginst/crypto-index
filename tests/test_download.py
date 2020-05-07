import unittest

import pandas as pd

from cryptoindex.data_download import ECB_rates_extractor


class TestDownload(unittest.TestCase):


    def test_ECB_downloader(self):
        Start_Period = '2020-01-02'
        key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']
        header = ['CURRENCY', 'CURRENCY_DENOM', 'OBS_VALUE', 'TIME_PERIOD']
        # timestamp since epoch in seconds
        Start_Period_TS = '1577923200'
        test_matrix = [
            ['USD', 'EUR', 1.1193, Start_Period_TS],
            ['GBP', 'EUR', 0.8482799999999999, Start_Period_TS],
            ['CAD', 'EUR', 1.4549, Start_Period_TS],
            ['JPY', 'EUR', 121.75, Start_Period_TS]
        ]
        test_df = pd.DataFrame(test_matrix, columns=header)
        ECB_data = ECB_rates_extractor(key_curr_vector,
                                       Start_Period, End_Period=None)
        self.assertTrue(ECB_data.equals(test_df))


    def test_CW_downloader(self):
        pass



if __name__ == "__main__":
    # execute only if run as a script
    unittest.main()
