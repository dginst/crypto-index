import datetime
import time
import unittest

import pandas as pd
from cryptoindex.data_download import ECB_rates_extractor


class TestDownload(unittest.TestCase):

    def test_ECB_downloader(self):
        Start = '2020-01-02'
        Start_temp = Start + " 01:00:00"
        Start_TS = time.mktime(
            datetime.datetime.strptime(Start_temp,
                                       "%Y-%m-%d %H:%M:%S").timetuple())
        # remove .0 and convert to string
        Start_TS = str(int(Start_TS))
        key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']
        header = ['CURRENCY', 'CURRENCY_DENOM', 'OBS_VALUE', 'TIME_PERIOD']
        test_matrix = [
            ['USD', 'EUR', 1.1193, Start_TS],
            ['GBP', 'EUR', 0.8482799999999999, Start_TS],
            ['CAD', 'EUR', 1.4549, Start_TS],
            ['JPY', 'EUR', 121.75, Start_TS]
        ]
        test_df = pd.DataFrame(test_matrix, columns=header)
        ECB_data = ECB_rates_extractor(key_curr_vector,
                                       Start, End_Period=None)
        self.assertTrue(ECB_data.equals(test_df))

    def test_CW_downloader(self):
        pass


if __name__ == "__main__":
    # execute only if run as a script
    unittest.main()


# pytest


def test_ECB_download():

    Start = '2020-01-02'
    Start_temp = Start + " 01:00:00"
    Start_TS = time.mktime(
        datetime.datetime.strptime(Start_temp,
                                   "%Y-%m-%d %H:%M:%S").timetuple())
    # remove .0 and convert to string
    Start_TS = str(int(Start_TS))
    key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']
    header = ['CURRENCY', 'CURRENCY_DENOM', 'OBS_VALUE', 'TIME_PERIOD']
    test_matrix = [
        ['USD', 'EUR', 1.1193, Start_TS],
        ['GBP', 'EUR', 0.8482799999999999, Start_TS],
        ['CAD', 'EUR', 1.4549, Start_TS],
        ['JPY', 'EUR', 121.75, Start_TS]
    ]
    test_df = pd.DataFrame(test_matrix, columns=header)
    ECB_data = ECB_rates_extractor(key_curr_vector,
                                   Start, End_Period=None)

    assert ECB_data.equals(test_df)
