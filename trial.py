# standard library import
import numpy as np

from datetime import datetime, timezone

from os import path
# third party import
import pandas as pd

# from cryptoindex.data_setup import timestamp_gen, timestamp_to_human


# ts_array = np.array([1577836800, 1577923200, 1578009600, 1578096000, 1578182400])
# date = timestamp_to_human(ts_array)
# print(date)

filename = "mongo_test.json"
data_folder = path.join(path.dirname(__file__), "test_folder", filename )

print(data_folder)

