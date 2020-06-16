# standard library import
import numpy as np

from datetime import datetime, timezone

# third party import
import pandas as pd

# from cryptoindex.data_setup import timestamp_gen, timestamp_to_human


# ts_array = np.array([1577836800, 1577923200, 1578009600, 1578096000, 1578182400])
# date = timestamp_to_human(ts_array)
# print(date)

start_period = "2015-12-31"
stop_period = "2016-01-15"

ciao = date_index = pd.date_range(start_period, stop_period)

print(ciao)

End_Period = datetime.now().strftime("%Y-%m-%d")

End_Period = datetime.strptime(End_Period, "%Y-%m-%d").strftime("%m-%d-%Y")
print(End_Period)
