import utils.data_download as dw
from datetime import datetime
import utils.data_setup as ds
import numpy as np
import datetime
import pandas as pd
from datetime import *

# a= dw.CW_data_reader('bitfinex','btcusd','01-01-2020')
# header = ['Time', 'Close Price', "Crypto Volume", "Pair Volume"]
# print(a)
# c=1578096000
# d = a[a['Time'] == c]['Close Price']
# print(float(d))
# f = a[a['Time'] == c].iloc[:,1:4]
# print(f)
# g= f*f
# print(g)
# t = np.column_stack((c,g))
# print(t)
# t = pd.DataFrame(t, columns = header 
# )
# a = a.append(t)
# a = a.sort_values(by=['Time'])
# a = a.reset_index(drop=True)
# a['ciao'] = np.zeros(20)
# print(a)
today = datetime.now().strftime('%Y-%m-%d')
key= ['USD', 'GBP', 'CAD', 'JPY']
rates = ds.ECB_setup(key, '2020-01-15', today, timeST='Y')
print(rates)
file_json = open(""+today+"_"".json", "w")


t= pd.read_json('C:/Projects/dginst/crypto-index/2020-01-22_.json')
print(t)
