from datetime import *
import requests
import time
import os
import pandas as pd
import io
import data_download
import numpy as np

# a = 'bitfinex'

# b= 'btcusd'
# c=data_download.CW_data_reader(a,b,'12-10-2019')
# print(c)
a=[1,3,4,5,6,2,7]
b=[2,3,4,5,6,7,8]
a=np.array(a)
b=np.array(b)
c=np.column_stack((a,b))
c=np.column_stack((c,a))
# i,j=np.where(c==2)
# z=np.where(c==2)
# coord = list(zip(z[0], z[1])) 
# print(c)
# print(i)
# print(j)
# print(z)
# print(coord)
# print(coord[0][0])
print(len(c))
a=np.append(a,'1')
