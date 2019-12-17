from datetime import *
import requests
import time
import os
import pandas as pd
import io
import data_download
import numpy as np

# a = 'bitfinex'


a=[1,3,4,5,6,2,7]
b=[2,3,4,5,6,7,8]
a=np.array(a)
b=np.array(b)
c=np.column_stack((a,b))
c=np.column_stack((c,a))
i,j=np.where(c==2)
z=np.where(c==2)
print(c)
v=c.sum(axis=1)
print(v)
f=c/a[:,None]
r=c[2,1:]/c[1,1:]
print(r)
print(len(c))
print(c*2)
t=np.row_stack((a,b))
print(t)

