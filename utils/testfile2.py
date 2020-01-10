from datetime import *
import datetime
import requests
import time
import os
import pandas as pd
import io
import data_download
import data_setup
import numpy as np

# a = 'bitfinex'


<<<<<<< HEAD
# a=[1000,3,4,5,6,2,7]
=======
# a=[1,3,4,5,6,2,7]
>>>>>>> 3b92781b0b89ee0ccfe078526a0afdcb85bb5e79
# b=[2,3,4,5,6,7,8]
# a=np.array(a)
# b=np.array(b)
# c=np.column_stack((a,b))
# c=np.column_stack((c,a))
# i,j=np.where(c==2)
# z=np.where(c==2)
# print(c)
# v=c.sum(axis=1)
# print(v)
# f=c/a[:,None]
# r=c[2,1:]/c[1,1:]
# print(r)
# print(len(c))
# print(c*2)
# t=np.row_stack((a,b))
# print(t)
<<<<<<< HEAD
# y=[1,2,3,4,5,6,7,8]
# y=np.array(y)
# print(y)
# x=np.append(t,y)
# print(x)

# print(len(c[0]))
# gg=np.sum(c,axis=1)
# print(gg/gg.sum())
# Start_Period='2020-01-02'
# End_Period='2020-01-05'
# key_currency_vector= ['USD','JPY']
# a=data_download.ECB_rates_extractor(key_currency_vector,Start_Period)
# print(a)

# frequency='D'
# currency_denominat='EUR'
# typeRates='SP00'
# series_variation='A'
# entrypoint = 'https://sdw-wsrest.ecb.europa.eu/service/' 
# resource = 'data'           
# flowRef ='EXR'
# parameters = {
#     'startPeriod': Start_Period, 
#     'endPeriod': End_Period    
# }
# Exchange_Rate_List=pd.DataFrame()
# pd.options.mode.chained_assignment = None
# for i,currency in enumerate(key_currency_vector):
#     key= frequency+'.'+currency+'.'+currency_denominat+'.'+typeRates+'.'+series_variation
#     request_url = entrypoint + resource + '/'+ flowRef + '/' + key
#     response = requests.get(request_url, params=parameters, headers={'Accept': 'text/csv'})
#     try:
#         Data_Frame = pd.read_csv(io.StringIO(response.text))
#         Main_Data_Frame = Data_Frame.filter(['TIME_PERIOD', 'OBS_VALUE','CURRENCY','CURRENCY_DENOM'], axis=1)
#     except:
#         Main_Data_Frame=[]

# print(request_url)
# print(Main_Data_Frame)
# a=[]
# a.append(2)
# print(a)
# a.append(None)
# print(a)
# a=np.array(a)
# print(a)
# if a[1]== None:
#     print('yes')
# b=np.column_stack((a,a))
# c=np.column_stack((a,b))
# print(len(c[:,0]))
# print(c)

s=data_setup.date_reformat('12-01-1991','-')
print(s)
=======

a = data_download.CW_data_reader('coinbase-pro', 'btcusdct', '01/01/2020')

print(a)
>>>>>>> 3b92781b0b89ee0ccfe078526a0afdcb85bb5e79
