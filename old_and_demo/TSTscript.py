# # standard library import
# import os.path
# from pathlib import Path
import json
from datetime import datetime
import cryptoindex.calc as calc
from datetime import *
import time

# third party import
from pymongo import MongoClient
import numpy as np
import pandas as pd

# local import
import cryptoindex.mongo_setup as mongo
import cryptoindex.data_setup as data_setup
import cryptoindex.data_download as data_download


#     sup_date_array = np.append(sup_date_array, int(a[0]))

# print(sup_date_array)
# a = [1,2,3,4,5,6,7]
# b=np.array(a)
# z = ['a','b']
# c=np.column_stack((b,b))
# c = pd.DataFrame(c, columns= z)
# print(c)
# x = c.loc[c['a']==10]
# # print(x)
# # print(x.empty)
# # if x.empty == True:
# #     print('yes')

# start_period = '2020-01-02'
# key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']
# header = ['CURRENCY', 'CURRENCY_DENOM', 'OBS_VALUE', 'TIME_PERIOD']
# function that downloads the exchange rates from the ECB web page and returns a matrix (pd.DataFrame) that
# indicates: on the first column the date, on the second tha exchange rate vakue eutro based,
# on the third the currency, on the fourth the currency of denomination (always 'EUR')
# key_curr_vector expects a list of currency in International Currency Formatting (ex. USD, GBP, JPY, CAD,...)
# the functions diplays the information better for a single day data retrival, however can works with multiple date
# regarding the other default variables consult the ECB api web page
# start_period has to be in YYYY-MM-DD format

# c =  data_download.ECB_rates_extractor(key_curr_vector, start_period, End_Period = None)
# d = data_download.ECB_rates_extractor(key_curr_vector, start_period, End_Period = None)
# #c.reset_index(drop=True, inplace=True)
# print(c)
# e = np.array(c)
# print(e)
# d = [['USD', 'EUR', 1.1193, '1577923200'],
#  ['GBP', 'EUR', 0.8482799999999999, '1577923200'],
#  ['CAD', 'EUR', 1.4549, '1577923200'],
#  ['JPY', 'EUR', 121.75, '1577923200']]
# d = pd.DataFrame(d, columns = header)
# print(d)
# print(c.equals(d))
# a= (12,13)
# b= (10,11)
# c = np.row_stack((a,b))
# # print(a)
# # print(c)
# # print(c[0])
# for row in c:
#     print(row[0])
# a = data_setup.timestamp_gen('02-15-2016', '11-15-2016')
# b = data_setup.timestamp_gen_legal_solar(a)
# print(a==b)
# print(b)
# print(len(a))
# print(len(b))

# connection = MongoClient('localhost', 27017)
# #creating the database called index
# db = connection.index

# db = 'index'

# ################################# coinbasetraw


# coll = 'coinbasetraw'

# df = mongo.query_mongo(db, coll)

# df = df.rename(columns={'pair' : 'Pair', 'exchange' : 'Exchange', 'time' : 'Time', 'price' : 'Close Price', 'volume' : 'Crypto Volume'})

# df['Time'] = df['Time'].apply(str)

# df['Pair'] = df['Pair'].str.lower()
# print(df)

# df = df.to_dict(orient='records')

# print(df)

# a = pd.read_json('EXC_rawdata.json', lines=True)
# head = ['Pair', 'Exchange', 'Time', 'Close Price', 'Crypto Volume', 'Pair Volume']
# a= a.drop(columns = ['_id', 'date'])
# #connecting to mongo in local
# connection = MongoClient('localhost', 27017)
# #creating the database called index
# db = connection.index

# db.EXC_test.drop()

# #creating the empty collection rawdata within the database index
# db.EXC_test.create_index([ ("id", -1) ])
# collection = db.EXC_test

# data = a.to_dict(orient='records')
# collection.insert_many(data)

####

# arr = data_setup.timestamp_gen('04-17-2020')
# arr = [str(el) for el in arr]
# v = list(arr)

# head = ['Time', 'Val']
# a = [ 1, 2]
# b = [2,3]
# c = np.row_stack((a,b))
# d = pd.DataFrame(c, columns = head)
# z=d.pct_change()
# print(z.iloc[[1]])

# rebalance_start_date = calc.start_q('01-01-2016')
# rebalance_start_date = calc.start_q_fix(rebalance_start_date) ####
# rebalance_stop_date = calc.stop_q(rebalance_start_date)
# board_date = calc.board_meeting_day()
# board_date_eve = calc.day_before_board()
# next_rebalance_date = calc.next_start()

# head = ['Time', 'Val']
# head1 = ['Time', 'letter']
# a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
# b = ['aa&', 'b&', 'cccc&', 'dggg&', 'f&hh',
#      'gjjj&', 'f&h', 'u&i', 'lu&h', '8&m']
# g = [1, 50, 88, 99]
# h = ['ciao', 'x', 'y', 'z']
# c = np.column_stack((a, b))
# p = np.column_stack((g, h))
# d = pd.DataFrame(c, columns=head)
# e = pd.DataFrame(p, columns=head1)
# zer = np.zeros_like(np.array(e))
# header = e.columns
# logic = np.column_stack((1, 2))
# zer = pd.DataFrame(zer, columns=header)
# zer.loc[:, :] = logic
# print(header)
# print(d)
# print(e)
# print(zer)
# print(zer.loc[1:, :])

# # print(list(d['Time'].unique()))
# merged = pd.merge(d, e, on='Time', how='left')
# print(merged)
# # k = e.apply(lambda x: x.nunique())
# # print(type(k))
# # index_ = list(k.index)
# # print(index_)
# # print(k)
# # dd = np.array([])
# # print(np.zeros((2, 2)))
# d['Time'] = pd.to_numeric(d['Time'])
# d['gfgfg'] = d['Time'].cumsum()
# print(d)
# merged.fillna('NaN', inplace=True)
# print(merged)
# aaa = merged.loc[merged['letter'] != 'NaN']
# print(aaa)
a = "2020-04-21T20:00:06.410+00:00"
print(type(a))
hour = a[11:16]
print(hour)
# connecting to mongo in local
connection = MongoClient("localhost", 27017)
# creating the database called index
db = "index"

# naming the existing collections as a variable
coll = "EXC_rawtest"

mat = mongo.query_mongo(db, coll)
mat["date"] = [str(el) for el in mat["date"]]
mat = mat[["Pair", "Exchange", "Close Price", "Time", "Crypto Volume", "date"]]
mat["hour"] = mat["date"].str[11:16]
print(mat.head(10))
