import utils.data_download as dw
from datetime import datetime
import utils.data_setup as ds
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import *
import time
#from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta


# def quarterly_period(start_date = '01-01-2016', stop_date = None):

#     start_date = datetime.strptime(start_date,'%m-%d-%Y')

#     if stop_date == None:

#         stop_date = datetime.now().strftime('%m-%d-%Y')
#         stop_date = datetime.strptime(stop_date,'%m-%d-%Y')

#     start_quarter = start_q(start_date, stop_date)
#     stop_quarter = start_quarter - 86400

#     for i in range(start_quarter.size - 1):
#         yield (start_quarter[i], stop_quarter[i+1])

# #####################

# # for start, stop in quarterly_period():
# #     print(start)
# #     print(stop)



# def stop_q(start_q_array):

#     stop_q_array = np.array([])

#     for i in range(start_q_array.size - 1):

#         stop_date = start_q_array[i + 1] - 86400
#         stop_q_array = np.append(stop_q_array, stop_date)
    
#     delta = relativedelta(months = 3)
#     last_start = start_q_array[start_q_array.size - 1]
#     print(last_start)
#     last_stop = datetime.fromtimestamp(last_start)
#     last_stop = last_stop + delta
#     print(last_stop)
#     last_stop = int(last_stop.timestamp()) - 86400
#     stop_q_array = np.append(stop_q_array, last_stop)

#     return stop_q_array

# def minus_nearer_date(date_array, date_to_check):

#     only_lesser = np.array([])
#     for element in date_array:

#         if element < date_to_check:

#             only_lesser = np.append(only_lesser, element)

#     nearest_date = only_lesser[only_lesser.size -1]    
#     # min_dist = np.absolute(only_lesser - date_to_check)
#     # nearest_date = np.amin(min_dist) + date_to_check

#     return nearest_date


# a=start_q()
# b= stop_q(a)
# print(a)
# print(b)

# today = datetime.now().strftime('%Y-%m-%d')
# today_TS = int(datetime.strptime(today,'%Y-%m-%d').timestamp()) + 3600
# d = datetime.fromtimestamp(today_TS)

# print(today)
# print(today_TS)
# print(d)


# a = [1,2,3,4,5,6,7,8,9,10]
# b=np.array(a)
# v=b*100
# head = ['ciao', 'test']
# c = np.column_stack((b,b))
# asd = ['ciao', 'test']
# d = pd.DataFrame(c,columns= head)
# g = d<3
# # print(d)
# # print(g)
# # g= g*1
# # print(g)
# # g= g.drop(columns=['A'])

# g[asd]= d[head]
# print(g)
# d['Time']=v
# g['Time']=v
# print(g)
# # pd.options.mode.chained_assignment = None  # default='warn'
# # g[asd][g['Time'].between(300, 800, inclusive = True)] = d[head][d['Time'] == 100]
# x=d.loc[d.Time == 100, head]
# print(x)
# g.loc[g.Time.between(300, 800, inclusive = True), asd] = np.array(x)
# # d[head][d['Time'] == 100]
# print(d)
# print(g)
# new = ['ciao', 'test', 'Time']
# p = (g>1)*1
# print(p)
# print(p*g)
# x = ['Ciao','fra']
# reference_date_array = [1,2,3,4,5,6,7,8,9]
# reference_date_array = np.array(reference_date_array)
# reshaped_matrix = pd.DataFrame(reference_date_array, columns = ['Time'])
# for el in x:
#     reshaped_matrix[el] = np.zeros(len(reference_date_array))
# print(reshaped_matrix)



# def timestamp_gen(start_date, end_date = None,  EoD = 'Y'):

#     if end_date == None:
#         end_date = datetime.now().strftime('%m-%d-%Y')

#     end_date = datetime.strptime(end_date,'%m-%d-%Y')
#     end = int(time.mktime(end_date.timetuple()))
#     end = end + 3600
#     start = datetime.strptime(start_date,'%m-%d-%Y')
#     start = int(time.mktime(start.timetuple()))
#     start = start + 3600

#     array = np.array([start])
#     date = start
#     print(end)
   
#     while date < end:
#         date = date + 86400
#         array = np.append(array, date)
    
#     array = array[:len(array) - 1]
    
#     return array

# x = timestamp_gen('01-01-2019') 
# print(x)

ts = 1459465200

#ts = ts - 3600
l = datetime.utcfromtimestamp(ts)
print(ts)
print(l)
print(l.hour)
if datetime.utcfromtimestamp(ts).hour == 23:
    ts = ts+3600

print(ts)
print(datetime.utcfromtimestamp(ts))