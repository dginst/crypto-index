import utils.data_download as dw
from datetime import datetime
import utils.data_setup as ds
import numpy as np
import pandas as pd
import datetime
from datetime import *
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

# def perdelta(start, end, delta):

#     curr = start
#     while curr < end:
#         yield curr
#         curr += delta

# #####################################

# def is_business_day(date):
    
#     return bool(len(pd.bdate_range(date, date)))


# #####################################################

# def previuos_business_day(date):

#     while is_business_day(date) == False:
#         date = date - timedelta(days = 1)
    
#     return date


# ########################################################

# def start_q(start_date = '01-01-2016', stop_date = None, delta = None, timeST = 'Y', lag_adj = 3600): 

#     start_date = datetime.strptime(start_date,'%m-%d-%Y')

#     if delta == None:

#         delta = relativedelta(months = 3)
#     if stop_date == None:

#         stop_date = datetime.now().strftime('%m-%d-%Y')
#         stop_date = datetime.strptime(stop_date,'%m-%d-%Y')

#     start_day_arr = np.array([])

#     for result in perdelta(start_date, stop_date, delta):
#         if timeST == 'Y':
#             result = int(result.timestamp())
#             result = result + lag_adj
#         else:
#             result = result.strftime('%m-%d-%Y')
        
#         start_day_arr = np.append(start_day_arr, result)

#     return start_day_arr

# #################################################################

# def board_meeting_day(start_date = '12-21-2015', stop_date = None, delta = None, timeST = 'Y', lag_adj = 3600):
    
#     start_date = datetime.strptime(start_date,'%m-%d-%Y')

#     if delta == None:

#         delta = relativedelta(months = 3)
#     if stop_date == None:

#         stop_date = datetime.now().strftime('%m-%d-%Y')
#         stop_date = datetime.strptime(stop_date,'%m-%d-%Y')

#     board_day = np.array([])

#     for result in perdelta(start_date, stop_date, delta):
#         # checks if the date generated is a business day, if yes the date is added to the array
#         # if not, the fuction go 1 day back and makes the same check. If the condition is still not respected, is going back another day.
#         # the if statement goes back two days maximum: Sunday and Saturday.
#         result = previuos_business_day(result) 

#         if timeST == 'Y':

#             result = int(result.timestamp())
#             result = result + lag_adj
#         else:

#             result = result.strftime('%m-%d-%Y')

#         board_day = np.append(board_day, result)


#     return board_day

# ###############################################################
# # b = datetime(2016, 1, 1)
# # print(b)

# # for i in start_q('01-01-2016'):
# #     print(i)

# # print(is_business_day('01-01-2020'))

# # start = '02-03-2020'
# # start_date = datetime.strptime(start,'%m-%d-%Y')
# # print(is_business_day(start_date))
# # print(previuos_business_day(start_date))
# # print(board_meeting_day())

# ###########################aggiungere descrizione e mettere in calc
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


a = [1,2,3,4,5,6,7,8,9,10]
b=np.array(a)
v=b*100
head = ['ciao', 'test']
c = np.column_stack((b,b))
asd = ['ciao', 'test']
d = pd.DataFrame(c,columns= head)
g = d<3
# print(d)
# print(g)
# g= g*1
# print(g)
# g= g.drop(columns=['A'])

g[asd]= d[head]
print(g)
d['Time']=v
g['Time']=v
print(g)
# pd.options.mode.chained_assignment = None  # default='warn'
# g[asd][g['Time'].between(300, 800, inclusive = True)] = d[head][d['Time'] == 100]
x=d.loc[d.Time == 100, head]
print(x)
g.loc[g.Time.between(300, 800, inclusive = True), asd] = np.array(x)
# d[head][d['Time'] == 100]
print(d)
print(g)
new = ['ciao', 'test', 'Time']
p = (g>1)*1
print(p)
print(p*g)

