import numpy as np
import pandas as pd
import datetime
from datetime import *
from datetime import date, datetime, timedelta


# Return the Initial Divisor for the index. It identifies the position of the initial date in the Curr_Volume_Matrix. 
# At the moment the initial date is 2016/01/01 or 1451606400 as timestamp
# where:
# logic_matrix = second requirement Curr_Volume_Matrix, composed by 0 if negative, 1 if positive
# Curr_Price_Matrix = final price Curr_Volume_Matrix of each currency 
# sm = synthetic market cap derived weight

def calc_initial_divisor(Curr_Price_Matrix, logic_matrix, sm, initial_date = '01-01-2016'):

    # convert the date into timestamp 
    initial_date = datetime.strptime(initial_date, '%m-%d-%Y')
    initial_timestamp = str(int(time.mktime(initial_date.timetuple())))

    # find the toimestamp related index in the data matrix
    index = np.where(Curr_Price_Matrix == initial_timestamp)
    index_tuple = list(zip(index[0], index[1])) 

    # computing the divisor 
    Initial_Divisor =  (Curr_Price_Matrix[index_tuple[0]] * sm[index_tuple[0]] * logic_matrix[index_tuple[0]]).sum() / 1000

    return Initial_Divisor



 # function that returns an array with the divisor for each day, the inputs are:
 # logic_matrix = second requirement Curr_Volume_Matrix, composed by 0 if negativa, 1 if positive
 # final_price Curr_Volume_Matrix of each currency
 # final_volume Curr_Volume_Matrix of each currency
 # initial date set by default in 01/01/16

def divisor_adjustment(Curr_Price_Matrix, Curr_Volume_Matrix, logic_matrix, sm, initial_date = '01-01-2016'):

    # use the function to compute the initial divisor
    divisor_array = np.array(calc_initial_divisor(Curr_Price_Matrix, logic_matrix, sm, initial_date))

    # for loop that iterates through all the date (length of logic matrix)
    # returning a divisor for each day
    for i in range(len(logic_matrix)-1):
        if logic_matrix[i+1].sum() == logic_matrix[i].sum():
            divisor_array=np.append(divisor_array, divisor_array[i])
        else:
            new_divisor = divisor_array[i]*(Curr_Price_Matrix[i+1] * Curr_Volume_Matrix[i+1] * logic_matrix[i+1]).sum() / (Curr_Price_Matrix[i+1] * Curr_Volume_Matrix[i] * logic_matrix[i]).sum()
            divisor_array = np.append(divisor_array, new_divisor)

    return divisor_array
 
 

# function that returns an array of the daily level of the Index, where:
# logic_matrix = second requirement Curr_Volume_Matrix, composed by 0 if negativa, 1 if positive
# Curr_Price_Matrix = final price of each currency (columns are different exchanges)
# Curr_Volume_Matrix = Volume matrix of each currency (columns are different exchanges)
# sm = synthetic market cap derived weight
# initial date default value set at 01/01/2016

def index_level_calc(Curr_Price_Matrix, Curr_Volume_Matrix, logic_matrix, sm, initial_date='01-01-2016'):

    # find the divisor related to each day starting from initil date
    divisor_array = divisor_adjustment(Curr_Price_Matrix, Curr_Volume_Matrix, logic_matrix, sm, initial_date)
    index_level = np.array([])

    for i in range(len(logic_matrix)):
        new_index_item = (Curr_Price_Matrix[i] * sm[i] * logic_matrix[i]).sum() / divisor_array[i]
        index_level = np.append(index_level, new_index_item)

    return index_level



# function that returns an array with the value of the smoothing factor for 90 days (0-89)
# is utilized to calc the EWMA(exponential weighted moving average)
# default lambda value is a standard and the period is set on default to be 90 days

def smoothing_factor(lambda_smooth = 0.94, moving_average_period = 90):

    # creates a vector of number between 0 and 89 
    num_vector =  np.array([range(moving_average_period)])    

    smooth_factor_array = np.array([])
    for index in num_vector:
        new_lambda = (1 - lambda_smooth) * (lambda_smooth ** (index))
        smooth_factor_array = np.append(smooth_factor_array, new_lambda)    

    return smooth_factor_array



# function that returns the 90-days EWMA volume for each currency.
# takes as input the period that is set on 90 days as default and the 
# Curr_Volume_Matrix = Volume matrix of each currency (columns are different exchanges)

def emwa_currencies_volume(Curr_Volume_Matrix, moving_average_period = 90):

    emwa_gen = np.array([])

    for col_id in range(Curr_Volume_Matrix.shape[1]):
        EWMA_coin = np.array([])
        period_start = 0
        period_end = moving_average_period-1

        while (period_start < (len(Curr_Volume_Matrix) - moving_average_period) and period_end < len(Curr_Volume_Matrix)):
            period_start += 1
            period_end += 1
            period_average = (Curr_Volume_Matrix[period_start:period_end, col_id] * smoothing_factor()).sum()
            EWMA_coin = np.append(EWMA_coin, period_average) 

        if emwa_gen.size == 0:
            emwa_gen = np.array(EWMA_coin)
        else:
            emwa_gen = np.column_stack((emwa_gen, EWMA_coin))   

    return emwa_gen

# this function return a range of data in a certain interval. Is usefull because is it possible  to iterate on it.

def perdelta(start, end, delta):

    curr = start
    while curr < end:
        yield curr
        curr += delta

    return 


# this function generates an array cointaing the date of the start of each quarter from the start of the index till the current day.
def start_q(perdelta): 

    start_q = np.array([])
    for result in perdelta(datetime.datetime(2016, 1, 1), datetime.datetime.today(), relativedelta(months=3)):
        start_q = np.append(start_q, result)
    
    return start_q


# this fuction checks if a day is a business day or not.
# returns a bool value 

def is_business_day(date):
    
    return bool(len(pd.bdate_range(date, date)))

# this function generates an array cointaing the date of the boeard meeting in each quarter on the 21st of 
# the third month of the quarter.

def board_meeting_day():
    
    board_day = np.array([])

    for result in perdelta(datetime.datetime(2015, 12, 21), datetime.datetime.today(), relativedelta(months=3)):
        # checks if the date generated is a business day, if yes the date is added to the array
        # if not, the fuction go 1 day back and makes the same check. If the condition is still not respected, is going back another day.
        # the if statement goes back two days maximum: Sunday and Saturday.
        if is_business_day(result) == True:
            board_day = np.append(board_day, result.timestamp())
        elif is_business_day(result - (datetime.timedelta(days = 1)).timestamp()) == True:
            board_day = np.append(board_day, (result - datetime.timedelta(days = 1)).timestamp())
        else:
            board_day = np.append(board_day, (result - datetime.timedelta(days = 2)).timestamp())
    
    return board_day


# returns an array with the day before of each board meeting.

def day_before_board():

    before_board_day = board_meeting_day() - 86400

    return before_board_day



# function returns a matrix with the weights that every currency should have
# takes as input the currecny matrix of volume and the logic matrix 

def EMWA_weights(Curr_Volume_Matrix, logic_matrix):

    emwa_volume_curr = emwa_currencies_volume(Curr_Volume_Matrix)
    total_EMWA_volume = emwa_volume_curr.sum(axis = 1)
    EMWA_weights_matrix = (emwa_volume_curr * logic_matrix) / total_EMWA_volume[:, None]

    return EMWA_weights_matrix


# return a matrix of the index weights at the start of each quarter
# takes as imput the array with the dates = the day before the meeing day ( day_before_board() )
# takes as imput the matrix EWMA_weights

def q_weights(day_before_board, EWMA_weights):
    
    dates = day_before_board()
    weights = EWMA_weights()
    q_weights = np.array([])
    
    for date in dates:

        if q_weights == None:
            row = weights[weights[:, 0] == date]
            q_weights = np.append(q_weights, row)
        
        else:
            row = weights[weights[:, 0] == date]
            q_weights = np.stack((q_weights, row))

    return q_weights


# this function converts the q_weights matrix and return the matrix with the quarter start date and weights
def weight_index(q_weights):

    init_q_days = np.array([])
    q_weights = q_weights()

    for date in q_weights[:,0]:

        conv = datetime.datetime.fromtimestamp(date)
        conv = conv.replace(day=1) + relativedelta(months=1)
        conv = conv.timestamp()

        init_q_days = np.append(init_q_days, conv)

    init_q_days = np.column_stack((init_q_days, q_weights[:, 1 : q_weights.shape[1]]))

    return init_q_days   
# questa matrice sarà sicuramente più corta delle altre, quindi o allungarla oppure cambiare la funzion
# la funzione di synt_matrix_daily



# function that returns the return matrix for all cryptocurrency
# the first column of the return_matrix contains date
# function works on default considering the data placed in date-ascendent way (oldest data in position 0)
# to consider the opposite case modify the default variable Date_order

def price_return(Curr_Price_Matrix, date_order = 'ascendent'):

    return_matrix = np.array([])

    for i in range(len(Curr_Price_Matrix)):
        if date_order != 'ascendent':
            return_value = (Curr_Price_Matrix[i+1][1:]-Curr_Price_Matrix[i][1:])/Curr_Price_Matrix[i][1:]
            return_date = Curr_Price_Matrix[i+1][0]
            vector=np.column_stack((return_date, return_value))
        else:
            return_value = (Curr_Price_Matrix[i][1:]-Curr_Price_Matrix[i+1][1:])/Curr_Price_Matrix[i+1][1:]
            return_date = Curr_Price_Matrix[i][0]
            vector = np.column_stack((return_date, return_value))   
        if return_matrix.size == 0:
            return_matrix = vector
        else:
            return_matrix = np.row_stack((return_matrix, vector))

    return return_matrix


# function returns a matrix with the same number and order of column of the Curr Price Matrix containing
# the value of the syntetic portfolio divided by single currency
# function take as input:
# Curr_Price_matrix: the Price matrix that has different cryptoasset as column and date as row
# weight_index: vector that contains the weights for every Crytpo Asset indicated in Curr_Price_matrix
# synt_matrix_old: the syntethic matrix of the previuos day, on default in None meaning that is the
# first day after the index rebalancing
# every c.a. 3 months the index is rebalanced, so the synt_matrix function has to be called anew

#def synt_matrix_daily(Curr_Price_Matrix, weight_index, synt_matrix_old = None, synt_ptf_value = 100):

    #returns computed considering that today is the last row and yesterday is the row before
  #  daily_return = (Curr_Price_Matrix[len(Curr_Price_Matrix)-1,1:]-Curr_Price_Matrix[len(Curr_Price_Matrix)-2,1:])/Curr_Price_Matrix[len(Curr_Price_Matrix)-2,1:]
  #  synt_matrix_date = np.array(Curr_Price_Matrix[len(Curr_Price_Matrix)-1,0])

  #  if synt_matrix_old == None:
  #      synt_matrix= weight_index*synt_ptf_value
  #      synt_matrix=np.column_stack((synt_matrix_date,synt_matrix))
  #  else:
 #     synt_matrix_new_value = daily_return*synt_matrix_old[len(synt_matrix_old), 1:]
 #       synt_matrix_new_row = np.column_stack((synt_matrix_date, synt_matrix_new_value))
   #     synt_matrix = np.row_stack((synt_matrix_old, synt_matrix_new_row))

 #   return synt_matrix


 #function that return the syntethic weight for the index at the end of the day of the first day of each quarter 

def q_synt_matrix(Curr_Price_Matrix, weight_index, synt_matrix_old = None, synt_ptf_value = 100):

    daily_return = price_return()
    weights = q_weights()
    logic_check = logic_matrix1()
    q_synt = np.append([])

    for date in q_weights[:,0]:

        calc1 = q_weights[q_weights[:,0] == date][0][1:d.shape[1]] * 100
        calc2 = price_return[price_return[:,0] == date][0][1:d.shape[1]] 
        calc3 = logic_matrix1[logic_matrix1[:,0] == date][0][1:d.shape[1]]
        calc_fin = (calc1 + calc1*calc2)*calc3
        q_synt = np.append(q_synt, calc_fin)

    q_synt_w = np.array([])

    for i in range(q_synt.shape[0]):
        tot = q_synt[i:1:q_synt.shape[1]].sum()
        weights = q_synt[i:1:q_synt.shape[1]] / tot
        q_synt_w = np.append(q_synt_w, weights)
    
    q_synt_w = np.column_stack(q_weights[:,0], q_synt_w)

    return q_synt_w


    

    return synt_matrix

    
############### series of function to create the Logic Matrix ############

# function that returns an array that contains the number of days
# between the first day of the quarter and the day of the board meeting
# the input values are set on default as starting from 01/01/2016 and ending on 21/10/2019
# rebalancing of index take place every 3 months and the comitee reunion date is set at the 21th day of the month

def datetime_diff(years_list = [2016, 2017, 2018, 2019], months_list = [1, 4, 7, 10], comitee_day = 21):

    datetime_diff = np.array([])

    for years in years_list:
        for months in months_list:
            difference = int(abs((datetime.datetime(years, months, 1)-datetime.datetime(years ,months+2 , comitee_day)).days))
            datetime_diff = np.append(datetime_diff, difference)

    return datetime_diff

# function returns a list of index in Curr_volume_matrix corresponding to the start date of each quarterly rebalance
# function takes as input a matrix/vector containing the complet set of date and the default years list and months list 

def quarter_initial_position(Curr_Volume_Matrix,years_list=[2016,2017,2018,2019], months_list=[1,4,7,10]):

    index = []

    for years in years_list:
        for months in months_list:
            timestamp = str(int(time.mktime(datetime.datetime(years , months, 1).timetuple())))
            coord = np.where(Curr_Volume_Matrix == timestamp)
            coord = list(zip(coord[0], coord[1]))
            index = index.append(coord[0])

    return index

# function that takes as input
# Curr_Exc_Vol: single Crypto volume matrix with exchanges as columns and date as rows; the matrix dimension has
# to be standardized not depending of the actual exchanges that trades the single crypto. If a Crypto is not present 
# in a Exchange the value will be set to 0
# function sum for every exchange the volume value of each day among one index rebalance and another 
# it returns a matrix where the first column contains the rebalancing date (timestamp) and the others columns
# contain the percentage that each exchanges represent on the total volume for the considered period
############da valutare a quali date associare le varue frazioni. es: 1/1/16 sicuro no poichè iniziamo a contare
##############da li mentre l'ultimo?

def perc_volumes_per_exchange(Crypto_Ex_Vol):

    Crypto_Ex_Vol = Crypto_Ex_Vol.to_numpy() 
    volume_fraction = np.array([])
    rebalance_interval = datetime_diff()
    rebalance_start = quarter_initial_position(Crypto_Ex_Vol)

    for i, index in enumerate(rebalance_start):

        rebalance_row = np.sum(Crypto_Ex_Vol[index:(index+rebalance_interval[i][1:])], axis=0)
        
        percentage = rebalance_row/rebalance_row.sum()
        volume_fraction = np.append(volume_fraction,percentage)
    volume_fraction = np.column_stack((rebalance_start[1:],volume_fraction))

    return volume_fraction


# This function creates a matrix of 0 and 1 checking if the first requirement is respected.
# First_requirement : The crypto-asset has no more than 80% of its combined trading volume 
# between the reconstitution day and the committe meeting day on any single pricing source.
# If the requirement is respected the function will had the value 1 on the matrix, if not it will add 0.

def Curr_logic_matrix1(Crypto_Ex_Vol):

    curr_logic_matrix1 = np.array([])
    volume_perc = perc_volumes_per_exchange(Crypto_Ex_Vol)

    for row in range(len(volume_perc)):

        # check if any of the value in array row is > than 0.80. If yes add a 0 value in the req1_matrix
        # if not add value 1 in the req_matrix
        if np.any(volume_perc[row]) > 0.80:
            curr_logic_matrix1 = np.append(curr_logic_matrix1, 0)
        else: 
            curr_logic_matrix1 = np.append(curr_logic_matrix1, 1)    

    return curr_logic_matrix1



# This function gives back the % of the EWMA-volume of any single coin compared to the aggregate EMWA-volume
# over the period between the reconstitution day and the board meeting day.
# Is is the pillar of the function to verify if the 2nd requirement is respected.

def perc_emwa_per_curr(emwa_currencies_volume):

    emwa_volume_fraction = np.array([])
    rebalance_interval = datetime_diff()
    rebalance_start = quarter_initial_position(Curr_Exc_Vol)

    for i,index in enumerate(rebalance_start):
        rebalance_row = np.sum(emwa_currencies_volume[index:(index+rebalance_interval[i][1:])], axis=0)
        percentage = rebalance_row/rebalance_row.sum()
        emwa_volume_fraction = np.append(emwa_volume_fraction, percentage)
        emwa_volume_fraction = np.column_stack((rebalance_start[1:], ewma_volume_fraction))

    return emwa_volume_fraction


# This function creates a matrix of 0 and 1 checking if the second requirement is respected.
# 2nd requirement : The crypto-asset's   trailing trading volume between the reconstitution day and the committe meeting day 
# is not less to the 2° percentile of the aggregate  trading volume for the same period 
# of available crypto-assets after the application of the precedent eligibility Rules.
# If the requirement is respected the function will had the value 1 on the matrix, if not it will add 0.

def Curr_logic_matrix2(perc_emwa_per_curr):

    logic_row = np.array([])
    curr_logic_matrix2 = np.array([])
    perc = perc_emwa_per_curr()

    # for loop that checks if the value j of the row i is bigger than 0.02 
    # where i is the row of the matrix emwa_volume_fraction
    # j = e percentage of the 90-days ewma volume against the aggreate 90-days ewma volume of the curr j
    for i in range(perc.shape[0]):
        for j in range(perc.shape[1]):
            if perc[i,j] > 0.02:
                logic_row = np.append(logic_row, 1)
            else:
                logic_row = np.append(logic_row, 0)
    
    curr_logic_matrix2 = np.column_stack((logic_row,curr_logic_matrix2))

    return curr_logic_matrix2



# function takes as input:
# Curr_Pice_matrix: the Price matrix that has different cryptoasset as column and date as row
# historic weight_index: matrix that contains the weights for every Crytpo Asset indicated in Curr_Price_matrix
# comitee_date: vector that contains the past theorical date of comitee reunion
# as implemented, comitee_date has to be in timestamp format (consider to upgrade)
# function iterate for every date in comitee_date vector constructing a portfolio with default 100 value
# rebalanced every comitee_date date; it returns a matrix that simulate the value of the portfolio over history
#####################################################################################################
#### consider that doen not makes sense, imo, to have the portfolio reduced (aumented) in value #####
#### after every commitee: fixing every time the value at 100 do not allow to show #################
####  the hisorical value constraction of the strategy #########################
###############################################################################################
def synt_matrix_historic(Curr_Price_Matrix,historic_weight_index, comitee_date):

    historical_synt_matrix=np.array([])

    for i,date in enumerate(comitee_date):
        start_period, = np.where(Curr_Price_Matrix[:,0]==date)
        periodic_synt_mat = np.array([])

        while start_period != comitee_date[i+1]:
            if periodic_synt_mat.size == 0:
                weight_index = historic_weight_index[start_period]
                periodic_synt_mat = synt_matrix_daily(Curr_Price_Matrix, weight_index)
                start_period = start_period+1
            else:
                weight_index = historic_weight_index[start_period]
                periodic_synt_mat = synt_matrix_daily(Curr_Price_Matrix, weight_index, periodic_synt_mat)
                start_period = start_period+1

        if historical_synt_matrix.size == 0:
            historical_synt_matrix = periodic_synt_mat
        else:
            historical_synt_matrix = np.row_stack((historical_synt_matrix, periodic_synt_mat))

    return historical_synt_matrix





