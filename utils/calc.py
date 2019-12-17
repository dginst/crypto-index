import numpy as np
import datetime
from datetime import *


# Return the Initial Divisor for the index. It identifies the position of the initial date in the Curr_Volume_Matrix. 
# At the moment the initial date is 2016/01/01 or 1451606400 as timestamp
# where:
# logic_matrix = second requirement Curr_Volume_Matrix, composed by 0 if negative, 1 if positive
# Curr_Price_Matrix = final price Curr_Volume_Matrix of each currency 
# sm = synthetic market cap derived weight

def calc_initial_divisor(Curr_Price_Matrix, logic_matrix, sm, initial_date='01-01-2016' ):
    initial_date = datetime.strptime(initial_date, '%m-%d-%Y')
    initial_timestamp=str(int(time.mktime(initial_date.timetuple())))
    index = np.where( Curr_Price_Matrix == initial_timestamp)
    index_tuple = list(zip(index[0], index[1])) 
    Initial_Divisor =  (Curr_Price_Matrix[index_tuple[0]] * sm[index_tuple[0]] * logic_matrix[index_tuple[0]]).sum() / 1000
    return Initial_Divisor



 # Return an array with the divisor for each day.
 # logic_matrix = second requirement Curr_Volume_Matrix, composed by 0 if negativa, 1 if positive
 # final_price Curr_Volume_Matrix of each currency
 # final_volume Curr_Volume_Matrix of each currency

def divisor_adjustment(Curr_Price_Matrix, Curr_Volume_Matrix, logic_matrix, sm, initial_date='01-01-2016'):
    divisor_array=np.array(calc_initial_divisor(Curr_Price_Matrix, logic_matrix, sm, initial_date))
    for i in range(len(logic_matrix)-1):
        if logic_matrix[i+1].sum() == logic_matrix[i].sum():
            divisor_array=np.append(divisor_array, divisor_array[i])
        else:
            new_divisor=divisor_array[i]*(Curr_Price_Matrix[i+1] * Curr_Volume_Matrix[i+1] * logic_matrix[i+1]).sum() / (Curr_Price_Matrix[i+1] * Curr_Volume_Matrix[i] * logic_matrix[i]).sum()
            divisor_array=np.append(divisor_array,new_divisor)
    return divisor_array
 
 

# Return an array of the daily level of the Index
 # where:
 # logic_matrix = second requirement Curr_Volume_Matrix, composed by 0 if negativa, 1 if positive
 # Curr_Price_Matrix = final price Curr_Volume_Matrix of each currency
 # f = final_volume Curr_Volume_Matrix of each currency
 # sm = synthetic market cap derived weight

def index_level_calc(Curr_Price_Matrix, Curr_Volume_Matrix, logic_matrix, sm, initial_date='01-01-2016'):
    index_level = np.array([])
    divisor_array=divisor_adjustment(Curr_Price_Matrix, Curr_Volume_Matrix, logic_matrix, sm, initial_date)
    for i in range(len(logic_matrix)):
        new_index_item=(Curr_Price_Matrix[i] * sm[i] * logic_matrix[i]).sum() / divisor_array[i]
        index_level=np.append(index_level,new_index_item)
    return index_level



# Return an array with the value of the smoothing factor for 90 days (0-89)
# is utilized to calc the EWMA(exponential weighted moving average)

def smoothing_factor(lambda_smooth=0.94, moving_average_period=90):
    num_vector =  np.array([range(moving_average_period)])        
    smooth_factor_array = np.array([])
    for index in num_vector:
        new_lambda=(1-lambda_smooth)*lambda_smooth**(index)
        smooth_factor_array=np.append(smooth_factor_array,new_lambda)    
    return smooth_factor_array



#Return the 90-days EWMA volume for each currency.

def emwa_currency_volume(Curr_Volume_Matrix,moving_average_period=90):
    emwa_gen=np.array([])
    for col_id in range(Curr_Volume_Matrix.shape[1]):
        EWMA_coin = np.array([])
        period_start = 0
        period_end = moving_average_period-1
        while period_start < (len(Curr_Volume_Matrix)-moving_average_period) and period_end < len(Curr_Volume_Matrix):
            period_start += 1
            period_end += 1
            period_average=(Curr_Volume_Matrix[period_start:period_end,col_id]*smoothing_factor()).sum()
            EWMA_coin=np.append(EWMA_coin,period_average)    
        if emwa_gen.size==0:
            emwa_gen = np.array(EWMA_coin)
        else:
            emwa_gen= np.column_stack((emwa_gen,EWMA_coin))       
    return emwa_gen


# creating a vector with the total volumes for each day

def EMWA_weights(Curr_Volume_Matrix,logic_matrix):
    emwa_volume_curr=emwa_currency_volume(Curr_Volume_Matrix)
    total_EMWA_volume = emwa_volume_curr.sum(axis=1)
    EMWA_weights_matrix = (emwa_volume_curr* logic_matrix) / total_EMWA_volume[:, None]
    return EMWA_weights_matrix


# function take as input:
# Curr_Pice_matrix: the Price matrix that has different cryptoasset as column and date as row
# weight_index: vector that contains the weights for every Crytpo Asset indicated in Curr_Price_matrix
# synt_matrix_old: the syntethic matrix of the previuos day, on default in None meaning that is the
# first day after the index rebalancing
# returns a matrix with the same number and order of column of the Curr Price Matrix containing
# the value of the syntetic portfolio divided by single currency
# every c.a. 3 months the index is rebalanced, so the synt_matrix function has to be called anew

def synt_matrix_daily(Curr_Price_Matrix,weight_index, synt_matrix_old=None, synt_ptf_value=100):
    #returns computed considering that today is the last row and yesterday is the row before
    daily_return=(Curr_Price_Matrix[len(Curr_Price_Matrix)-1,1:]-Curr_Price_Matrix[len(Curr_Price_Matrix)-2,1:])/Curr_Price_Matrix[len(Curr_Price_Matrix)-2,1:]
    synt_matrix_date=np.array(Curr_Price_Matrix[len(Curr_Price_Matrix)-1,0])
    if synt_matrix_old == None:
        synt_matrix= weight_index*synt_ptf_value
        synt_matrix=np.column_stack((synt_matrix_date,synt_matrix))
    else:
        synt_matrix_new_value=daily_return*synt_matrix_old[len(synt_matrix_old),1:]
        synt_matrix_new_row=np.column_stack((synt_matrix_date,synt_matrix_new_value))
        synt_matrix=np.row_stack((synt_matrix_old,synt_matrix_new_row))
    return synt_matrix



# crating a matrix with the returns of the currencies computed from the Curr_Price_Matrix

def price_return(Curr_Price_Matrix):
    return_matrix = np.array([])
    for i in range(1,len(Curr_Price_Matrix)):
        return_calc = (Curr_Price_Matrix[i+1]-Curr_Price_Matrix[i])/Curr_Price_Matrix[i]
        np.append(return_matrix, return_calc)
    return return_matrix

    
# series of function to create the logic_matrix1
# 
# this create the days between the first day of the quarter and the day of the board meeting.  
def datetime_diff():
    datetime_diff = np.array([])
    for years in [2016,2017,2018,2019]:
        for months in [1,4,7,10]:
            difference = (datetime.datetime(years,months,1)-datetime.datetime(years,months+2,21)).days
            datetime_diff = np.append(datetime_diff,difference)
    return datetime_diff

# this find the cordinate of the first day of each quarter in the curr_matrix_volume 
def quarter_initial_position(Curr_Volume_Matrix):
    index = []
    for years in [2016,2017,2018,2019]:
        for months in [1,4,7,10]:
            coord = np.where(Curr_Volume_Matrix == datetime.datetime(years,months,1))
            coord = list(zip(coord[0], coord[1]))
            index = index.append(coord[0])  
    return index



def perc_volumes_per_exchange():
    col: len(Curr_exchanges_volumes[0]) 
    sum_array = np.array([])
    tota_per_row = np.array([])  
    for coord in quarter_initial_position():
        for delta in datetime_diff():
            for col in range(1,col):
                sum_array = np.append(sum_array,(np.sum(Curr_Volume_Matrix[coord:coord+delta,1:col], axis = 0)))
                total_per_row = np.append(total_per_row,(Curr_Volume_Matrix[coord:coord+delta,1:col]).sum())
                requirement = sum_array / total_per_row[:, None]
    return requirement


# it returns a matrix of 0 and 1 following the requirements

def Curr_logic_matrix1(Overall_Requirement_Matrix):
    req1_matrix = np.array([])
    Overall_req1_matrix = np.array([])
    for curr_matrix in range(len(Overall_req1_matrix)):
        for row in range(len(Curr_requirement_matrix)):
            if np.any(Curr_Requirement_matrix[i]) > 0.80:
                req1_matrix = np.append(req1_matrix, 0)
            else: 
                req1_matrix = np.append(req1_matrix, 1)    
            Overall_req1_matrix = np.column_stack(req1_matrix)    

        return Overall_req1_matrix



       

# function takes as input:
# Curr_Pice_matrix: the Price matrix that has different cryptoasset as column and date as row
# historic weight_index: matrix that contains the weights for every Crytpo Asset indicated in Curr_Price_matrix
# comitee_date: vector that contains the past theorical date of comitee reunion
# as implemented, comitee_date has to be in timestamp format (consider to upgrade)
# function iterate for every date in comitte_date vector constructing a portfolio with default 100 value
# rebalanced every comitee_date date; it returns a matrix that simulate the value of the portfolio over history
#####################################################################################################
#### consider that doen not makes sense, imo, to have the portfolio reduced (aumented) in value #####
#### after every commitee: fixing every time the value at 100 do not allow to show #################
####  the hisorical value constraction of the strategy #########################
###############################################################################################
def synt_matrix_historic(Curr_Price_Matrix,historic_weight_index, comitee_date):
    historical_synt_matrix=np.array([])
    for i,date in enumerate(comitee_date):
        start_period, =np.where(Curr_Price_Matrix[:,0]==date)
        periodic_synt_mat=np.array([])
        while start_period != comitee_date[i+1]:
            if periodic_synt_mat.size==0:
                weight_index=historic_weight_index[start_period]
                periodic_synt_mat=synt_matrix_daily(Curr_Price_Matrix,weight_index)
                start_period=start_period+1
            else:
                weight_index=historic_weight_index[start_period]
                periodic_synt_mat=synt_matrix_daily(Curr_Price_Matrix,weight_index, periodic_synt_mat)
                start_period=start_period+1
        if historical_synt_matrix.sixe==0:
            historical_synt_matrix=periodic_synt_mat
        else:
            historical_synt_matrix=np.row_stack((historical_synt_matrix,periodic_synt_mat))
    return historical_synt_matrix
