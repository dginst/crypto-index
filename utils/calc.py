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


# crating a matrix with the returns of the currencies computed from the Curr_Price_Matrix

def price_return(Curr_Price_Matrix):
    return_matrix = np.array([])
    for i in range(1,len(Curr_Price_Matrix)):
        return_calc = (Curr_Price_Matrix[i+1]-Curr_Price_Matrix[i])/Curr_Price_Matrix[i]
        np.append(return_matrix, return_calc)
    return return_matrix

    
    
    #logic_matrix nr.1

col: len(Curr_exchanges_volumes[0]) # this count how many columns has the matrix

for matrix in matrix_volumes_list:

        sum_array = np.sum(matrix[i:i+x,1:col], axis = 0]) # va ciclato su col this gives back an array with the sum of the array. We need to sum a certain range in the matrix.

        requirement = sum_array / matrix[i:i+x,1:col].sum()        #dividiamo l'array per la somma del totale dei volumi scambiati su tutti gli exchange. 

#questo ci da la % dei volumi scambiati per l'i-esimo coin su un dato exchange.

# ora facciamo il check per controllare che i volumi scambiati siano massimo su un exchange. Se un exchange possiede più dell'80% non sarà idoneo


if np.any(requirement) > 0.80:
    
    req1_matrix[i, j] = 0

else: 

    req1_matrix[i, j] = 1
