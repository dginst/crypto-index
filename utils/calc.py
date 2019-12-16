import numpy as np

# Return the Initial Divisor for the index. It identifies the position of the initial date in the matrix. 
# At the moment the initial date is 2016/01/01 or 1451606400 as timestamp
# where:
# logic_matrix = second requirement matrix, composed by 0 if negative, 1 if positive
# Currency_Price_Matrix = final price matrix of each currency 
# sm = synthetic market cap derived weight

def calc_initial_divisor(Currency_Price_Matrix, logic_matrix, sm, initial_date='01-01-2016' ):
    initial_date = datetime.strptime(initial_date, '%m-%d-%Y')
    initial_timestamp=str(int(time.mktime(initial_date.timetuple())))
    index = np.where( Currency_Price_Matrix == initial_timestamp)
    index_tuple = list(zip(index[0], index[1])) 
    Initial_Divisor =  (Currency_Price_Matrix[index_tuple[0]] * sm[index_tuple[0]] * logic_matrix[index_tuple[0]]).sum() / 1000
    return Initial_Divisor



 # Return an array with the divisor for each day.
 # logic_matrix = second requirement matrix, composed by 0 if negativa, 1 if positive
 # final_price matrix of each currency
 # final_volume matrix of each currency

def divisor_adjustment(Currency_Price_Matrix, Currency_Volume_Matrix, logic_matrix, sm, initial_date='01-01-2016'):
    divisor_array=np.array(calc_initial_divisor(Currency_Price_Matrix, logic_matrix, sm, initial_date))
    for i in range(len(logic_matrix)-1):
        if logic_matrix[i+1].sum() == logic_matrix[i].sum():
            divisor_array=np.append(divisor_array, divisor_array[i])
        else:
            new_divisor=divisor_array[i]*( Currency_Price_Matrix[i+1] * Currency_Volume_Matrix[i+1] * logic_matrix[i+1]).sum() / ( Currency_Price_Matrix[i+1] * Currency_Volume_Matrix[i] * logic_matrix[i]).sum()
            divisor_array=np.append(divisor_array,new_divisor)
    return divisor_array
 
 

# Return an array of the daily level of the Index
 # where:
 # logic_matrix = second requirement matrix, composed by 0 if negativa, 1 if positive
 # Currency_Price_Matrix = final price matrix of each currency
 # f = final_volume matrix of each currency
 # sm = synthetic market cap derived weight

def index_level_calc(Currency_Price_Matrix, Currency_Volume_Matrix, logic_matrix, sm, initial_date='01-01-2016'):
    index_level = np.array([])
    divisor_array=divisor_adjustment(Currency_Price_Matrix, Currency_Volume_Matrix, logic_matrix, sm, initial_date)
    for i in range(len(logic_matrix)):
        new_index_item=(Currency_Price_Matrix[i] * sm[i] * logic_matrix[i]).sum() / divisor_array[i]
        index_level=np.append(index_level,new_index_item)
    return index_level



# Return an array with the value of the smoothing factor for 90 days (0-89)
# is utilized to calc the EWMA(exponential weighted moving average)

def smoothing_factor():

    lamba1 = 0.94
    i =  list(range(1,90))        
    i = np.array(i)

    w = []
    for ind in range(0,len(i)):
        w.append((1-lamba1)*lamba1**i[ind])
    w = np.array(w)
    
    return w


#Return the 90-days EWMA volume for each currency.

def EMWA_calc(matrix):
    emwa_gen=np.array([])
    for col_id in [0,1,2,3,4,5]:
        EWMA_coin1 = []
        c = 0
        n = 90
        while c < (len(matrix)-90) and n < len(matrix):
            c += 1
            n += 1
            EWMA_coin1.append((matrix[c:n,col_id]*smoothing_factor()).sum())    
        if emwa_gen.size==0:
            emwa_gen = np.array(EWMA_coin1)
        else:
            emwa_gen= np.column_stack((emwa_gen,EWMA_coin1))       
    return emwa_gen


# creating a vector with the total volumes for each day
total_EMWA_volume = []
for i in range(0,len(EMWA_calc(data))):
     total_EMWA_volume.append(data2[i].sum())    
total_EMWA_volume = np.array(a)

# creating a matrix of EMWA_weights.
 EMWA_weights_matrix = (EMWA_calc(data)* first_requirement_matrix) / a[:, None]

# creating the return matrix

return_matrix = []

for i in range(1,len(final_prices_matrix)):
    return_matrix.append((p[i]-p[i-1])/p[i-1])

return_matrix = np.array(return_matrix)