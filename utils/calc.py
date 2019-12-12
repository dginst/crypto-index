# Return the Initial Divisor for the index. It identifies the position of the initial date in the matrix. 
# At the moment the initial date is 2016/01/01
# where:
# sr = second requirement matrix, composed by 0 if negative, 1 if positive
# p = final price matrix of each currency 
# sm = synthetic market cap derived weight

def calc_initial_divisor(initial_timestamp = 1451606400 ):

    result = np.where( p == initial_timestamp)
    coord = list(zip(result[0], result[1])) 
    
    Initial_Divisor =  (p[coord[0]] * v[coord[0]-x] * sr[coord[0]-x]).sum() / 1000
    
    
            
    return Initial_Divisor

calc_initial_divisor()




 # Return the divisor for each day.
 # sr = second requirement matrix, composed by 0 if negativa, 1 if positive
 # final_price matrix of each currency
 # final_volume matrix of each currency

 
divisor_array = np.array(calc_Initial_divisor())

def divisor_adjustment():
    
    for i in range(len(sr)):
        
        if sr[i].sum() == sr[i-1].sum():

            divisor_list.append(divisor_list[i-1])

        else:
            
            append(divisor_array,divisor_list[i-1]*( p[i] * v[i] * sr[i]).sum() / ( p[i-1] * v[i-1] * sr[i-1]).sum())


    return divisor_array


 # Return the daily level of the Index
 # where:
 # sr = second requirement matrix, composed by 0 if negativa, 1 if positive
 # p = final price matrix of each currency
 # f = final_volume matrix of each currency
 # sm = synthetic market cap derived weight

index_level = np.array([])

def index_level_calc():

    for i in range(len(sr)):
        
        append(index_level,(p[i] * sm[i] * sr[i] ).sum() / divisor_adjustment()[i])

    return index_level