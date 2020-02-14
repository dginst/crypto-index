def ECB_setup (key_curr_vector, Start_Period, End_Period, timeST = 'N'):

    # defining the array of date to be used
    date = date_array_gen(Start_Period, End_Period, timeST = 'N')
    # defining the headers of the returning data frame
    header = ['Date', 'Currency', 'Rate']

    database = 'index'
    collection = 'ecb_raw'
    
    # for each date in "date" array the funcion retrieves data from ECB website
    # and append the result in the returning matrix
    Exchange_Matrix = np.array([])
    for i, single_date in enumerate(date):
        query = {'Time': date[i]}
        # retrieving data from ECB website
        single_date_ex_matrix = mongo.query_mongo(database,collection, query)
        
        # check if the API actually returns values 
        if Check_null(single_date_ex_matrix) == False:

            date_arr = np.full(len(key_curr_vector),single_date)
            # creating the array with 'XXX/USD' format
            curr_arr = single_date_ex_matrix['CURRENCY'] + '/USD'
            curr_arr = np.where(curr_arr == 'USD/USD', 'EUR/USD', curr_arr)
            # creating the array with rate values USD based
            # since ECB displays rate EUR based some changes needs to be done
            rate_arr = single_date_ex_matrix['USD based rate']
            rate_arr = np.where(rate_arr == 1.000000, 1/single_date_ex_matrix['OBS_VALUE'][0], rate_arr)

            # stacking the array together
            array = np.column_stack((date_arr, curr_arr, rate_arr))

            # filling the return matrix
            if Exchange_Matrix.size == 0:
                Exchange_Matrix = array
            else:
                Exchange_Matrix = np.row_stack((Exchange_Matrix, array))

        # if the first API call returns an empty matrix, function will takes values of the
        # last useful day        
        else:

            exception_date = datetime.strptime(date[i], '%Y-%m-%d') - timedelta(days = 1)
            date_str = exception_date.strftime('%Y-%m-%d')            
            exception_matrix = data_download.ECB_rates_extractor(key_curr_vector, date_str)

            while Check_null(exception_matrix) != False:

                exception_date = exception_date - timedelta(days = 1)
                date_str = exception_date.strftime('%Y-%m-%d') 
                exception_matrix = data_download.ECB_rates_extractor(key_curr_vector, date_str)

            date_arr = np.full(len(key_curr_vector),single_date)
            curr_arr = exception_matrix['CURRENCY'] + '/USD'
            curr_arr = np.where(curr_arr == 'USD/USD', 'EUR/USD', curr_arr)
            rate_arr = exception_matrix['USD based rate']
            rate_arr = np.where(rate_arr == 1.000000, 1/exception_matrix['OBS_VALUE'][0], rate_arr)
            array = np.column_stack((date_arr, curr_arr, rate_arr))

            if Exchange_Matrix.size == 0:
                Exchange_Matrix = array
            else:
                Exchange_Matrix = np.row_stack((Exchange_Matrix, array))
    
    if timeST != 'N':

        for j, element in enumerate(Exchange_Matrix[:,0]):

            to_date = datetime.strptime(element, '%Y-%m-%d')
            time_stamp = datetime.timestamp(to_date) + 3600
            Exchange_Matrix[j,0] = int(time_stamp)


    return pd.DataFrame(Exchange_Matrix, columns = header)



# function returns the Data Frame relative to a specified exchange/crypto/pair
# with the "pair" value converted in USD, more specifically converts the columns
# 'Close Price' and 'Pair Volume' into USD
# function takes as input:
# CW_matrix = CryptoWatch dataframe to be changed
# Ex_Rate_matrix = data frame of ECB exchange rates
# currency = string that specify the currency of CW_matrix (EUR, CAD, GBP,...)

def CW_data_setup (CW_matrix, Ex_Rate_matrix, currency):

    currency = currency.upper()

    if currency != 'USD':
            
        ex_curr = currency + '/USD'

        for i in range ((CW_matrix.shape[0])):
            
            date = CW_matrix['Time'][i]
            rate = Ex_Rate_matrix[(Ex_Rate_matrix['Date'] == date) & (Ex_Rate_matrix['Currency'] == ex_curr)]
            CW_matrix['Close Price'][i] = int(CW_matrix['Close Price'][i] / rate['Rate'])
            CW_matrix['Pair Volume'][i] = int(CW_matrix['Pair Volume'][i] / rate['Rate'])
    
    else:
        CW_matrix = CW_matrix

    return CW_matrix