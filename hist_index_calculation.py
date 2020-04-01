# standard library import
import os.path
from pathlib import Path
import json
from datetime import datetime
import utils.calc as calc
from datetime import *
import time

# third party import
from pymongo import MongoClient
import numpy as np
import pandas as pd

# local import
import utils.mongo_setup as mongo
import utils.data_setup as data_setup
import utils.data_download as data_download

############################################# INITIAL SETTINGS #############################################

pair_array = ['gbp', 'usd', 'cad', 'jpy', 'eur']
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur'] 
Crypto_Asset = ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
# crypto complete ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
Exchanges = [ 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']
# exchange complete = [ 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']
#############################################################################################################

####################################### setup mongo connection ###################################

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
db = connection.index

# define database name and collection name
db_name = "index"
collection_converted_data = "CW_final_data"

# drop the pre-existing collection (if there is one)
db.crypto_price.drop()
db.crypto_volume.drop()
db.crypto_price_return.drop()
db.index_weights.drop()
db.index_level.drop()
db.index_EMWA.drop()
db.index_logic_matrix_one.drop()
db.index_logic_matrix_two.drop()
db.index_EMWA_logic_checked.drop()
db.index_divisor.drop()
db.index_divisor_reshaped.drop()
db.index_synth_matrix.drop()

# creating some the empty collections within the database index

# collection for index weights
db.index_weights.create_index([ ("id", -1) ])
collection_weights = db.index_weights
# collection for index level
db.index_level.create_index([ ("id", -1) ])
collection_index_level = db.index_level
# collection for crypto prices
db.crypto_price.create_index([ ("id", -1) ])
collection_price = db.crypto_price
# collection for crytpo volume
db.crypto_volume.create_index([ ("id", -1) ])
collection_volume = db.crypto_volume
# collection for price returns
db.crypto_price_return.create_index([ ("id", -1) ])
collection_price_ret = db.crypto_price_return
# collection for EMWA
db.index_EMWA.create_index([ ("id", -1) ])
collection_EMWA = db.index_EMWA
# collection for first logic matrix
db.index_logic_matrix_one.create_index([ ("id", -1) ])
collection_logic_one = db.index_logic_matrix_one
# collection for second logic matrix
db.index_logic_matrix_two.create_index([ ("id", -1) ])
collection_logic_two = db.index_logic_matrix_two
# collection for EMWA double checked with both logic matrix
db.index_EMWA_logic_checked.create_index([ ("id", -1) ])
collection_EMWA_check = db.index_EMWA_logic_checked
# collection for the divisor array
db.index_divisor.create_index([ ("id", -1) ])
collection_divisor = db.index_divisor
# collection for the reshaped divisor array
db.index_divisor_reshaped.create_index([ ("id", -1) ])
collection_divisor_reshaped = db.index_divisor_reshaped
# collection for the relative syntethic matrix
db.index_synth_matrix.create_index([ ("id", -1) ])
collection_relative_synth = db.index_synth_matrix




##################################### DATE SETTINGS ###################################################

# define the start date as MM-DD-YYYY
start_date = '01-01-2016'

# define today date as timestamp
today = datetime.now().strftime('%Y-%m-%d')
today_TS = int(datetime.strptime(today,'%Y-%m-%d').timestamp()) + 3600

# define end date as as MM-DD-YYYY
end_date = datetime.now().strftime('%m-%d-%Y')
# or
# end_date = 

# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = data_setup.timestamp_gen(start_date, end_date)

# define all the useful arrays containing the rebalance start date, stop date, board meeting date 
rebalance_start_date = calc.start_q('01-01-2016')
rebalance_start_date = calc.start_q_fix(rebalance_start_date) ####
rebalance_stop_date = calc.stop_q(rebalance_start_date)
board_date = calc.board_meeting_day()
board_date_eve = calc.day_before_board()
next_rebalance_date = calc.next_start()


# call the function that creates a object containing the couple of quarterly start-stop date
quarterly_date = calc.quarterly_period()

#############################################################################################################

##################################### MAIN PART ###################################################


# initialize the matrices that will contain the prices and volumes of all the cryptoasset
Crypto_Asset_Prices = np.matrix([])
Crypto_Asset_Volume = np.matrix([])
# initialize the matrix that will contain the complete first logic matrix
logic_matrix_one = np.matrix([])

for CryptoA in Crypto_Asset:

    print(CryptoA)
    # initialize useful matrices 
    currencypair_array = []
    Exchange_Price = np.matrix([])
    Exchange_Volume = np.matrix([])
    Ex_PriceVol = np.matrix([])

    # create the crypto-fiat strings useful to download from CW
    for pair in pair_array:
        currencypair_array.append(CryptoA.lower() + pair)

    for exchange in Exchanges:
        print(exchange)
        # initialize the matrices that will contain the data related to all currencypair for the single exchange
        Ccy_Pair_PriceVolume = np.matrix([])
        Ccy_Pair_Volume = np.matrix([])
        Ccy_Pair_Price = np.matrix([])
        
        for cp in currencypair_array:
            
            crypto = cp[:3]
            fiat_curr = cp[3:]
            print(cp)
  
            # defining the dictionary for the MongoDB query
            query_dict = {"Exchange" : exchange, "Pair": cp}
            # retriving the needed information on MongoDB
            matrix = mongo.query_mongo(db_name, collection_converted_data, query_dict)
     
            try:
                
                cp_matrix = matrix.to_numpy()

                # retrieves the wanted data from the matrix
                priceXvolume = cp_matrix[:,1] * cp_matrix[:,3] # 2 for crypto vol, 3 for pair volume
                volume = cp_matrix[:,3] # 2 for crypto vol, 3 for pair volume
                price = cp_matrix[:,1]

                # every "cp" the for loop adds a column in the matrices referred to the single "exchange"
                if Ccy_Pair_PriceVolume.size == 0:
                    Ccy_Pair_PriceVolume = priceXvolume
                    Ccy_Pair_Volume = volume
                else:
                    Ccy_Pair_PriceVolume = np.column_stack((Ccy_Pair_PriceVolume, priceXvolume))
                    Ccy_Pair_Volume = np.column_stack((Ccy_Pair_Volume, volume))
                    
            except AttributeError:

                pass

        # computing the volume weighted average price of the single exchange
        if Ccy_Pair_Volume.size != 0 and Ccy_Pair_Volume.size > reference_date_vector.size:
            
            PxV = Ccy_Pair_PriceVolume.sum(axis = 1)
            V = Ccy_Pair_Volume.sum(axis = 1)
            Ccy_Pair_Price = np.divide(PxV, V, out = np.zeros_like(V), where = V != 0.0 )
            
            # computing the total volume of the exchange
            Ccy_Pair_Volume = Ccy_Pair_Volume.sum(axis = 1) 
            # computing price X volume of the exchange
            Ccy_Pair_PxV = Ccy_Pair_Price * Ccy_Pair_Volume
        
        # case when just one crypto-fiat has the values in that exchange
        elif Ccy_Pair_Volume.size != 0 and Ccy_Pair_Volume.size == reference_date_vector.size:

            np.seterr(all = None, divide = 'warn')
            Ccy_Pair_Price = np.divide(Ccy_Pair_PriceVolume, Ccy_Pair_Volume, out = np.zeros_like(Ccy_Pair_Volume), where = Ccy_Pair_Volume != 0.0)
            Ccy_Pair_Price = np.nan_to_num(Ccy_Pair_Price)
            Ccy_Pair_PxV = Ccy_Pair_Price * Ccy_Pair_Volume

        else:

            Ccy_Pair_Price = np.array([])
            Ccy_Pair_Volume =  np.array([])
            Ccy_Pair_PxV = np.array([])

        # creating every loop the matrices containing the data referred to all the exchanges
        # Exchange_Price contains the crypto ("cp") prices in all the different Exchanges
        # Exchange_Volume contains the crypto ("cp") volume in all the different Exchanges
        if Exchange_Price.size == 0:

            if Ccy_Pair_Volume.size != 0:

                Exchange_Price = Ccy_Pair_Price
                Exchange_Volume = Ccy_Pair_Volume
                Ex_PriceVol = Ccy_Pair_PxV

            else:

                Exchange_Price = np.zeros(reference_date_vector.size)
                Exchange_Volume = np.zeros(reference_date_vector.size)
                Ex_PriceVol = np.zeros(reference_date_vector.size)

        else:

            if Ccy_Pair_Volume.size != 0:

                Exchange_Price = np.column_stack((Exchange_Price, Ccy_Pair_Price))
                Exchange_Volume = np.column_stack((Exchange_Volume, Ccy_Pair_Volume))
                Ex_PriceVol = np.column_stack((Ex_PriceVol, Ccy_Pair_PxV))

            else:

                Exchange_Price = np.column_stack((Exchange_Price, np.zeros(reference_date_vector.size)))
                Exchange_Volume = np.column_stack((Exchange_Volume, np.zeros(reference_date_vector.size)))
                Ex_PriceVol = np.column_stack((Ex_PriceVol, np.zeros(reference_date_vector.size)))

    # dataframes that contain volume and price of a single crytpo for all the exchanges.
    # if an exchange does not have value in the crypto will be insertd a column with zero
    Exchange_Vol_DF = pd.DataFrame(Exchange_Volume, columns = Exchanges)
    Exchange_Price_DF = pd.DataFrame(Exchange_Price, columns = Exchanges)

    # adding "Time" column to both Exchanges dataframe
    Exchange_Vol_DF['Time'] = reference_date_vector
    Exchange_Price_DF['Time'] = reference_date_vector

    # for each CryptoAsset compute the first logic array
    first_logic_array = calc.first_logic_matrix(Exchange_Vol_DF, Exchanges)

    # put the logic array into the logic matrix
    if logic_matrix_one.size == 0:
        logic_matrix_one = first_logic_array
    else:
        logic_matrix_one = np.column_stack((logic_matrix_one, first_logic_array))
        

    try:
        # computing the volume weighted average price of the single Crypto_Asset ("CryptoA") into a single vector
        Ex_price_num = Ex_PriceVol.sum(axis = 1)
        Ex_price_den = Exchange_Volume.sum(axis = 1) 
        Exchange_Price = np.divide(Ex_price_num, Ex_price_den, out = np.zeros_like(Ex_price_num), where = Ex_price_num!= 0.0)
        # computing the total volume  average price of the single Crypto_Asset ("CryptoA") into a single vector
        Exchange_Volume = Exchange_Volume.sum(axis = 1)
    except np.AxisError:
        Exchange_Price = Exchange_Price
        Exchange_Volume = Exchange_Volume

    # creating every loop the matrices containing the data referred to all the Cryptoassets
    # Crypto_Asset_Price contains the prices of all the cryptocurrencies
    # Crypto_Asset_Volume contains the volume of all the cryptocurrencies
    if Crypto_Asset_Prices.size == 0:
        Crypto_Asset_Prices = Exchange_Price
        Crypto_Asset_Volume = Exchange_Volume
    else:
        Crypto_Asset_Prices = np.column_stack((Crypto_Asset_Prices, Exchange_Price))
        Crypto_Asset_Volume = np.column_stack((Crypto_Asset_Volume, Exchange_Volume))

# turn prices and volumes into pandas dataframe
Crypto_Asset_Prices = pd.DataFrame(Crypto_Asset_Prices, columns = Crypto_Asset)
Crypto_Asset_Volume = pd.DataFrame(Crypto_Asset_Volume, columns = Crypto_Asset)

# compute the price returns over the defined period
price_ret = Crypto_Asset_Prices.pct_change()

# then add the 'Time' column
time_header = ['Time']
time_header.extend(Crypto_Asset) 
Crypto_Asset_Prices = pd.DataFrame(Crypto_Asset_Prices, columns = time_header)
Crypto_Asset_Prices['Time'] = reference_date_vector
Crypto_Asset_Volume = pd.DataFrame(Crypto_Asset_Volume, columns = time_header)
Crypto_Asset_Volume['Time'] = reference_date_vector
price_ret['Time'] = reference_date_vector

# turn the first logic matrix into a dataframe and add the 'Time' column
# containg the stop_date of each quarter as in "rebalance_stop_date" array
# the 'Time' column does not take into account the last value because it refers to a 
# period that has not been yet calculated (and will be this way until today == new quarter start_date)
first_logic_matrix = pd.DataFrame(logic_matrix_one, columns = Crypto_Asset)
first_logic_matrix['Time'] = rebalance_stop_date[0:len(rebalance_stop_date)]


# computing the Exponential Moving Weighted Average of the selected period
emwa_df = calc.emwa_crypto_volume(Crypto_Asset_Volume, Crypto_Asset, reference_date_vector, time_column = 'N')

# computing the second logic matrix
second_logic_matrix = calc.second_logic_matrix(Crypto_Asset_Volume, first_logic_matrix, Crypto_Asset, reference_date_vector, time_column = 'Y')

# computing the emwa checked with both the first and second logic matrices
double_checked_EMWA = calc.emwa_second_logic_check(first_logic_matrix, second_logic_matrix, emwa_df, reference_date_vector, Crypto_Asset, time_column = 'Y')

# computing the Weights that each CryptoAsset should have starting from each new quarter
# every weigfhts is computed in the period that goes from present quarter start_date to 
# present quarter board meeting date eve
weights_for_board = calc.quarter_weights(double_checked_EMWA, board_date_eve[1:], Crypto_Asset)

# compute the syntethic matrix and the rekative syntethic matrix
syntethic = calc.quarterly_synt_matrix(Crypto_Asset_Prices, weights_for_board, reference_date_vector, board_date_eve, Crypto_Asset)
syntethic_relative_matrix = calc.relative_syntethic_matrix(syntethic, Crypto_Asset)

# changing the "Time" column of the second logic matrix using the rebalance date
second_logic_matrix['Time'] = next_rebalance_date[1:]

# changing the "time" column of the weights in order to disply the application start date of each row
weights_for_period = weights_for_board 
weights_for_period['Time'] = next_rebalance_date[1:]

divisor_array = calc.divisor_adjustment(Crypto_Asset_Prices, weights_for_period, second_logic_matrix, Crypto_Asset, reference_date_vector)

reshaped_divisor = calc.divisor_reshape(divisor_array, reference_date_vector)

index_values = calc.index_level_calc(Crypto_Asset_Prices, syntethic_relative_matrix, divisor_array, reference_date_vector)

index_1000_base = calc.index_based(index_values)
#pd.set_option('display.max_rows', None)

####################################### MONGO DB UPLOADS ############################################
# creating the array with human readable Date
human_date = data_setup.timestamp_to_human(reference_date_vector)


# put the "weights" dataframe on MongoDB
weight_human_date = data_setup.timestamp_to_human(weights_for_board['Time'])
weights_for_board['Date'] = weight_human_date
weights_for_board = weights_for_board[['Date','ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC', 'Time']]
up_weights = weights_for_board.drop(columns = ['Time'])
up_weights = up_weights.to_dict(orient = 'records')     
collection_weights.insert_many(up_weights)

# put the "price_ret" dataframe on MongoDB
price_ret['Date'] = human_date
price_ret_up = price_ret.drop(columns = 'Time')
price_ret_up = price_ret_up[['Date','ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']]
price_ret_up = price_ret_up.to_dict(orient = 'records')  
collection_price_ret.insert_many(price_ret_up)

# put the "Crypto_Asset_Prices" dataframe on MongoDB
Crypto_Asset_Prices['Date'] = human_date
price_up = Crypto_Asset_Prices.drop(columns = 'Time')
price_up = price_up[['Date','ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']]
price_up = price_up.to_dict(orient = 'records')  
collection_price.insert_many(price_up)

# put the "Crypto_Asset_Volumes" dataframe on MongoDB
Crypto_Asset_Volume['Date'] = human_date
volume_up = Crypto_Asset_Volume.drop(columns = 'Time')
volume_up = volume_up[['Date','ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']]
volume_up = volume_up.to_dict(orient = 'records') 
collection_volume.insert_many(volume_up)

# put the EWMA dataframe on MongoDB
emwa_df['Date'] = human_date
emwa_df_up = emwa_df[['Date','ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']]
emwa_df_up = emwa_df_up.to_dict(orient = 'records') 
collection_EMWA.insert_many(emwa_df_up)

# put the double checked EMWA on MongoDB
double_checked_EMWA['Date'] = human_date
double_EMWA_up = double_checked_EMWA.drop(columns = 'Time')
double_EMWA_up = double_EMWA_up[['Date','ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']]
double_EMWA_up = double_EMWA_up.to_dict(orient = 'records')
collection_EMWA_check.insert_many(double_EMWA_up)

# put the index level on MongoDB
index_1000_base['Date'] = human_date
index_val_up = index_1000_base.drop(columns = 'Time')
index_val_up = index_val_up[['Date', 'Index Value']]
index_val_up = index_val_up.to_dict(orient = 'records')
collection_index_level.insert_many(index_val_up)

# put the divisor array on MongoDB
divisor_date = data_setup.timestamp_to_human(divisor_array['Time'])
divisor_array['Date'] = divisor_date
divisor_up = divisor_array.drop(columns = 'Time')
divisor_up = divisor_up[['Date', 'Divisor Value']]
divisor_up = divisor_up.to_dict(orient = 'records')
collection_divisor.insert_many(divisor_up)

# put the reshaped divisor array on MongoDB
reshaped_divisor_date = data_setup.timestamp_to_human(reshaped_divisor['Time'])
reshaped_divisor['Date'] = reshaped_divisor_date
reshaped_divisor_up = reshaped_divisor.drop(columns = 'Time')
reshaped_divisor_up = reshaped_divisor_up[['Date', 'Divisor Value']]
reshaped_divisor_up = reshaped_divisor_up.to_dict(orient = 'records')
collection_divisor_reshaped.insert_many(reshaped_divisor_up)

# put the first logic matrix on MongoDB
first_date = data_setup.timestamp_to_human(first_logic_matrix['Time'])
first_logic_matrix['Date'] = first_date
first_up = first_logic_matrix.drop(columns = 'Time')
first_up = first_up[['Date','ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']]
first_up = first_up.to_dict(orient = 'records') 
collection_logic_one.insert_many(first_up)

# put the second logic matrix on MongoDB
second_date = data_setup.timestamp_to_human(second_logic_matrix['Time'])
second_logic_matrix['Date'] = second_date
second_up = second_logic_matrix.drop(columns = 'Time')
second_up = second_up[['Date','ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']]
second_up = second_up.to_dict(orient = 'records') 
collection_logic_two.insert_many(second_up)

# put the relative synth matrix on MongoDB
syntethic_relative_matrix['Date'] = human_date
synth_up = syntethic_relative_matrix[['Date','ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']]
synth_up = synth_up.to_dict(orient = 'records') 
collection_relative_synth.insert_many(synth_up)

######## some printing ##########
print('Crypto_Asset_Prices')
print(Crypto_Asset_Prices)
print('Crypto_Asset_Volume')
print(Crypto_Asset_Volume)
print('Price Returns')
print(price_ret)
print('EMWA DataFrame')
print(emwa_df)
print('WEIGHTS for board')
print(weights_for_board)
print('Syntethic relative matrix')
print(syntethic_relative_matrix)
print('index value')
print(index_values)
#################################







