import numpy as np
import os.path
from pathlib import Path
import json
from datetime import datetime
import utils.data_setup as data_setup
import utils.data_download as data_download
import utils.calc as calc
import pandas as pd
from datetime import *
import time

############################################# INITIAL SETTINGS #############################################

# we choose just a couple of crypto and fiat
# other fiat and crypto needs more test due to historical series incompletness
#crypto = ['btc', 'eth', 'ltc']
pair_array = ['gbp', 'usd'] 
Crypto_Asset = ['BTC', 'ETH', 'LTC']

# we use all the xchanges except for Kraken that needs some more test in order to be introduced without error
Exchanges = [ 'coinbase-pro', 'bitflyer', 'poloniex', 'bitstamp', 'gemini', 'bittrex'] #'kraken'
#############################################################################################################

##################################### DATE SETTINGS ###################################################

# we choose a start date that ensures the presence of multiple quarter period and avoids some problem 
# related to older date that has to be tested more deeply
start_date = '01-01-2019'

# define today date as timestamp
today = datetime.now().strftime('%Y-%m-%d')
today_TS = int(datetime.strptime(today,'%Y-%m-%d').timestamp()) + 3600

# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = data_setup.timestamp_gen('01-01-2019')

# define all the useful arrays containing the rebalance start date, stop date, board meeting date 
# we use the complete set of date (from 01-01-2016) to today, the code will take care of that
rebalance_start_date = calc.start_q('01-01-2016')
rebalance_stop_date = calc.stop_q(rebalance_start_date)
board_date = calc.board_meeting_day()
board_date_eve = calc.day_before_board()

# call the function that creates a object containing the couple of quarterly start-stop date
quarterly_date = calc.quarterly_period()

#############################################################################################################

##################################### MAIN PART ###################################################
# due to the required amount of time  (A LOT) for the rates download from ECB and relative conversion off all
# the values (both volumes and prices) into USD, we choose to turn off the functions that allow the conversion.
# this choice affects teh result, but it is just temporary because we are working on the moving towards Mongo DB.


# initialize the matrices that will contain the prices and volumes of all the cryptoasset
Crypto_Asset_Prices = np.matrix([])
Crypto_Asset_Volume = np.matrix([])
# initialize the matrix that will contain the complete first logic matrix
logic_matrix_one = np.matrix([])

for CryptoA in Crypto_Asset:

    # initialize useful matrices 
    currencypair_array = []
    Exchange_Price = np.matrix([])
    Exchange_Volume = np.matrix([])
    Ex_PriceVol = np.matrix([])

    # create the crypto-fiat strings useful to download from CW
    for pair in pair_array:
        currencypair_array.append(CryptoA.lower() + pair)

    for exchange in Exchanges:
        
        # initialize the matrices that will contain the data related to all currencypair for the single exchange
        Ccy_Pair_PriceVolume = np.matrix([])
        Ccy_Pair_Volume = np.matrix([])
        Ccy_Pair_Price = np.matrix([])
        
        for cp in currencypair_array:

            # create the    matrix for the single currency_pair connecting to CryptoWatch website
            matrix = data_download.CW_data_reader(exchange, cp, start_date)

            # checking if the matrix is not empty
            if data_setup.Check_null(matrix) == False:

                # checking if the matrix has missing data and if ever fixing it
                if matrix.shape[0] != reference_date_vector.size:

                    matrix = data_setup.fix_missing(matrix, exchange, CryptoA, pair, start_date)

                ######## TEMPORARILY TURNED OFF ########################################
                ## changing the "fiat" values into USD (Close Price and Volume)
                #matrix= data_setup.CW_data_setup(matrix, rates, pair)
                ####################################################################
                cp_matrix = matrix.to_numpy()

                # retrieves the wanted data from the matrix
                priceXvolume = cp_matrix[:,1] * cp_matrix[:,2]
                volume = cp_matrix[:,2]
                price = cp_matrix[:,1]
                priceXvolume = cp_matrix[:,1] * cp_matrix[:,2]
                volume = cp_matrix[:,2]
                price = cp_matrix[:,1]

                # every "cp" the for loop adds a column in the matrices referred to the single "exchange"
                if Ccy_Pair_PriceVolume.size == 0:
                    Ccy_Pair_PriceVolume = priceXvolume
                    Ccy_Pair_Volume = volume
                else:
                    Ccy_Pair_PriceVolume = np.column_stack((Ccy_Pair_PriceVolume, priceXvolume))
                    Ccy_Pair_Volume = np.column_stack((Ccy_Pair_Volume, volume))


        # computing the volume weighted average price of the single exchange
        if Ccy_Pair_Volume.size != 0 and Ccy_Pair_Volume.size > reference_date_vector.size:
            
            Ccy_Pair_Price = Ccy_Pair_PriceVolume.sum(axis = 1) / Ccy_Pair_Volume.sum(axis = 1)  
            # computing the total volume of the exchange
            Ccy_Pair_Volume = Ccy_Pair_Volume.sum(axis = 1) 
            # computing price X volume of the exchange
            Ccy_Pair_PxV = Ccy_Pair_Price * Ccy_Pair_Volume
        
        elif Ccy_Pair_Volume.size != 0 and Ccy_Pair_Volume.size == reference_date_vector.size:

            Ccy_Pair_Price = Ccy_Pair_PriceVolume / Ccy_Pair_Volume
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
        Exchange_Price = Ex_PriceVol.sum(axis = 1) / Exchange_Volume.sum(axis = 1) 
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
# then add the 'Time' column
time_header = ['Time']
time_header.extend(Crypto_Asset) 
Crypto_Asset_Prices = pd.DataFrame(Crypto_Asset_Prices, columns = time_header)
Crypto_Asset_Prices['Time'] = reference_date_vector
Crypto_Asset_Volume = pd.DataFrame(Crypto_Asset_Volume, columns = time_header)
Crypto_Asset_Volume['Time'] = reference_date_vector

# compute the price returns over the defined period
price_ret = Crypto_Asset_Prices.pct_change()

# turn the first logic matrix into a dataframe and add the 'Time' column
# containg the stop_date of each quarter as in "rebalance_stop_date" array
# the 'Time' column does not take into account the last value because it refers to a 
# period that has not been yet calculated (and will be this way until today == new quarter start_date)
first_logic_matrix = pd.DataFrame(logic_matrix_one, columns = Crypto_Asset)
first_logic_matrix['Time'] = rebalance_stop_date[0:len(rebalance_stop_date) - 1]


# computing the Exponential Moving Weighted Average of the selected period
emwa_df = calc.emwa_crypto_volume(Crypto_Asset_Volume, Crypto_Asset, reference_date_vector, time_column = 'N')

# computing the second logic matrix
second_logic_matrix = calc.second_logic_matrix(Crypto_Asset_Volume, first_logic_matrix, Crypto_Asset, reference_date_vector, time_column = 'Y')

# computing the emwa checked with both the first and second logic matrices
double_checked_EMWA = calc.emwa_second_logic_check(first_logic_matrix, second_logic_matrix, emwa_df, reference_date_vector, Crypto_Asset, time_column = 'Y')

# computing the Weights that each CryptoAsset should have starting from each new quarter
# every weigfhts is computed in the period that goes from present quarter start_date to 
# present quarter board meeting date eve
weights = calc.quarter_weights(double_checked_EMWA, board_date_eve, Crypto_Asset)


syntethic = calc.quarterly_synt_matrix(Crypto_Asset_Prices, weights, reference_date_vector, board_date_eve, Crypto_Asset)




######## some printing ##########
print('Crypto_Asset_Prices')
print(Crypto_Asset_Prices)
print('Crypto_Asset_Volume')
print(Crypto_Asset_Volume)
print('Price Returns')
print(price_ret)
print('EMWA DataFrame')
print(emwa_df)
print('WEIGHTS')
print(weights)
print('Syntethic matrix')
print(syntethic)
#################################






