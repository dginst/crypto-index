import numpy as np
import os.path
from pathlib import Path
import json
from pymongo import MongoClient
from datetime import datetime
import utils.data_setup as data_setup
import utils.data_download as data_download
import utils.calc as calc
import pandas as pd
from datetime import *
import time
import mongoconn as mongo

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
db.rawdata.create_index([ ("id", -1) ])
#creating the empty collection rawdata within the database index
collection_raw = db.rawdata
collection_clean = db.cleandata
collection_ECB_raw = db.ecb_raw


crypto = ['btc', 'eth']
pair_array = ['jpy', 'gbp', 'usd'] #, 'eur', 'cad', 'usdt', 'usdc'


Crypto_Asset = ['BTC', 'ETH']

Exchanges = ['kraken', 'bitflyer', 'poloniex' ] #, 'bitstamp','bittrex','coinbase-pro','gemini']
start_date = '01-01-2020'
today = datetime.now().strftime('%Y-%m-%d')
today_TS = int(datetime.strptime(today,'%Y-%m-%d').timestamp()) + 3600

reference_date_vector = np.array(data_setup.date_array_gen(start_date, timeST='Y'))

##
# define the array containing the rebalance start date
rebalance_start_date = calc.start_q('01-01-2016')
rebalance_stop_date = calc.stop_q(rebalance_start_date)

board_date = calc.board_meeting_day()
board_date_eve = calc.day_before_board()
##

# potrei dire che si attiva il calcolo del rebalance solo quando (today in rebalance start date o end date
# quidi solo in quella occasione cambia il vettore di 0 e 1


print(reference_date_vector)

key= ['USD', 'GBP', 'CAD', 'JPY']
rates = data_setup.ECB_setup(key, '2020-01-01', today, timeST='Y')
#print(rates)



Crypto_Asset_Prices = np.matrix([])
Crypto_Asset_Volume = np.matrix([])

for CryptoA in Crypto_Asset:
    print(CryptoA)
    currencypair_array = []
    Exchange_Price = np.matrix([])
    Exchange_Volume = np.matrix([])
    Ex_PriceVol = np.matrix([])
    first_logic_matrix = np.matrix([])
    new_first_logic_matrix = np.matrix([])
    for i in pair_array:
        currencypair_array.append(CryptoA.lower() + i)

    for exchange in Exchanges:
        
        # initialize the matrices that will contain the data related to all currencypair for the single exchange
        Ccy_Pair_PriceVolume = np.matrix([])
        Ccy_Pair_Volume = np.matrix([])
        Ccy_Pair_Price = np.matrix([])
        
        for cp in currencypair_array:

            crypto = cp[:3]
            pair = cp[3:]
            # create the matrix for the single currency_pair connecting to CryptoWatch website
            mongo.CW_data_reader(exchange, cp, start_date)
            matrix = mongo.query_raw_mongo(cp)

            

            # creates the to-be matrix of the cp assigning the reference date vector as first column
            cp_matrix = reference_date_vector

            # checking if the matrix is not empty
            if data_setup.Check_null(matrix) == False:

                # checking if the matrix has missing data and if ever fixing it
                if matrix.shape[0] != reference_date_vector.size:

                    matrix= data_setup.fix_missing(matrix, exchange, crypto, pair, start_date)


                # changing the "fiat" values into USD (Close Price and Volume)
                matrix= data_setup.CW_data_setup(matrix, rates, pair)
                cp_matrix = matrix.to_numpy()

                # then retrieves the wanted data 
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
    print('this is the single crytpo matrix fr every exch' )
    Exchange_Vol_DF = pd.DataFrame(Exchange_Volume, columns = Exchanges)
    Exchange_Price_DF = pd.DataFrame(Exchange_Price, columns = Exchanges)

    # adding "Time" column to both Exchanges dataframe
    Exchange_Vol_DF['Time'] = reference_date_vector
    Exchange_Price_DF['Time'] = reference_date_vector

    # check if today is a rebalance date and then compute the new logic matrix 1
    if today_TS in board_date_eve:

        start_calc = calc.minus_nearer_date(rebalance_start_date, today_TS)
        crypto_reb_perc = calc.perc_volumes_per_exchange(Exchange_Vol_DF, Exchanges, start_calc)
        if new_first_logic_matrix.size == 0:
            new_first_logic_matrix = crypto_reb_perc
        else:
            new_first_logic_matrix = np.column_stack((new_first_logic_matrix, crypto_reb_perc))






##### capire come inserire il fatto che, suki dati giornalieri deve consiederae solo l'ultima start date
# sui dati storici tutti



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


if today_TS in rebalance_start_date:

    first_logic_matrix = new_first_logic_matrix


Crypto_Asset_Prices = pd.DataFrame(Crypto_Asset_Prices, columns = Crypto_Asset)
Crypto_Asset_Volume = pd.DataFrame(Crypto_Asset_Volume, columns = Crypto_Asset)
print(Crypto_Asset_Prices)
price_ret = Crypto_Asset_Prices.pct_change()
print(price_ret)