import numpy as np
import os.path
from pathlib import Path
import json
from datetime import datetime
import utils.data_setup as data_setup
import utils.data_download as data_download
import pandas as pd
from datetime import *
import time


crypto = ['btc', 'eth']
pair_array = ['jpy', 'gbp', 'usd'] #, 'eur', 'cad', 'usdt', 'usdc'


Crypto_Asset = ['BTC', 'ETH']
Exchanges = ['bitfinex','bitflyer', 'poloniex', 'bitstamp','bittrex','coinbase-pro','gemini']#,'kraken']
start_date = '01-01-2020'
reference_date_vector = np.array(data_setup.date_array_gen(start_date, timeST='Y'))


key= ['USD', 'GBP', 'CAD', 'JPY']
rates = data_setup.ECB_setup(key, '2020-01-01', '2020-01-15', timeST='Y')
print(rates)



Crypto_Asset_Prices = np.matrix([])
Crypto_Asset_Volume = np.matrix([])

for CryptoA in Crypto_Asset:
    print(CryptoA)
    currencypair_array = []
    Exchange_Price = np.matrix([])
    Exchange_Volume = np.matrix([])
    Ex_PriceVol = np.matrix([])
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
            matrix=data_download.CW_data_reader(exchange, cp, start_date)
            print(exchange)
            print(cp)
            print(matrix.shape[0])
            print(matrix)
            # changing the "fiat" values into USD (Close Price and Volume)
            matrix= data_setup.CW_data_setup(matrix, rates, pair)
            print(matrix)

            # creates the to-be matrix of the cp assigning the reference date vector as first column
            cp_matrix = reference_date_vector

            # checking if the matrix is not empty
            if data_setup.Check_null(matrix) == False:

                # checking if the matrix has missing data and if ever fixing it
                if matrix.shape[0] != reference_date_vector.size:
                                    
                    for i in range(7):
                        fixed_vector = data_setup.fix_missing(matrix, exchange, crypto, pair, i+1, start_date)
                        cp_matrix = np.column_stack((cp_matrix, fixed_vector))

                else:
                    cp_matrix = matrix.to_numpy()

                # then retrieves the wanted data 
                priceXvolume = cp_matrix[:,4] * cp_matrix[:,6]
                volume = cp_matrix[:,6]
                price = cp_matrix[:,4]

                # every "cp" the for loop adds a column in the matrices referred to the single "exchange"
                if Ccy_Pair_PriceVolume.size == 0:
                    Ccy_Pair_PriceVolume = priceXvolume
                    Ccy_Pair_Volume = volume
                else:
                    Ccy_Pair_PriceVolume = np.column_stack((Ccy_Pair_PriceVolume, priceXvolume))
                    Ccy_Pair_Volume = np.column_stack((Ccy_Pair_Volume, volume))

        print('Ccy_Pair_PriceVolume')           
        print(Ccy_Pair_PriceVolume)
        print('Ccy_Pair_Volume')  
        print( Ccy_Pair_Volume)
        print( Ccy_Pair_Volume.shape)

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
        print('Ccy_Pair_Price')
        print(Ccy_Pair_Price)
        #Ccy_Pair_PxV = Ccy_Pair_Price * Ccy_Pair_Volume
        print(Ccy_Pair_PxV)
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
    print(Exchange_Vol_DF)
    try:
        # computing the volume weighted average price of the single Crypto_Asset ("CryptoA") into a single vector
        Exchange_Price = Ex_PriceVol.sum(axis = 1) / Exchange_Volume.sum(axis = 1) 
        # computing the total volume  average price of the single Crypto_Asset ("CryptoA") into a single vector
        Exchange_Volume = Exchange_Volume.sum(axis = 1)
    except np.AxisError:
        Exchange_Price = Exchange_Price
        Exchange_Volume = Exchange_Volume
    print(Exchange_Volume)
    # creating every loop the matrices containing the data referred to all the Cryptoassets
    # Crypto_Asset_Price contains the prices of all the cryptocurrencies
    # Crypto_Asset_Volume contains the volume of all the cryptocurrencies
    if Crypto_Asset_Prices.size == 0:
        Crypto_Asset_Prices = Exchange_Price
        Crypto_Asset_Volume = Exchange_Volume
    else:
        Crypto_Asset_Prices = np.column_stack((Crypto_Asset_Prices, Exchange_Price))
        Crypto_Asset_Volume = np.column_stack((Crypto_Asset_Volume, Exchange_Volume))

# Crypto_Asset_Prices = pd.DataFrame(Crypto_Asset_Prices, columns = Crypto_Asset)
# Crypto_Asset_Volume = pd.DataFrame(Crypto_Asset_Volume, columns = Crypto_Asset)
# print(Crypto_Asset_Prices)