import numpy as np
import os.path
from pathlib import Path
import json
from datetime import datetime
import utils.data_setup as data_setup
import utils.data_download as data_download

for pair in ['btc']:
	currencypair_array = [""+pair+"usd", ""+pair+"jpy"] #, ""+pair+"cad", ""+pair+"gbp", ""+pair+"usdt", ""+pair+"usdc", ""+pair+"jpy"]
Crypto_Asset = ['BTC']
Exchanges = ['bitfinex','bitflyer']
start_date = '01-01-2020'

reference_date_vector = np.array(data_setup.date_array_gen(start_date))
Crypto_Asset_Prices = np.matrix([])
Crypto_Asset_Volume = np.matrix([])

for CryptoA in Crypto_Asset:

    Exchange_Price = np.matrix([])
    Exchange_Volume = np.matrix([])
    Ex_PriceVol = np.matrix([])

    for exchange in Exchanges:
        
        # initialize the matrices that will contain the data related to all currencypair for the single exchange
        Ccy_Pair_PriceVolume = np.matrix([])
        Ccy_Pair_Volume = np.matrix([])
        
        for cp in currencypair_array:

            crypto = cp[:3]
            pair = cp[3:]
            # create the matrix for the single currency_pair connecting to CryptoWatch website
            matrix=data_download.CW_data_reader(exchange, cp, start_date)

            # creates the to-be matrix of the cp assigning the reference date vector as first column
            cp_matrix = reference_date_vector

            # checking if the matrix is not empty
            if data_setup.Check_null(matrix) == False:

                # checking if the matrix has missing data and if ever fixing it
                if matrix.size != reference_date_vector.size:
                                    
                    for i in range(7):
                        fixed_vector = data_setup.fix_missing(matrix, exchange, crypto, pair, i+1, start_date)
                        cp_matrix = np.column_stack((cp_matrix, fixed_vector))
                    
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
                    
       
        # computing the volume weighted average price of the single exchange
        Ccy_Pair_Price = Ccy_Pair_PriceVolume.sum(axis = 1) / Ccy_Pair_Volume.sum(axis = 1)  
        # computing the total volume of the exchange
        Ccy_Pair_Volume = Ccy_Pair_Volume.sum(axis = 1) 
        # computing price X volume of the exchange
        Ccy_Pair_PxV = Ccy_Pair_Price * Ccy_Pair_Volume

        # creating every loop the matrices containing the data referred to all the exchanges
        # Exchange_Price contains the crypto ("cp") prices in all the different Exchanges
        # Exchange_Volume contains the crypto ("cp") volume in all the different Exchanges
        if Exchange_Price.size == 0:
            Exchange_Price = Ccy_Pair_Price
            Exchange_Volume = Ccy_Pair_Volume
            Ex_PriceVol = Ccy_Pair_PxV
        else:
            Exchange_Price = np.column_stack((Exchange_Price, Ccy_Pair_Price))
            Exchange_Volume = np.column_stack((Exchange_Volume, Ccy_Pair_Volume))
            Ex_PriceVol = np.column_stack((Ex_PriceVol, Ccy_Pair_PxV))
        
    # computing the volume weighted average price of the single Crypto_Asset ("CryptoA") into a single vector
    Exchange_Price = Ex_PriceVol.sum(axis = 1) / Exchange_Volume.sum(axis = 1) 
    # computing the total volume  average price of the single Crypto_Asset ("CryptoA") into a single vector
    Exchange_Volume = Exchange_Volume.sum(axis = 1)

    # creating every loop the matrices containing the data referred to all the Cryptoassets
    # Crypto_Asset_Price contains the prices of all the cryptocurrencies
    # Crypto_Asset_Volume contains the volume of all the cryptocurrencies
    if Crypto_Asset_Prices.size == 0:
        Crypto_Asset_Prices = Exchange_Price
        Crypto_Asset_Volume = Exchange_Volume
    else:
        Crypto_Asset_Prices = np.column_stack((Crypto_Asset_Prices, Exchange_Price))
        Crypto_Asset_Volume = np.column_stack((Crypto_Asset_Volume, Exchange_Volume))


