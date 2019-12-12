import numpy as np
import os.path
from pathlib import Path
import json
from datetime import datetime
import data_setup

for pair in ['btc']:
	currencypair_array = [""+pair+"usd", ""+pair+"jpy"] #, ""+pair+"cad", ""+pair+"gbp", ""+pair+"usdt", ""+pair+"usdc", ""+pair+"jpy"]
CryptoAsset = ['BTC']
exchanges = ['bitfinex','bitflyer']
Crypto_Asset_Prices=np.matrix([])
Crypto_Asset_Volume=np.matrix([])

for CryptoA in CryptoAsset:

    Exchange_Price=np.matrix([])
    Exchange_Volume=np.matrix([])
    for elements in exchanges:

        CCy_Pair_Price=np.matrix([])
        Ccy_Pair_PriceVolume=np.matrix([])
        Ccy_Pair_Volume=np.matrix([])
        for ccy in currencypair_array:
            Pair_ccy=ccy[3:]
            Crypto_ccy=ccy[:3]
            path_name=os.path.join("C:\\","Users","fcodega","hello")
            file_name=""+elements+"_"+ccy+".json"
            path=os.path.join(path_name, str(file_name))
            print(path)
            matrix=np.array(data_setup.json_to_matrix(path))
            #print(matrix.shape)
            if data_setup.Check_null(matrix)==False:
                priceXvolume=matrix[:,4]*matrix[:,6]
                volume=matrix[:,6]
                if Ccy_Pair_PriceVolume.size==0:
                    Ccy_Pair_PriceVolume=priceXvolume
                    Ccy_Pair_Volume=volume
                else:
                    Ccy_Pair_PriceVolume=np.column_stack((Ccy_Pair_PriceVolume,priceXvolume))
                    Ccy_Pair_Volume=np.column_stack((Ccy_Pair_Volume,volume))
        CCy_Pair_Price=Ccy_Pair_PriceVolume.sum(axis=1)/Ccy_Pair_Volume.sum(axis=1) #volume weighted average price of the exchange 
        Ccy_Pair_Volume=Ccy_Pair_Volume.sum(axis=1) #total volume of he exchange
        if Exchange_Price.size==0:
            Exchange_Price=CCy_Pair_Price
            ex_vol=Ccy_Pair_Volume
        else:
            Exchange_Price=np.column_stack((Exchange_Price,CCy_Pair_Price))
            ex_vol=np.column_stack((ex_vol,Ccy_Pair_Volume))
        # Exchange_Price is the matrix containing the cryptoasset prices in the different exchanges
        print('this is exchanges matrix of prices')
        print(Exchange_Price)
        print(Exchange_Price.shape)
    Exchange_Price=Exchange_Price.sum(axis=1)/ex_vol.sum(axis=1) #volume weighted average price of the CryptoAsset
    ex_vol=ex_vol.sum(axis=1) #total volume for the CryptoAsset
    if Crypto_Asset_Prices.size==0:
        Crypto_Asset_Prices=Exchange_Price
        Crypto_Asset_Volume=ex_vol
    else:
        Crypto_Asset_Prices=np.column_stack((Crypto_Asset_Prices,Exchange_Price))
        Crypto_Asset_Volume=np.column_stack((Crypto_Asset_Volume,ex_vol))
print('Crypto asset price of BTC')
print(Crypto_Asset_Prices)
print(Crypto_Asset_Prices.shape)