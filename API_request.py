import requests
from requests import get
from datetime import *
import pandas as pd
import numpy as np
from time import sleep


# function takes as input Start Date, End Date (both string in mm-dd-yyyy format) and delta (numeric)
# then creates an object containing multiple couples of date in ISO format with "delta"  days between them
# for a total length defined by start date and end date. If the last delta is bigger than end date, then 
# end date is taken as last date of the object

def date_gen_isoformat(Start_Date, End_Date, delta):
     
    # converting string to date format
    start = datetime.strptime(Start_Date,'%m-%d-%Y') 
    stop = datetime.strptime(End_Date,'%m-%d-%Y') 
    # define delta as days
    delta = timedelta(days=delta)

    pace = start
    if Start_Date != End_Date:

        while (pace < stop):

            end = pace + delta

            if end > stop:
                end = stop

            yield (str(pace.isoformat()), str(end.isoformat()))
            pace = end + timedelta(days = 1)

    # if start date == end date, then a couple containing the same date is returned
    else:
        
        yield (str(start.isoformat()), str(stop.isoformat()))



# function takes as input Start Date, End Date (both string in mm-dd-yyyy format) and delta (numeric)
# then creates an object containing multiple couples of date in TIMESTAMP format with "delta"  days between them
# for a total length defined by start date and end date. If the last delta is bigger than end date, then 
# end date is taken as last date of the object

def date_gen_timestamp(Start_Date, End_Date, delta):

    # converting string to date format
    start = datetime.strptime(Start_Date,'%m-%d-%Y') 
    stop = datetime.strptime(End_Date,'%m-%d-%Y') 
    # define delta as days
    delta = timedelta(days=delta)

    pace = start
    if Start_Date != End_Date:

        while (pace < stop):

            end = pace + delta
            if end > stop:

                end = stop

            yield (str(pace.timestamp()), str(end.timestamp()))
            pace = end + timedelta(days = 1)
    
    # if start date == end date, then a couple containing the same date is returned
    else:

        yield (str(start.isoformat()), str(stop.isoformat()))


#####################################################################################################
################################ COINBASE - PRO #####################################################
#####################################################################################################

# REST API request for coinbase-pro exchange. 
# It requires:
# crypto pair = crypto-fiat (ex: BTC - USD)
# start date = ISO 8601
# end date = ISO 8601
# granularity = in seconds. Ex 86400 = 1 day
# this api gives back 300 responses max for each request.

#-------------------------------------------------------------------------------------------------------

# function that given Crypto (ex. BTC), Fiat (ex. USD), start DAte and End Date 
# retrieve the Historical Series from start date to End Date of the definend Crypto + Fiat
# on the Coinbase Pro Exchange
# the output will be displayed in three columns containing: 'Time' in timestamp format,
# 'Close Price' in "Fiat" currency, 'Crypto Volume' in "Crypto" amount

def Coinbase_API(Crypto, Fiat, Start_Date, End_Date = None, granularity = '86400'):

    Crypto = Crypto.upper()
    Fiat = Fiat.upper()

    if End_Date == None:
        End_Date = datetime.now().strftime('%m-%d-%Y')
   
    date_object = date_gen_isoformat(Start_Date, End_Date, 49)
    df = np.array([])
    header = ['Time', 'low', 'high', 'open', 'Close Price', 'Crypto Volume']

    for asset in Crypto:

        for fiat in Fiat:
            
            for start,stop in date_object:

                entrypoint = 'https://api.pro.coinbase.com/products/'
                key = asset+"-"+fiat+"/candles?start="+start+"&end="+stop+"&granularity="+granularity
                request_url = entrypoint + key

                response = requests.get(request_url)
                sleep(0.25)

                response = response.json()

                if df.size == 0 :
                    df = np.array(response)
                else:
                    df = np.row_stack((df, response))
                
               
    #non dovrebbe essere all'interno del ciclo?
    Coinbase_df = pd.DataFrame(df, columns=header)        
    Coinbase_df = Coinbase_df.drop(columns = ['open', 'high', 'low'])      

    return Coinbase_df    



#####################################################################################################
################################      KRAKEN    #####################################################
#####################################################################################################

# The REST API OHLC endpoint only provides a limited amount of historical data, specifically
# 720 data points of the requested interval.
# unfortunally the since option seems to not work. so here the date_gen fuction is useless

#-------------------------------------------------------------------------------------------------------

# function that given Crypto (ex. BTC), Fiat (ex. USD), start DAte and End Date 
# retrieve the Historical Series from start date to End Date of the definend Crypto + Fiat
# on the Kraken Exchange
# the output will be displayed in three columns containing: 'Time' in timestamp format,
# 'Close Price' in "Fiat" currency, 'Crypto Volume' in "Crypto" amount

def kraken_API(Start_Date, End_Date, Crypto, Fiat, interval  = '1440', ):

    df = np.array([])
    header = ['Time', 'open', 'high', 'low', 'Crypto Price', 'vwap', 'Crypto Volume', 'count']

    for asset in Crypto:

        for fiat in Fiat:
            
            entrypoint = 'https://api.kraken.com/0/public/OHLC?'
            key = 'pair='+asset+fiat+'&interval='+interval
            request_url = entrypoint + key

            response = requests.get(request_url)
            sleep(0.25)

            response = response.json()

            if df.size == 0 :
                df = np.array(response)
            else:
                df = np.row_stack((df, response))
            
            

    Kraken_df = pd.DataFrame(df, columns=header)        
    Kraken_df = Kraken_df.drop(columns = ['open', 'high', 'low', 'vwap', 'count'])      

    return Kraken_df    




#####################################################################################################
################################     BITTREX    #####################################################
#####################################################################################################

# https://bittrex.github.io/api/v3 api actually not working for historical data




#####################################################################################################
################################    POLONIEX    #####################################################
#####################################################################################################

# function that given Crypto (ex. BTC), Fiat (ex. USD), start DAte and End Date 
# retrieve the Historical Series from start date to End Date of the definend Crypto + Fiat
# on the Poloniex Exchange
# the output will be displayed in three columns containing: 'Time' in timestamp format,
# 'Close Price' in "Fiat" currency, 'Crypto Volume' in "Crypto" amount

def Poloniex_API(Start_Date, End_Date, Crypto, Fiat, period = '86400'):

    if End_Date == None:
        End_Date = datetime.now().strftime('%m-%d-%Y')
   
    date_object = date_gen_timestamp(Start_Date, End_Date, 49)
    df = np.array([])
    header = ['Time', 'open', 'high', 'low', 'Crypto Price', 'Crypto Volume', 'volume_usd', 'vwap']

    for asset in Crypto:

        for fiat in Fiat:
            
            for start,stop in date_object:

                entrypoint = 'https://poloniex.com/public?command=returnChartData&currencyPair='
                key = asset+'_'+fiat+'&start='+start+'&end='+stop+'&period='+period
                request_url = entrypoint + key

                response = requests.get(request_url)
                sleep(0.25)

                response = response.json()

                if df.size == 0:

                    df = np.array(response)

                else:

                    df = np.row_stack((df, response))
                
               

    Poloniex_df = pd.DataFrame(df, columns=header)        
    Poloniex_df = Poloniex_df.drop(columns = ['open', 'high', 'low', 'vwap', 'volume usd'])      

    return Poloniex_df    



#####################################################################################################
################################      ITBIT     #####################################################
#####################################################################################################

# https://www.itbit.com/api api actually not working for historical data 



#####################################################################################################
################################    BITFLYER    #####################################################
#####################################################################################################

# https://lightning.bitflyer.com/docs?lang=en api actually not working for historical data 




#####################################################################################################
################################     GEMINI     #####################################################
#####################################################################################################

# returns data just starting from 1 year back


def Gemini_API(Start_Date, End_Date, Crypto, Fiat, time_frame = '1day'):

    if End_Date == None:
        End_Date = datetime.now().strftime('%m-%d-%Y')
   
    df = np.array([])

    header = ['Time', 'low', 'high', 'open', 'Close Price', 'Crypto Volume']

    for asset in Crypto:

        for fiat in Fiat:
            
            entrypoint = 'https://api.gemini.com/v2'
            key = '/candles/'+asset+fiat+'/'+time_frame
            request_url = entrypoint + key

            response = requests.get(request_url)
            sleep(0.25)

            response = response.json()

            if df.size == 0 :

                df = np.array(response)
            else:

                df = np.row_stack((df, response))
                
               
    Gemini_df = pd.DataFrame(df, columns=header)        
    Gemini_df = Gemini_df.drop(columns = ['open', 'high', 'low'])      

    return Gemini_df    



    #####################################################################################################
    ################################    BITSTAMP    #####################################################
    #####################################################################################################

    # https://www.bitstamp.net/api/ api actually not working for historical data 
