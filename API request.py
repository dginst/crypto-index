#####################################################################################################
################################ COINBASE - PRO #####################################################
#####################################################################################################

#REST API request for coinbase-pro exchange. 
#It requires:
#crypto pair = crypto-fiat (ex: BTC - USD)
#start date = ISO 8601
#end date = ISO 8601
#granularity = in seconds. Ex 86400 = 1 day
#this api gives back 300 responses max for each request.
##for the moment it is just downloading one cryptocurr.
#-------------------------------------------------------------------------------------------------------

#BTC USD import
import requests
from requests import get
from datetime import *
import pandas as pd
import numpy as np
from time import sleep


#questa da il problema della chiave
def date_gen_isoformat(Start_Date, End_Date, delta):
    start = datetime.strptime(Start_Date,'%m-%d-%Y') 
    stop = datetime.strptime(End_Date,'%m-%d-%Y') 
    delta = timedelta(days=delta)
    pace = start
    if Start_Date != End_Date:

        while (pace < stop):

            end = pace + delta

            if end > stop:
                end = stop
            yield (str(pace.isoformat()), str(end.isoformat()))
            pace = end + timedelta(days = 1)

    else:
        
        yield (str(start.isoformat()), str(stop.isoformat()))



def date_gen_timestamp(Start_Date, End_Date, delta):
    start = datetime.strptime(Start_Date,'%m-%d-%Y') 
    stop = datetime.strptime(End_Date,'%m-%d-%Y') 
    delta = timedelta(days=delta)
    pace = start
    while (pace < stop):
        end = pace + delta
        if end > stop:
            end = stop
        yield (str(pace.timestamp()), str(end.timestamp()))
        pace = end + timedelta(days = 1)



crypto =  ['BTC']
curr = ['USD']#, 'USDC', 'USD']

def Coinbase_API(Crypto, Fiat, Start_Date, End_Date = None, granularity = '86400', ):

    if End_Date == None:
        End_Date = datetime.now().strftime('%m-%d-%Y')
   
    date_object = date_gen(Start_Date, End_Date, 49)
    df = np.array([])
    header = ['Time', 'low', 'high', 'open', 'Close Price', 'Crypto Volume']
    # d = {}

    for asset in Crypto:

        for fiat in Fiat:
            
            for start,stop in date_object:

                entrypoint = 'https://api.pro.coinbase.com/products/'
                key = assets+"-"+fiat+"/candles?start="+start+"&end="+stop+"&granularity="+granularity
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

    #The REST API OHLC endpoint only provides a limited amount of historical data, specifically 720 data points of the requested interval.
    #unfortunally the since option seems to not work. so here the date_gen fuction is useless

    def kraken_API(Start_Date, End_Date, Crypto, Fiat, interval  = '1440', ):

    df = np.array([])
    header = ['Time', 'low', 'high', 'open', 'Close Price', 'Crypto Volume']
    d = {}

    for asset in Crypto:

        for fiat in Fiat:
            
            entrypoint = 'https://api.kraken.com/0/public/OHLC?'
            key = 'pair='+assets+fiat+'&interval='+interval
            request_url = entrypoint + key

            response = requests.get(request_url)
            sleep(0.25)

            response = response.json()

            if df.size == 0 :
                df = np.array(response)
            else:
                df = np.row_stack((df, response))
            
               

    Coinbase_df = pd.DataFrame(df, columns=header)        
    Coinbase_df = Coinbase_df.drop(columns = ['open', 'high', 'low'])      

    return Kraken_df    




    #####################################################################################################
    ################################     BITTREX    #####################################################
    #####################################################################################################

    # https://bittrex.github.io/api/v3 api actually not working for historical data




    #####################################################################################################
    ################################    POLONIEX    #####################################################
    #####################################################################################################





    def Poloniex_API(Start_Date, End_Date, Crypto, Fiat, period = '86400', ):

    if End_Date == None:
        End_Date = datetime.now().strftime('%m-%d-%Y')
   
    date_object = date_gen(Start_Date, End_Date, 49)
    df = np.array([])
    header = ['Time', 'low', 'high', 'open', 'Close Price', 'Crypto Volume']
    d = {}

    for asset in Crypto:

        for fiat in Fiat:
            
            for start,stop in date_object:

                entrypoint = 'https://poloniex.com/public?command=returnChartData&currencyPair='
                key = asset+'_'fiat+'&start='+start+'&end='+stop+'&period='+period
                request_url = entrypoint + key

                response = requests.get(request_url)
                sleep(0.25)

                response = response.json()

                if df.size == 0 :
                    df = np.array(response)
                else:
                    #errore qui
                    df = np.row_stack((df, response))
                
               
    #non dovrebbe essere all'interno del ciclo?
    Coinbase_df = pd.DataFrame(df, columns=header)        
    Coinbase_df = Coinbase_df.drop(columns = ['open', 'high', 'low'])      

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
    
    # it gives back data from 1 year before, that's bad


    def Gemini_API(Start_Date, End_Date, Crypto, Fiat, time_frame = '1day', ):

    if End_Date == None:
        End_Date = datetime.now().strftime('%m-%d-%Y')
   
    date_object = date_gen(Start_Date, End_Date, 49)
    df = np.array([])
    header = ['Time', 'low', 'high', 'open', 'Close Price', 'Crypto Volume']
    d = {}

    for asset in Crypto:

        for fiat in Fiat:
            
            for start,stop in date_object:

                entrypoint = 'https://api.gemini.com/v2'
                key = '/candles/'+Crypto+Fiat+'/'+time_frame
                request_url = entrypoint + key

                response = requests.get(request_url)
                sleep(0.25)

                response = response.json()

                if df.size == 0 :
                    df = np.array(response)
                else:
                    #errore qui
                    df = np.row_stack((df, response))
                
               
    #non dovrebbe essere all'interno del ciclo?
    Coinbase_df = pd.DataFrame(df, columns=header)        
    Coinbase_df = Coinbase_df.drop(columns = ['open', 'high', 'low'])      

    return Poloniex_df    



    #####################################################################################################
    ################################    BITSTAMP    #####################################################
    #####################################################################################################

    # https://www.bitstamp.net/api/ api actually not working for historical data 
