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
def date_gen(Start_Date, End_Date, delta):
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




crypto =  ['BTC']
curr = ['USD']#, 'USDC', 'USD']

def Coinbase_API(Crypto, Fiat, Start_Date, End_Date = None, granularity = '86400', ):

    if End_Date == None:
        End_Date = datetime.now().strftime('%m-%d-%Y')
   
    date_object = date_gen(Start_Date, End_Date, 49)
    df = np.array([])
    header = ['Time', 'low', 'high', 'open', 'Close Price', 'Crypto Volume']
    # d = {}

    for assets in Crypto:

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
                
               
    #             d["df_{}_{}".format(assets, fiat)] = dataframe ##perche???????

    Coinbase_df = pd.DataFrame(df, columns=header)        
    Coinbase_df = Coinbase_df.drop(columns = ['open', 'high', 'low'])      

    return Coinbase_df    