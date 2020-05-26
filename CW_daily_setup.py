# va messo check con CW clean del giorno prima. prendo tutti le key formtae
# da crypto-fiat-exchange e faccio il check sui dati del giorno.
# se ci sono dati nuovi vediamo il da farsi
# se mancano dati (seri morta) lo individuo e metto 0

# standard library import
import time

from datetime import datetime

# third party import
from pymongo import MongoClient
import pandas as pd
import numpy as np

# local import
import cryptoindex.data_setup as data_setup
import cryptoindex.data_download as data_download
import cryptoindex.mongo_setup as mongo


# ############# INITIAL SETTINGS ################################

pair_array = ['gbp', 'usd', 'cad', 'jpy', 'eur', 'usdt', 'usdc']
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur', 'usdt', 'usdc']
Crypto_Asset = ['BTC', 'ETH', 'XRP', 'LTC', 'BCH',
                'EOS', 'ETC', 'ZEC', 'ADA', 'XLM', 'XMR', 'BSV']
# crypto complete [ 'BTC', 'ETH', 'XRP', 'LTC', 'BCH', 'EOS',
# 'ETC', 'ZEC', 'ADA', 'XLM', 'XMR', 'BSV']
Exchanges = ['coinbase-pro', 'poloniex', 'bitstamp',
             'gemini', 'bittrex', 'kraken', 'bitflyer']
# exchange complete = [ 'coinbase-pro', 'poloniex', 'bitstamp',
# 'gemini', 'bittrex', 'kraken', 'bitflyer']

# #################### setup mongo connection ##################

# connecting to mongo in local
connection = MongoClient('localhost', 27017)
# creating the database called index
db = connection.index

# naming the existing collections as a variable
collection_clean = db.cleandata
collection_volume = db.volume_checked_data

# defining the database name and the collection name where to look for data
database = "index"
collection_raw = "CW_rawdata"
collection_clean_check = "CW_cleandata"
collection_volume_check = "volume_checked_data"

# ############################ missing days check #############################


# set start_period (ever)
Start_Period = '01-01-2016'
# set today
today = datetime.now().strftime('%Y-%m-%d')
today_TS = int(datetime.strptime(today, '%Y-%m-%d').timestamp()) + 3600
yesterday_TS = today_TS - 86400

# defining the array containing all the date from start_period until today
date_complete = data_setup.timestamp_gen(Start_Period)
# converting the timestamp format date into string
date_complete = [str(single_date) for single_date in date_complete]

# searching only the last five days
last_five_days = date_complete[(len(date_complete) - 5): len(date_complete)]

# date_complete = data_setup.timestamp_to_str(date_complete)
# defining the MongoDB path where to look for the rates
query = {'Exchange': "coinbase-pro", 'Pair': "ethusd"}

# retrieving data from MongoDB 'index' and 'ecb_raw' collection
matrix = mongo.query_mongo(database, collection_clean_check, query)

# checking the time column
date_list = np.array(matrix["Time"])
last_five_days_mongo = date_list[(len(date_list) - 5): len(date_list)]

# finding the date to download as difference between complete array of date and
# date now stored on MongoDB
date_to_add = data_setup.Diff(last_five_days, last_five_days_mongo)
print(date_to_add)

if date_to_add != []:

    if len(date_to_add) > 1:

        date_to_add.sort()
        start_date = data_setup.timestamp_to_human(
            [date_to_add[0]], date_format='%m-%d-%Y')
        start_date = start_date[0]
        end_date = data_setup.timestamp_to_human(
            [date_to_add[len(date_to_add)-1]], date_format='%m-%d-%Y')
        end_date = end_date[0]

    else:

        start_date = datetime.fromtimestamp(int(date_to_add[0]))
        start_date = start_date.strftime('%m-%d-%Y')
        end_date = start_date

    relative_reference_vector = data_setup.timestamp_gen(
        start_date, end_date, EoD='N')

    # creating a date array of support that allows to manage the one-day missing data
    if start_date == end_date:

        day_before = int(relative_reference_vector[0]) - 86400
        support_date_array = np.array([day_before])
        support_date_array = np.append(
            support_date_array, int(relative_reference_vector[0]))


# ################### fixing the "Pair Volume" information #################
db = 'index'
collection_raw = "CW_rawdata"
q_dict = {'Time': yesterday_TS}

daily_matrix = mongo.query_mongo2(db, collection_raw, q_dict)
daily_matrix = daily_matrix.loc[tot_matrix.Time != 0]
daily_matrix = daily_matrix.drop(columns=['Low', 'High', 'Open'])

for Crypto in Crypto_Asset:

    currencypair_array = []

    for i in pair_array:

        currencypair_array.append(Crypto.lower() + i)

    for exchange in Exchanges:

        for cp in currencypair_array:

            matrix = daily_matrix.loc[daily_matrix['Exchange'] == exchange]
            matrix = matrix.loc[matrix['Pair'] == cp]
            # checking if the matrix is not empty
            if matrix.shape[0] > 1:

                matrix['Pair Volume'] = matrix['Close Price'] * \
                    matrix['Crypto Volume']

            # put the manipulated data on MongoDB
            try:

                data = matrix.to_dict(orient='records')
                collection_volume.insert_many(data)

            except TypeError:
                pass

end = time.time()

print("This script took: {} seconds".format(float(end - start)))


# ##############################

# ######################## logic matrix of pair ############################

collection_volume_check = "volume_checked_data"
collection_logic_key = 'exchange_pair_key'
q_dict = {'Time': yesterday_TS}

# downloading from MongoDB the matrix with the daily values and the
# matrix containing the exchange-pair logic values
daily_matrix = mongo.query_mongo2(db, collection_volume_check, q_dict)
logic_key = mongo.query_mongo2(db, collection_logic_key)

# ########## adding the dead series to the daily values ##################

# selecting only the exchange-pair couples present in the historical series
key_present = logic_key.loc[logic_key.logic_value == 1]
key_present.drop(columns=['logic_value'])

# creating the exchange-pair couples key for the daily matrix
daily_matrix['key'] = daily_matrix['Exchange'] + '&' + daily_matrix['Pair']

# applying a left join between the prresent keys matrix and the daily
# matrix, this operation returns a matrix containing all the keys in
# "key_present" and, if some keys are missing in "daily_matrix" put NaN
merged = pd.merge(key_present, daily_matrix, on='key', how='left')

# assigning some columns values and substituting NaN with 0
merged['Time'] = yesterday_TS
split_val = merged['key'].str.split('&', expand=True)
merged['Exchange'] = split_val[0]
merged['Pair'] = split_val[1]
merged.fillna(0, inplace=True)

# ########## checking potential new exchange-pair couple ##################

key_absent = logic_key.loc[logic_key.logic_value == 1]
key_absent.drop(columns=['logic_value'])

for Crypto in Crypto_Asset:

    currencypair_array = []

    for i in pair_array:

        currencypair_array.append(Crypto.lower() + i)

    for exchange in Exchanges:

        ex_matrix = daily_matrix.loc[tot_matrix['Exchange'] == exchange]

        for cp in currencypair_array:

            crypto = cp[:3]
            pair = cp[3:]

            cp_matrix = ex_matrix.loc[ex_matrix['Pair'] == cp]
            cp_matrix = cp_matrix.drop(columns=['Exchange', 'Pair'])
            # checking if the matrix is not empty
            if cp_matrix.shape[0] > 1:

                # check if the historical series start at the same date as
                # the start date if not fill the dataframe with zero values
                cp_matrix = data_setup.homogenize_series(
                    cp_matrix, reference_date_vector)

                # check if the series stopped at certain point in
                # the past, if yes fill with zero
                cp_matrix = data_setup.homogenize_dead_series(
                    cp_matrix, reference_date_vector)

                # checking if the matrix has missing data and if ever fixing it
                if cp_matrix.shape[0] != reference_date_vector.size:

                    cp_matrix = data_setup.CW_series_fix_missing2(
                        cp_matrix, exchange, cp, reference_date_vector,
                        db, collection_volume_check)

                # ######## part that transform the timestamped date into string

                new_date = np.array([])
                for element in cp_matrix['Time']:

                    element = str(element)
                    new_date = np.append(new_date, element)

                cp_matrix['Time'] = new_date
                # ############################################################

                # add exchange and currency_pair column
                cp_matrix['Exchange'] = exchange
                cp_matrix['Pair'] = cp
                # put the manipulated data on MongoDB
                data = cp_matrix.to_dict(orient='records')
                collection_clean.insert_many(data)

# #######################################################################
db = connection.index
# db.volume_checked_data.drop()

############################## fixing historical series main part ##################################

for Crypto in Crypto_Asset:

     print(Crypto)
      currencypair_array = []

       for i in pair_array:

            currencypair_array.append(Crypto.lower() + i)

        for exchange in Exchanges:

            print(exchange)
            for cp in currencypair_array:

                print(cp)
                crypto = cp[:3]
                pair = cp[3:]

                # defining the dictionary for the MongoDB query
                query_dict = {"Exchange": exchange, "Pair": cp}
                # retrieving the needed information from rawdata collection on MongoDB
                matrix = mongo.query_mongo(
                    database, collection_raw, query_dict)
                matrix = matrix.drop(columns=['Low', 'High', 'Open'])
                # retrieving the needed information from cleandata collection
                service_matrix = mongo.query_mongo(
                    database, collection_clean_check, query_dict)
                # selecting the date of interest
                if start_date == end_date:

                    matrix = matrix.loc[matrix['Time'].isin(
                        support_date_array)]

                else:

                    matrix = matrix.loc[matrix['Time'].isin(
                        relative_reference_vector)]

                matrix = matrix.drop(columns=['Exchange', 'Pair'])
                # checking if the matrix is not empty
                if (matrix.shape[0] > 1 or service_matrix.shape[0] > 1):

                    if start_date == end_date:

                        t_value = matrix.loc[matrix['Time'].isin(
                            relative_reference_vector)]
                        t_1_value = matrix.loc[matrix['Time'] == (
                            int(relative_reference_vector[0]) - 86400)]

                        # the volume are so low that CW put 0 into the series
                        # so the values of the day before are substituted for the crytpo price
                        if t_value.empty == True:

                            if t_1_value.empty == False:

                                matrix = t_1_value
                                matrix['Time'] = relative_reference_vector[0]
                                matrix['Close Price'] = t_1_value['Close Price']
                                matrix['Crypto Volume'] = 0
                                matrix['Pair Volume'] = 0

                            else:

                                t_1_value = service_matrix.loc[service_matrix['Time'] == str(
                                    (int(relative_reference_vector[0]) - 86400))]
                                matrix = t_1_value
                                matrix['Time'] = relative_reference_vector[0]
                                matrix['Close Price'] = t_1_value['Close Price']
                                matrix['Crypto Volume'] = 0
                                matrix['Pair Volume'] = 0

                        else:

                            matrix = t_value

                            if matrix.shape[0] != relative_reference_vector.size:

                                matrix = data_setup.CW_series_fix_missing(
                                    matrix, exchange, Crypto, pair, start_date, end_date)

                    else:

                        # checking if the matrix has missing data and if ever fixing it

                        if matrix.shape[0] != relative_reference_vector.size:

                            matrix = data_setup.CW_series_fix_missing(
                                matrix, exchange, Crypto, pair, start_date, end_date)

                ######### part that transform the timestamped date into string ###########
                new_date = np.array([])
                for element in matrix['Time']:

                    element = str(element)
                    new_date = np.append(new_date, element)

                matrix['Time'] = new_date
                ########################################################################

                # add exchange and currency_pair column
                matrix['Exchange'] = exchange
                matrix['Pair'] = cp
                print(matrix)

                try:

                    # put the manipulated data on MongoDB
                    data = matrix.to_dict(orient='records')
                    collection_clean.insert_many(data)

                except:

                    pass

else:

    print('Message: No new date to fix, the cleandata collection on MongoDB is updated.')
