# standard library import
from datetime import datetime

# third party import
from pymongo import MongoClient
import numpy as np
import pandas as pd

# local import
import cryptoindex.mongo_setup as mongo
import cryptoindex.data_setup as data_setup
import cryptoindex.calc as calc

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


# ################# setup mongo connection ################

# connecting to mongo in local
connection = MongoClient('localhost', 27017)
db = connection.index

# define database name and collection name
db_name = "index"
# coll_data = "index_data_feed" or coll_data = "CW_final_data"
coll_data = "CW_final_data"
coll_log1 = 'index_logic_matrix_one'
coll_log2 = 'index_logic_matrix_two'
coll_ewma = 'index_EWMA'
coll_price = 'crypto_price'
coll_volume = 'crypto_volume'
coll_divisor = 'index_divisor'
coll_weights = 'index_weights'
coll_rel_synt = 'index_synth_matrix'
coll_divisor_res = 'index_divisor_reshaped'
coll_raw_index = 'index_level_raw'
coll_1000_index = 'index_level_1000'

# creating some the empty collections within the database index

# collection for index weights
db.index_weights.create_index([("id", -1)])
collection_weights = db.index_weights
# collection for index level base 1000
db.index_level_1000.create_index([("id", -1)])
collection_index_level = db.index_level_1000
# collection for index level raw
db.index_raw_level.create_index([("id", -1)])
collection_index_level = db.index_level_raw
# collection for crypto prices
db.crypto_price.create_index([("id", -1)])
collection_price = db.crypto_price
# collection for crytpo volume
db.crypto_volume.create_index([("id", -1)])
collection_volume = db.crypto_volume
# collection for price returns
db.crypto_price_return.create_index([("id", -1)])
collection_price_ret = db.crypto_price_return
# collection for EWMA
db.index_EWMA.create_index([("id", -1)])
collection_EWMA = db.index_EWMA
# collection for first logic matrix
db.index_logic_matrix_one.create_index([("id", -1)])
collection_logic_one = db.index_logic_matrix_one
# collection for second logic matrix
db.index_logic_matrix_two.create_index([("id", -1)])
collection_logic_two = db.index_logic_matrix_two
# collection for EWMA double checked with both logic matrix
db.index_EWMA_logic_checked.create_index([("id", -1)])
collection_EWMA_check = db.index_EWMA_logic_checked
# collection for the divisor array
db.index_divisor.create_index([("id", -1)])
collection_divisor = db.index_divisor
# collection for the reshaped divisor array
db.index_divisor_reshaped.create_index([("id", -1)])
collection_divisor_reshaped = db.index_divisor_reshaped
# collection for the relative syntethic matrix
db.index_synth_matrix.create_index([("id", -1)])
collection_relative_synth = db.index_synth_matrix
# collection for index level raw
db.index_raw_level.create_index([("id", -1)])
collection_index_raw = db.index_level_raw
# collection for index level base 1000
db.index_level_1000.create_index([("id", -1)])
collection_index_1000 = db.index_level_1000

# ################ DATE SETTINGS ########################

# define the start date as MM-DD-YYYY
start_date = '01-01-2016'

# define today in various format
today = datetime.now().strftime('%Y-%m-%d')
today_TS = int(datetime.strptime(today, '%Y-%m-%d').timestamp()) + 3600 + 3600
yesterday_TS = today_TS - 86400
two_before_TS = yesterday_TS - 86400
today_human = data_setup.timestamp_to_human([today_TS])
yesterday_human = data_setup.timestamp_to_human([yesterday_TS])
two_before_human = data_setup.timestamp_to_human([two_before_TS])


# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = data_setup.timestamp_gen(start_date)

# define all the useful arrays containing the rebalance start
# date, stop date, board meeting date
rebalance_start_date = calc.start_q('01-01-2016')
rebalance_start_date = calc.start_q_fix(rebalance_start_date)
rebalance_stop_date = calc.stop_q(rebalance_start_date)
board_date = calc.board_meeting_day()
board_date_eve = calc.day_before_board()
next_rebalance_date = calc.next_start()

# call the function that creates a object containing
# the couple of quarterly start-stop date
quarterly_date = calc.quarterly_period()

# defining time variables
old_reb_start = rebalance_start_date[len(rebalance_start_date) - 2]
new_reb_start = rebalance_start_date[len(rebalance_start_date) - 1]
next_reb_stop = rebalance_stop_date[len(rebalance_stop_date) - 1]
curr_board_eve = board_date_eve[len(board_date_eve) - 1]

# ####################### MAIN PART #################################

# downloading the daily value from MongoDB and put it into a dataframe

# defining the dictionary for the MongoDB query
query_dict = {"Time": str(yesterday_TS)}
# retriving the needed information on MongoDB
daily_matrix = mongo.query_mongo(db_name, coll_data, query_dict)

# initialize the matrices that will contain the prices
# and volumes of all the cryptoasset
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
        # initialize the matrices that will contain the data related
        # to all currencypair for the single exchange
        Ccy_Pair_PriceVolume = np.matrix([])
        Ccy_Pair_Volume = np.matrix([])
        Ccy_Pair_Price = np.matrix([])

        for cp in currencypair_array:
            print(cp)

            crypto = cp[:3]
            fiat_curr = cp[3:]

            # ######### LEAVING OUT NEW CRYPTO-FIAT PAIRS ##################

            c_1 = (exchange == 'bittrex' and fiat_curr == 'eur')
            c_2 = (exchange == 'bittrex' and crypto ==
                   'ltc' and fiat_curr == 'usd')
            c_3 = (exchange == 'poloniex' and crypto ==
                   'bch' and fiat_curr == 'usdc')
            if c_1 or c_2 or c_3:

                continue

            # ##############################################################

            # selecting the data referring to specific
            # exchange and crypto-fiat pair
            matrix = daily_matrix.loc[
                (daily_matrix["Exchange"] == exchange) &
                (daily_matrix['Pair'] == cp)
            ]

            if matrix.empty is False:

                price = np.array(float(matrix['Close Price']))
                volume = np.array(float(matrix['Pair Volume']))
                priceXvolume = np.array(price * volume)

                # every "cp" the loop adds a column in the matrices referred
                # to the single "exchange"
                if Ccy_Pair_PriceVolume.size == 0:
                    Ccy_Pair_PriceVolume = priceXvolume
                    Ccy_Pair_Volume = volume
                else:
                    Ccy_Pair_PriceVolume = np.column_stack(
                        (Ccy_Pair_PriceVolume, priceXvolume))
                    Ccy_Pair_Volume = np.column_stack(
                        (Ccy_Pair_Volume, volume))

            else:
                pass

        # computing the volume weighted average price of the single exchange

        # first if condition: the crypto-fiat is in more than 1 exchange
        if Ccy_Pair_Volume.size != 0 and Ccy_Pair_Volume.size > 1:

            PxV = Ccy_Pair_PriceVolume.sum(axis=1)
            V = Ccy_Pair_Volume.sum(axis=1)
            Ccy_Pair_Price = np.divide(
                PxV, V, out=np.zeros_like(V), where=V != 0.0)

            # computing the total volume of the exchange
            Ccy_Pair_Volume = Ccy_Pair_Volume.sum(axis=1)
            # computing price X volume of the exchange
            Ccy_Pair_PxV = Ccy_Pair_Price * Ccy_Pair_Volume

        # second if condition: the crypto-fiat is traded in just one exchange
        elif Ccy_Pair_Volume.size != 0 and Ccy_Pair_Volume.size == 1:

            np.seterr(all=None, divide='warn')
            Ccy_Pair_Price = np.divide(Ccy_Pair_PriceVolume,
                                       Ccy_Pair_Volume,
                                       out=np.zeros_like(Ccy_Pair_Volume),
                                       where=Ccy_Pair_Volume != 0.0)
            Ccy_Pair_Price = np.nan_to_num(Ccy_Pair_Price)
            Ccy_Pair_PxV = Ccy_Pair_Price * Ccy_Pair_Volume

        # third if condition: the crypto-fiat is not traded at all
        else:

            Ccy_Pair_Price = np.array([])
            Ccy_Pair_Volume = np.array([])
            Ccy_Pair_PxV = np.array([])

        # creating every loop the matrices containing the data referred to all
        # the exchanges Exchange_Price contains the crypto ("cp") prices in all
        # the different Exchanges, Exchange_Volume contains the crypto ("cp")
        # volume in all the different Exchanges if no values is found,
        # script put "0" instead
        if Exchange_Price.size == 0:

            if Ccy_Pair_Volume.size != 0:

                Exchange_Price = Ccy_Pair_Price
                Exchange_Volume = Ccy_Pair_Volume
                Ex_PriceVol = Ccy_Pair_PxV

            else:

                Exchange_Price = np.zeros(1)
                Exchange_Volume = np.zeros(1)
                Ex_PriceVol = np.zeros(1)

        else:

            if Ccy_Pair_Volume.size != 0:

                Exchange_Price = np.column_stack(
                    (Exchange_Price, Ccy_Pair_Price))
                Exchange_Volume = np.column_stack(
                    (Exchange_Volume, Ccy_Pair_Volume))
                Ex_PriceVol = np.column_stack((Ex_PriceVol, Ccy_Pair_PxV))

            else:

                Exchange_Price = np.column_stack(
                    (Exchange_Price, np.zeros(1)))
                Exchange_Volume = np.column_stack(
                    (Exchange_Volume, np.zeros(1)))
                Ex_PriceVol = np.column_stack(
                    (Ex_PriceVol, np.zeros(1)))

    # dataframes that contain volume and price of a single crytpo
    # for all the exchanges. If an exchange does not have value
    # in the crypto will be insertd a column with zero
    Exchange_Vol_DF = pd.DataFrame(Exchange_Volume, columns=Exchanges)
    Exchange_Price_DF = pd.DataFrame(Exchange_Price, columns=Exchanges)

    # adding "Time" column to both Exchanges dataframe
    Exchange_Vol_DF['Time'] = str(today_TS)
    Exchange_Price_DF['Time'] = str(today_TS)

    try:

        # computing the volume weighted average price of the single
        # Crypto_Asset ("CryptoA") into a single vector
        Ex_price_num = Ex_PriceVol.sum(axis=1)
        Ex_price_den = Exchange_Volume.sum(axis=1)
        Exchange_Price = np.divide(Ex_price_num, Ex_price_den,
                                   out=np.zeros_like(Ex_price_num),
                                   where=Ex_price_num != 0.0)
        # computing the total volume  average price of the
        # single Crypto_Asset ("CryptoA") into a single vector
        Exchange_Volume = Exchange_Volume.sum(axis=1)

    except np.AxisError:

        Exchange_Price = Exchange_Price
        Exchange_Volume = Exchange_Volume

    # creating every loop the matrices of all the Cryptoassets
    # Crypto_Asset_Price contains the prices of all the cryptocurrencies
    # Crypto_Asset_Volume contains the volume of all the cryptocurrencies
    if Crypto_Asset_Prices.size == 0:

        Crypto_Asset_Prices = Exchange_Price
        Crypto_Asset_Volume = Exchange_Volume

    else:

        Crypto_Asset_Prices = np.column_stack(
            (Crypto_Asset_Prices, Exchange_Price))
        Crypto_Asset_Volume = np.column_stack(
            (Crypto_Asset_Volume, Exchange_Volume))

# turn prices and volumes into pandas dataframe
Crypto_Asset_Prices = pd.DataFrame(Crypto_Asset_Prices, columns=Crypto_Asset)
Crypto_Asset_Volume = pd.DataFrame(Crypto_Asset_Volume, columns=Crypto_Asset)

# compute the price return of the day
two_before_price = mongo.query_mongo(
    db_name, coll_price, {"Time": two_before_TS})
two_before_price = two_before_price.drop(columns=['Time', 'Date'])
return_df = two_before_price.append(Crypto_Asset_Prices)
price_ret = return_df.pct_change()
price_ret = price_ret.iloc[[1]]
# then add the 'Time' column
time_header = ['Time']
time_header.extend(Crypto_Asset)
Crypto_Asset_Prices = pd.DataFrame(Crypto_Asset_Prices, columns=time_header)
Crypto_Asset_Prices['Time'] = str(yesterday_TS)
Crypto_Asset_Volume = pd.DataFrame(Crypto_Asset_Volume, columns=time_header)
Crypto_Asset_Volume['Time'] = str(yesterday_TS)
price_ret['Time'] = str(yesterday_TS)
print(Crypto_Asset_Prices)
print(price_ret)


# computing the Exponential Weighted Moving Average of the day
hist_volume = mongo.query_mongo(db_name, coll_volume)
hist_volume = hist_volume.drop(columns=['Date'])
hist_volume = hist_volume.append(Crypto_Asset_Volume)
daily_ewma = calc.daily_ewma_crypto_volume(hist_volume, Crypto_Asset)
print(daily_ewma)
# daily_ewma = ewma_df.iloc[[len(reference_date_vector) - 1]]

# downloading from mongoDB the current logic matrices (1 e 2)
logic_one = mongo.query_mongo(db_name, coll_log1)
logic_two = mongo.query_mongo(db_name, coll_log2)
# taking only the logic value referred to the new period
current_logic_one = logic_one.iloc[[len(logic_one['Date']) - 2]]  # check -2
current_logic_one = current_logic_one.drop(columns=['Date', 'Time'])
# taking only the logic value referred to the current period
current_logic_two = logic_two.iloc[[len(logic_two['Date']) - 2]]  # check -2
current_logic_two = current_logic_two.drop(columns=['Date', 'Time'])

# computing the ewma checked with both the first and second logic matrices
daily_ewma_first_check = (np.array(daily_ewma) * np.array(current_logic_one))
daily_ewma_double_check = daily_ewma_first_check * np.array(current_logic_two)
daily_ewma_double_check = pd.DataFrame(daily_ewma_double_check,
                                       columns=Crypto_Asset)

# downloading from mongoDB the current weights
weights = mongo.query_mongo(db_name, coll_weights)

# daily syntethic matrix computation
synt_ptf_value = 100
yest_ret = np.array(price_ret[Crypto_Asset])
new_weights = weights.loc[weights.Time == new_reb_start, Crypto_Asset]
synt_w = np.array(new_weights) * synt_ptf_value
daily_synth = synt_w * (1 + yest_ret)
# turn into a df
daily_synth_df = pd.DataFrame(daily_synth, columns=Crypto_Asset)


# daily synthtic relative matrix computation
daily_synth_df['row_sum'] = daily_synth_df[Crypto_Asset].sum(axis=1)
daily_synt_rel = daily_synth_df.loc[Crypto_Asset].div(
    daily_synth_df['row_sum'], axis=0)


# find new divisor value
yest_price = np.array(two_before_price)
new_divisor_df = calc.new_divisor(yest_price, weights, logic_two,
                                  Crypto_Asset, old_reb_start,
                                  new_reb_start)


# daily index value computation
curr_div_val = np.array(new_divisor_df['Divisor Value'])
index_numerator = np.array(
    Crypto_Asset_Prices[Crypto_Asset]) * np.array(daily_synt_rel)
numerator_sum = index_numerator.sum(axis=1)
num = pd.DataFrame(numerator_sum)
daily_index_value = np.array(num) / curr_div_val
raw_index_df = pd.DataFrame(daily_index_value, columns=['Index Value'])

# retrieving from mongoDB the yesterday value of the raw index
yesterday_raw_index = mongo.query_mongo(
    db_name, coll_raw_index, {'Date': two_before_human[0]})
yesterday_raw_index = yesterday_raw_index.drop(columns=['Date', 'Time'])
raw_curr = yesterday_raw_index.append(raw_index_df)
variation = raw_curr.pct_change()
variation = np.array(variation.iloc[1])

# retrieving from mongoDB the yesterday value of the raw index
yesterday_1000_index = mongo.query_mongo(
    db_name, coll_1000_index,  {'Date': two_before_human[0]})
daily_index_1000 = np.array(
    yesterday_1000_index['Index Value']) * (1 + variation)
daily_index_1000_df = pd.DataFrame(daily_index_1000, columns=['Index Value'])
print(daily_index_1000_df)


# ############ MONGO DB UPLOADS ############################################

# put the daily crypto prices on MongoDB
Crypto_Asset_Prices['Date'] = yesterday_human
price_up = Crypto_Asset_Prices[['Date', 'Time', 'BTC', 'ETH', 'XRP',
                                'LTC', 'BCH', 'EOS', 'ETC', 'ZEC',
                                'ADA', 'XLM', 'XMR', 'BSV']]
price_up = price_up.to_dict(orient='records')
collection_price.insert_many(price_up)

# put the daily crypto volumes on MongoDB
Crypto_Asset_Volume['Date'] = yesterday_human
volume_up = Crypto_Asset_Volume[['Date', 'Time', 'BTC', 'ETH', 'XRP',
                                 'LTC', 'BCH', 'EOS', 'ETC', 'ZEC',
                                 'ADA', 'XLM', 'XMR', 'BSV']]
volume_up = volume_up.to_dict(orient='records')
collection_volume.insert_many(volume_up)

# put the daily return on MongoDB
price_ret['Date'] = yesterday_human
price_ret_up = price_ret[['Date', 'Time', 'BTC', 'ETH', 'XRP',
                          'LTC', 'BCH', 'EOS', 'ETC', 'ZEC',
                          'ADA', 'XLM', 'XMR', 'BSV']]
price_ret_up = price_ret_up.to_dict(orient='records')
collection_price_ret.insert_many(price_ret_up)

# put the EWMA dataframe on MongoDB
daily_ewma['Date'] = yesterday_human
daily_ewma['Time'] = str(today_TS)
ewma_df_up = daily_ewma[['Date', 'Time', 'BTC', 'ETH', 'XRP',
                         'LTC', 'BCH', 'EOS', 'ETC', 'ZEC',
                         'ADA', 'XLM', 'XMR', 'BSV']]
ewma_df_up = ewma_df_up.to_dict(orient='records')
collection_EWMA.insert_many(ewma_df_up)

# put the double checked EWMA on MongoDB
daily_ewma_double_check['Date'] = yesterday_human
daily_ewma_double_check['Time'] = str(today_TS)
double_EWMA_up = daily_ewma_double_check[[
    'Date', 'Time', 'BTC', 'ETH', 'XRP',
    'LTC', 'BCH', 'EOS', 'ETC', 'ZEC',
    'ADA', 'XLM', 'XMR', 'BSV']]
double_EWMA_up = double_EWMA_up.to_dict(orient='records')
collection_EWMA_check.insert_many(double_EWMA_up)

# put the relative synth matrix on MongoDB
daily_synt_rel['Date'] = yesterday_human
daily_synt_rel['Time'] = str(today_TS)
synth_up = daily_synt_rel[['Date', 'Time', 'BTC', 'ETH', 'XRP',
                           'LTC', 'BCH', 'EOS', 'ETC', 'ZEC',
                           'ADA', 'XLM', 'XMR', 'BSV']]
synth_up = synth_up.to_dict(orient='records')
collection_relative_synth.insert_many(synth_up)

# uploading the new divisor value
new_divisor_df['Time'] = yesterday_TS
new_divisor_df['Date'] = yesterday_human
new_div_up = new_divisor_df[['Date', 'Time', 'Divisor Value']]
new_div_up = new_div_up.to_dict(orient='records')
collection_divisor.insert_many(new_div_up)

# uploading the collection of the reshaped divisor
current_divisor = new_divisor_df['Divisor Value']
current_divisor['Date'] = yesterday_human
current_divisor['Time'] = yesterday_TS
div_res_up = current_divisor.to_dict(orient='records')
collection_divisor_reshaped.insert_many(div_res_up)

# put the index level raw on MongoDB
raw_index_df['Date'] = yesterday_human
raw_index_df['Time'] = yesterday_TS
raw_index_val_up = raw_index_df[['Date', 'Time', 'Index Value']]
raw_index_val_up = raw_index_val_up.to_dict(orient='records')
collection_index_raw.insert_many(raw_index_val_up)

# put the index level 1000 on MongoDB
daily_index_1000_df['Date'] = yesterday_human
daily_index_1000_df['Time'] = yesterday_TS
index_val_up = daily_index_1000_df[['Date', 'Time', 'Index Value']]
index_val_up = index_val_up.to_dict(orient='records')
collection_index_1000.insert_many(index_val_up)