# standard library import
import io
from datetime import datetime, timezone

# third party import
import pandas as pd

import requests
from requests import get

from cryptoindex.config import (
    START_DATE, DAY_IN_SEC
)


# #################### ECB rates download function ###################

# function that downloads the exchange rates from the ECB web page and returns a matrix (pd.DataFrame) that
# indicates: on the first column the date, on the second tha exchange rate vakue eutro based,
# on the third the currency, on the fourth the currency of denomination (always 'EUR')
# key_curr_vector expects a list of currency in International Currency Formatting (ex. USD, GBP, JPY, CAD,...)
# the functions diplays the information better for a single day data retrival, however can works with multiple date
# regarding the other default variables consult the ECB api web page
# start_period has to be in YYYY-MM-DD format


def ECB_rates_extractor(
    key_curr_vector,
    start_period,
    end_period=None,
    freq="D",
    curr_den="EUR",
    type_rates="SP00",
    series_var="A",
):

    # set end_period = start_period if empty, so that is possible to perform daily download
    if end_period is None:
        end_period = start_period

    # API settings
    entrypoint = "https://sdw-wsrest.ecb.europa.eu/service/"
    resource = "data"
    flow_ref = "EXR"
    param = {"startPeriod": start_period, "endPeriod": end_period}

    exc_rate_list = pd.DataFrame()
    # turning off a pandas warning about slicing of DF
    pd.options.mode.chained_assignment = None

    for currency in key_curr_vector:
        key = (
            freq + "." + currency + "." + curr_den + "." + type_rates + "." + series_var
        )
        request_url = entrypoint + resource + "/" + flow_ref + "/" + key

        # API call
        response = get(request_url, params=param,
                       headers={"Accept": "text/csv"})

        # if data is empty, it is an holiday, therefore exit
        try:

            df = pd.read_csv(io.StringIO(response.text))

        except pd.errors.EmptyDataError:

            break

        main_df = df.filter(
            ["TIME_PERIOD", "OBS_VALUE", "CURRENCY", "CURRENCY_DENOM"], axis=1
        )
        print(main_df)
        # transform date from datetime to string
        date_to_string = main_df["TIME_PERIOD"].to_string(
            index=False).strip()
        # transform date into unix timestamp and add 3600 sec in order to uniform the date at 12:00 am
        date_to_string = datetime.strptime(date_to_string, "%Y-%m-%d")
        date_timestamp = int(date_to_string.replace(
            tzinfo=timezone.utc).timestamp())
        date_timestamp = str(date_timestamp)
        # reassigning the timestamp date to the dataframe
        main_df["TIME_PERIOD"] = date_timestamp

        if exc_rate_list.size == 0:

            exc_rate_list = main_df

        else:

            exc_rate_list = exc_rate_list.append(
                main_df, sort=True)

    exc_rate_list.reset_index(drop=True, inplace=True)

    return exc_rate_list


# CRYPTOWATCH DOWNLOAD FUNCTION

# function that retrieves data from CryptoWatch websites, download them and store them on Mongo DB
# inputs are:
# exchange: name of the exchange pof interest
# currencypair: string containing both the name of cryptocurrency and fiat pair (ex. btcusd)
# mongo_collection: name of the collection on MongoDB where to put the downloaded data


def CW_raw_to_mongo(
    exchange,
    currencypair,
    mongo_collection,
    start_date=START_DATE,
    end_date=None,
    periods="86400"
):

    Pair = currencypair[3:].upper()

    start_date = datetime.strptime(start_date, "%m-%d-%Y")

    # set end_date = today if empty
    if end_date is None:

        end_date = datetime.now().strftime("%m-%d-%Y")

    end_date = datetime.strptime(end_date, "%m-%d-%Y")
    # transform date into timestamps
    start_date = str(int(start_date.replace(tzinfo=timezone.utc).timestamp()))
    end_date = str(int(end_date.replace(
        tzinfo=timezone.utc).timestamp()) - DAY_IN_SEC)
    # API settings
    entrypoint = "https://api.cryptowat.ch/markets/"
    key = (
        exchange
        + "/"
        + currencypair
        + "/ohlc?periods="
        + periods
        + "&after="
        + start_date
        + "&before="
        + end_date
    )
    request_url = entrypoint + key

    # API call
    response = requests.get(request_url)
    response = response.json()

    try:

        for i in range(len(response["result"]["86400"])):

            r = response["result"]["86400"]
            Exchange = exchange
            Pair = currencypair
            Time = r[i][0]
            Open = r[i][1]
            High = r[i][2]
            Low = r[i][3]
            Close_Price = r[i][4]
            Crypto_Volume = r[i][5]
            Pair_Volume = r[i][6]

            rawdata = {
                "Exchange": Exchange,
                "Pair": Pair,
                "Time": Time,
                "Low": Low,
                "High": High,
                "Open": Open,
                "Close Price": Close_Price,
                "Crypto Volume": Crypto_Volume,
                "Pair Volume": Pair_Volume,
            }

            mongo_collection.insert_one(rawdata)

    except KeyError:

        r = response
        Exchange = exchange
        Pair = currencypair
        Time = 0
        Open = 0
        High = 0
        Low = 0
        Close_Price = 0
        Crypto_Volume = 0
        Pair_Volume = 0
        rawdata = {
            "Exchange": Exchange,
            "Pair": Pair,
            "Time": Time,
            "Low": Low,
            "High": High,
            "Open": Open,
            "Close Price": Close_Price,
            "Crypto Volume": Crypto_Volume,
            "Pair Volume": Pair_Volume,
        }

        mongo_collection.insert_one(rawdata)

    return None


def cw_raw_download(
    exchange,
    currencypair,
    dataframe,
    start_date=START_DATE,
    end_date=None,
    periods="86400"
):

    Pair = currencypair[3:].upper()

    start_date = datetime.strptime(start_date, "%m-%d-%Y")

    # set end_date = today if empty
    if end_date is None:

        end_date = datetime.now().strftime("%m-%d-%Y")
        end_date = datetime.strptime(end_date, "%m-%d-%Y")
        end_date = str(int(end_date.replace(
            tzinfo=timezone.utc).timestamp()) - DAY_IN_SEC)

    else:

        end_date = datetime.strptime(end_date, "%m-%d-%Y")
        end_date = str(int(end_date.replace(
            tzinfo=timezone.utc).timestamp()))

    # transform date into timestamps
    start_date = str(int(start_date.replace(tzinfo=timezone.utc).timestamp()))

    print(start_date)
    print(end_date)
    # API settings
    entrypoint = "https://api.cryptowat.ch/markets/"
    key = (
        exchange
        + "/"
        + currencypair
        + "/ohlc?periods="
        + periods
        + "&after="
        + start_date
        + "&before="
        + end_date
    )
    request_url = entrypoint + key
    print(request_url)
    # API call
    response = requests.get(request_url)
    response = response.json()

    try:

        for i in range(len(response["result"]["86400"])):

            r = response["result"]["86400"]
            Exchange = exchange
            Pair = currencypair
            Time = r[i][0]
            Open = r[i][1]
            High = r[i][2]
            Low = r[i][3]
            Close_Price = r[i][4]
            Crypto_Volume = r[i][5]
            Pair_Volume = r[i][6]

            rawdata = {
                "Exchange": Exchange,
                "Pair": Pair,
                "Time": Time,
                "Low": Low,
                "High": High,
                "Open": Open,
                "Close Price": Close_Price,
                "Crypto Volume": Crypto_Volume,
                "Pair Volume": Pair_Volume,
            }

            rawdata_df = pd.DataFrame.from_dict(rawdata, orient="index")
            rawdata_df = rawdata_df.transpose()

            dataframe = dataframe.append(rawdata_df, ignore_index=True)

    except KeyError:

        r = response
        Exchange = exchange
        Pair = currencypair
        Time = 0
        Open = 0
        High = 0
        Low = 0
        Close_Price = 0
        Crypto_Volume = 0
        Pair_Volume = 0
        rawdata = {
            "Exchange": Exchange,
            "Pair": Pair,
            "Time": Time,
            "Low": Low,
            "High": High,
            "Open": Open,
            "Close Price": Close_Price,
            "Crypto Volume": Crypto_Volume,
            "Pair Volume": Pair_Volume,
        }
        rawdata_df = pd.DataFrame.from_dict(rawdata, orient="index")
        rawdata_df = rawdata_df.transpose()
        dataframe = dataframe.append(rawdata_df, ignore_index=True)

    return dataframe
