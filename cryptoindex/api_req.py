import json
from datetime import datetime, timezone

import requests


def today_ts():

    today = datetime.now().strftime("%Y-%m-%d-%H")
    today = datetime.strptime(today, "%Y-%m-%d-%H")
    today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())

    return str(today_TS)


# COINBASE-PRO


# REST API request for coinbase-pro exchange.
# It requires:
# crypto Pair = crypto-fiat (ex: BTC - USD)
# start date = ISO 8601
# end date = ISO 8601
# granularity = in seconds. Ex 86400 = 1 day
# this api gives back 300 responses max for each request.


def coinbase_ticker(Crypto, Fiat, collection):

    asset = Crypto.upper()
    fiat = Fiat.upper()

    entrypoint = "https://api.pro.coinbase.com/products"
    key = "/" + asset + "-" + fiat + "/ticker"
    # print(key)
    request_url = entrypoint + key
    response = requests.get(request_url)
    response = response.json()
    # print(response)
    try:

        r = response
        pair = asset + fiat
        print(pair)
        exchange = "coinbase-pro"
        time = today_ts()
        date = datetime.now()
        ticker_time = r["time"]
        price = r["price"]
        volume = r["volume"]
        size = r["size"]
        bid = r["bid"]
        ask = r["ask"]
        traded_id = r["trade_id"]

        rawdata = {
            "Pair": pair,
            "Exchange": exchange,
            "Time": time,
            "date": date,
            "ticker_time": ticker_time,
            "Close Price": price,
            "Crypto Volume": volume,
            "size": size,
            "bid": bid,
            "ask": ask,
            "traded_id": traded_id,
        }

        collection.insert_one(rawdata)

        return pair

    except KeyError:

        err = "This key doesn't exist"
        print(err)

        return err


# KRAKEN

# The REST API OHLC endpoint only provides a limited amount of historical data, specifically
# 720 data points of the requested interval.
# unfortunally the since option seems to not work. so here the date_gen fuction is useless

# Kraken ticker


def kraken_ticker(Crypto, Fiat, collection):

    asset = Crypto.lower()
    fiat = Fiat.lower()

    if asset == "btc":
        asset = "xbt"
    elif (asset == "dog" or asset == "doge"):
        asset = "xdg"

    entrypoint = "https://api.kraken.com/0/public/Ticker?pair="
    key = asset + fiat
    # print(key)
    request_url = entrypoint + key
    response = requests.get(request_url)
    response = response.json()
    try:
        asset = asset.upper()
        fiat = fiat.upper()

        if (
        fiat != "USDT"
        or fiat != "USDC"
        or fiat != "CHF"
        or asset == "XBT"
        or asset == "ETH"
        or (asset == "XRP" and fiat != "GBP")
        or asset == "ZEC"
        or asset == "XDG"
        or asset == "XLM"
        or asset == "XMR"
        or (asset == "LTC" and fiat != "GBP")
        or asset == "ETC"
        ):

            pair = "X" + asset + "Z" + fiat
        else:
           pair = asset + fiat
        exchange = "kraken"
        # if (
        #     fiat == "USDT"
        #     or fiat == "USDC"
        #     or fiat == "CHF"
        #     or asset == "ADA"
        #     or asset == "EOS"
        #     or asset == "BCH"
        #     or (asset == "XRP" and fiat == "GBP")
        #     or (asset == "LTC" and fiat == "GBP")
        # ):
        #     pair = asset + fiat
        r = response["result"][pair]
        pair = asset + fiat
        print(pair)
        time = today_ts()
        date = datetime.now()
        price = r["c"][0]
        crypto_volume = r["v"][1]

        rawdata = {
            "Pair": pair,
            "Exchange": exchange,
            "Time": time,
            "date": date,
            "Close Price": price,
            "Crypto Volume": crypto_volume,
        }

        collection.insert_one(rawdata)

        return pair

    except KeyError:

        err = "This key doesn't exist"
        print(err)

        return err


# BITTREX

# https://bittrex.github.io/api/v3 api actually not working for historical data


def bittrex_ticker(Crypto, Fiat, collection):

    asset = Crypto.lower()
    fiat = Fiat.lower()
    entrypoint = "https://api.bittrex.com/api/v1.1/public/getmarketsummary?market="
    key = fiat + "-" + asset
    request_url = entrypoint + key
    print(request_url)

    response = requests.get(request_url)

    response = response.json()

    try:
        r = response["result"][0]
        pair = asset.upper() + fiat.upper()
        exchange = "bittrex"
        time = today_ts()
        date = datetime.now()
        ticker_time = r['TimeStamp']
        price = r["Last"]
        volume = r["Volume"]
        basevolume = r["BaseVolume"]
        high = r["High"]
        low = r["Low"]
        openbuyorders = r["OpenBuyOrders"]
        opensellorders = r["OpenSellOrders"]
        prevday = r["PrevDay"]

        rawdata = {
            "Pair": pair,
            "Exchange": exchange,
            "Time": time,
            "date": date,
            "ticker_time": ticker_time,
            "Close Price": price,
            "Crypto Volume": volume,
            "Pair Volume": basevolume,
            "high": high,
            "low": low,
            "openbuyorders": openbuyorders,
            "opensellorders": opensellorders,
            "prevday": prevday,
        }

        collection.insert_one(rawdata)

        return pair

    except KeyError:

        err = "This key doesn't exist"
        print(err)

        return err

    except TypeError:

        err = "Nonetype object"

        print(err)

        return err


# poloniex_ticker


def poloniex_ticker(Crypto, Fiat, collection):

    asset = Crypto.upper()
    stbc = Fiat.upper()
    pair = stbc + "_" + asset
    entrypoint = "https://poloniex.com/public?command=returnTicker"
    request_url = entrypoint
    response = requests.get(request_url)
    response = response.json()

    try:
        response_short = response[pair]
        r = response_short
        time = today_ts()
        date = datetime.now()
        price = r["last"]
        exchange = "poloniex"
        lowestAsk = r["lowestAsk"]
        highestBid = r["highestBid"]
        percentChange = r["percentChange"]
        base_volume = r["baseVolume"]
        crypto_volume = r["quoteVolume"]
        isFrozen = r["isFrozen"]
        high24hr = r["high24hr"]
        low24hr = r["low24hr"]

        rawdata = {
            "Pair": pair,
            "Exchange": exchange,
            "Time": time,
            "date": date,
            "Close Price": price,
            "lowestAsk": lowestAsk,
            "highestBid": highestBid,
            "percentChange": percentChange,
            "Pair Volume": base_volume,
            "Crypto Volume": crypto_volume,
            "isFrozen": isFrozen,
            "high24hr": high24hr,
            "low24hr": low24hr,
        }

        collection.insert_one(rawdata)

        return pair

    except KeyError:

        err = "This key doesn't exist"
        print(err)

        return err

    return


# ITBIT

# https://www.itbit.com/api api actually not working for historical data

# instead of BTC here the call is with XBT


def itbit_ticker(Crypto, Fiat, collection):

    asset = Crypto.upper()
    fiat = Fiat.upper()

    if asset == "BTC":
        asset = "XBT"

    entrypoint = "https://api.itbit.com/v1/markets/"
    key = asset + fiat + "/ticker"
    request_url = entrypoint + key
    response = requests.get(request_url)
    response = response.json()
    if asset == "XBT":
        asset = "BTC"

    try:
        r = response
        pair = asset + fiat
        exchange = "itbit"
        time = today_ts()
        date = datetime.now()
        price = r["lastPrice"]
        volume = r["volume24h"]
        volumeToday = r["volumeToday"]
        high24h = r["high24h"]
        low24h = r["low24h"]
        highToday = r["highToday"]
        lowToday = r["lowToday"]
        openToday = r["openToday"]
        vwapToday = r["vwapToday"]
        vwap24h = r["vwap24h"]

        rawdata = {
            "Pair": pair,
            "Exchange": exchange,
            "Time": time,
            "date": date,
            "Close Price": price,
            "Crypto Volume": volume,
            "volumeToday": volumeToday,
            "high24h": high24h,
            "low24h": low24h,
            "highToday": highToday,
            "lowToday": lowToday,
            "openToday": openToday,
            "vwapToday": vwapToday,
            "vwap24h": vwap24h,
        }

        collection.insert_one(rawdata)

        return pair

    except KeyError:

        err = "This key doesn't exist"
        print(err)

        return err


# BITFLYER

# https://lightning.bitflyer.com/docs?lang=en api actually not working for historical data


def bitflyer_ticker(Crypto, Fiat, collection):

    asset = Crypto.upper()
    fiat = Fiat.upper()
    entrypoint = "https://api.bitflyer.com/v1/"
    key = "getticker?product_code=" + asset + "_" + fiat
    request_url = entrypoint + key

    response = requests.get(request_url)
    response = response.json()

    try:

        r = response
        pair = asset + fiat
        exchange = "bitflyer"
        time = today_ts()
        date = datetime.now()
        ticker_time = r["timestamp"]
        price = r["ltp"]
        volume = r["volume"]
        volume_by_product = r["volume_by_product"]
        best_bid = r["best_bid"]
        best_ask = r["best_ask"]
        best_bid_size = r["best_bid_size"]
        best_ask_size = r["best_ask_size"]
        total_bid_depth = r["total_bid_depth"]

        rawdata = {
            "Pair": pair,
            "Exchange": exchange,
            "Time": time,
            "date": date,
            "ticker_time": ticker_time,
            "Crypto Volume": volume_by_product,  # changed from "volume" into present
            "Close Price": price,
            "other_volume": volume,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "best_bid_size": best_bid_size,
            "best_ask_size": best_ask_size,
            "total_bid_depth": total_bid_depth,
        }

        collection.insert_one(rawdata)

        return pair

    except KeyError:

        err = "This key doesn't exist"
        print(err)

        return err


# GEMINI


def gemini_ticker(Crypto, Fiat, collection):

    asset = Crypto.lower()
    fiat = Fiat.lower()
    entrypoint = "https://api.gemini.com/v1/pubticker/"
    key = asset + fiat
    request_url = entrypoint + key

    response = requests.get(request_url)
    response = response.json()

    try:
        asset = asset.upper()
        fiat = fiat.upper()
        r = response
        pair = asset + fiat
        exchange = "gemini"
        time = today_ts()
        date = datetime.now()
        ticker_time = r["volume"]["timestamp"]
        price = r["last"]
        asset = asset.upper()
        volume = r["volume"][asset]
        bid = r["bid"]
        ask = r["ask"]

        rawdata = {
            "Pair": pair,
            "Exchange": exchange,
            "Time": time,
            "date": date,
            "ticker_time": ticker_time,
            "Close Price": price,
            "Crypto Volume": volume,
            "bid": bid,
            "ask": ask,
        }

        collection.insert_one(rawdata)

        return pair

    except KeyError:

        err = "This key doesn't exist"
        print(err)

        return err


# BITSTAMP

# https://www.bitstamp.net/api/ api actually not working for historical data


def bitstamp_ticker(Crypto, Fiat, collection):

    asset = Crypto.lower()
    fiat = Fiat.lower()
    entrypoint = "https://www.bitstamp.net/api/v2/ticker/"
    key = asset + fiat
    request_url = entrypoint + key

    response = requests.get(request_url)

    try:
        response = response.json()
        r = response
        pair = asset.upper() + fiat.upper()
        exchange = "bistamp"
        time = today_ts()
        date = datetime.now()
        ticker_time = r["timestamp"]
        price = r["last"]
        volume = r["volume"]
        high = r["high"]
        low = r["low"]
        bid = r["bid"]
        ask = r["ask"]
        vwap = r["vwap"]
        Open = r["open"]

        rawdata = {
            "Pair": pair,
            "Exchange": exchange,
            "Time": time,
            "date": date,
            "ticker_time": ticker_time,
            "Close Price": price,
            "Crypto Volume": volume,
            "high": high,
            "low": low,
            "bid": bid,
            "ask": ask,
            "vwap": vwap,
            "open": Open,
        }

        collection.insert_one(rawdata)

        return pair

    except KeyError:

        err = "This key doesn't exist"
        print(err)

        return err

    except json.decoder.JSONDecodeError:

        err = "No value in response"
        print(err)

        return err
