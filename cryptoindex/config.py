START_DATE = "01-01-2016"

FIRST_BOARD_DATE = "12-21-2015"

ECB_START_DATE_D = "2015-12-31"

ECB_START_DATE = "12-31-2015"

EXC_START_DATE = "04-18-2020"

DAY_IN_SEC = 86400

SYNT_PTF_VALUE = 100

PAIR_ARRAY = ["gbp", "usd", "cad", "jpy", "eur", "usdt", "usdc"]
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur', 'usdt', 'usdc']

ECB_FIAT = ["USD", "GBP", "CAD", "JPY"]

CONVERSION_FIAT = ["gbp", "cad", "jpy", "eur"]

STABLE_COIN = ["usdt", "usdc"]

CRYPTO_ASSET = [
    "BTC",
    "ETH",
    "XRP",
    "LTC",
    "BCH",
    "EOS",
    "ETC",
    "ZEC",
    "XLM",
    "XMR",
    "BSV",
    "SHIB",
    "MATIC",
    "ADA",
    "AVAX",
    "DOGE",
    "DOT",
    "LUNA",
    "SOL"
]
# crypto complete ['BTC', 'ETH', 'XRP', 'LTC', 'BCH', 'EOS',
# 'ETC', 'ZEC', 'ADA', 'XLM', 'XMR', 'BSV']

EXCHANGES = [
    "coinbase-pro",
    "poloniex",
    "bitstamp",
    "gemini",
    "bittrex",
    "kraken",
    "bitflyer",
]
# exchange complete = [ 'coinbase-pro', 'poloniex', 'bitstamp',
# 'gemini', 'bittrex', 'kraken', 'bitflyer']

DB_NAME = "index"

MONGO_DICT = {
    "coll_cw_raw": "CW_rawdata",
    "coll_cw_clean": "CW_cleandata",
    "coll_cw_conv": "CW_converted_data",
    "coll_vol_chk": "CW_volume_checked_data",
    "coll_cw_final": "CW_final_data",
    "coll_cw_keys": "CW_keys",

    "coll_exc_raw": "EXC_rawdata",
    "coll_exc_uniform": "EXC_uniform",
    "coll_exc_clean": "EXC_cleandata",
    "coll_exc_final": "EXC_final_data",
    "coll_ecb_raw": "ecb_raw",
    "coll_ecb_clean": "ecb_clean",
    "coll_exc_keys": "EXC_keys",
    "coll_stable_rate": "stable_coin_rates",
    "coll_data_feed": "index_data_feed",
    "coll_log1": "index_logic_matrix_one",
    "coll_log2": "index_logic_matrix_two",
    "coll_price": "crypto_price",
    "coll_volume": "crypto_volume",
    "coll_ret": "crypto_price_return",
    "coll_ewma": "index_EWMA",
    "coll_ewma_checked": "index_EWMA_logic_checked",
    "coll_weights": "index_weights",
    "coll_rel_synt": "index_rel_synth_matrix",
    "coll_synt": "index_synth_matrix",
    "coll_divisor": "index_divisor",
    "coll_divisor_res": "index_divisor_reshaped",
    "coll_raw_index": "index_level_raw",
    "coll_1000_index": "index_level_1000",
    "coll_all_exc": "all_exc_volume"
}

CLEAN_DATA_HEAD = [
    "Time", "Close Price",
    "Crypto Volume", "Pair Volume",
    "Pair", "Exchange"
]
# CRYPTO_DF_COL =

USDT_EXC_LIST = ["poloniex", "bittrex", "kraken"]

USDC_EXC_LIST = ["poloniex", "coinbase-pro", "kraken"]

CW_RAW_HEAD = ["Exchange",
               "Pair",
               "Time",
               "Low",
               "High",
               "Open",
               "Close Price",
               "Crypto Volume",
               "Pair Volume"]
