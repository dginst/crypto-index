from logging import info

from cryptoindex.cw_daily_func import cw_daily_operation
from cryptoindex.cw_hist_func import cw_hist_download_op, cw_hist_operation
from cryptoindex.ecb_daily_func import (ecb_daily_op, ecb_hist_no_download,
                                        ecb_hist_op)
from cryptoindex.exc_func import (exc_daily_feed, exc_daily_op, exc_hist_op,
                                  hist_data_feed_op)
from cryptoindex.index_func import index_daily
from cryptoindex.index_hist import index_hist_total


# function that runs everything from scratch:
# 1) downloads all the used ECB history
# 2) downloads all yhe used CryptoWatch history
# 3) retrieve the EXC rawdata from DB
# 4) computes Index values
# this function takes long to run and has to be used only in inception or if
# something went missing and its not retrivable

def hist_complete(coll_to_use="coll_data_feed"):

    ecb_hist_op()
    cw_hist_download_op()
    cw_hist_operation()
    exc_hist_op()
    hist_data_feed_op()
    index_hist_total(coll_to_use)

# the function computes the Index values and all the related tables from the scratch
# WITHOUT downloading everything anew
# has to be used every time the code broke down (or the server stops working) and after having retrived
# and uploaded on Mongo the updated needed collections


def hist_no_download(coll_to_use="coll_data_feed"):

    ecb_hist_no_download()
    cw_hist_operation()
    exc_hist_op()
    hist_data_feed_op()
    index_hist_total(coll_to_use)


# function that runs everyday for the Index computation and related download operations

def daily_complete():

    info("ecb_daily_op function start")
    ecb_daily_op()
    info("ecb_daily_op function end")

    info("cw_daily_operation function start")
    cw_daily_operation()
    info("cw_daily_operation function end")

    info("exc_daily_op function start")
    exc_daily_op()
    info("exc_daily_op function end")

    info("exc_daily_feed function start")
    exc_daily_feed()
    info("exc_daily_feed function end")

    info("index_daily function start")
    index_daily()
    info("index_daily function end")


def hist_only_exc(coll_to_use="coll_data_feed"):

    exc_hist_op()
    hist_data_feed_op()
    index_hist_total(coll_to_use)