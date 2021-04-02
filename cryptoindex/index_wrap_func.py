from cryptoindex.cw_daily_func import cw_daily_operation
from cryptoindex.cw_hist_func import cw_hist_operation, cw_hist_download_op
from cryptoindex.ecb_daily_func import ecb_daily_op, ecb_hist_op
from cryptoindex.exc_func import (data_feed_op, exc_daily_feed, exc_daily_op,
                                  exc_hist_op, hist_data_feed_op)
from cryptoindex.index_func import index_daily
from cryptoindex.index_hist import index_hist_total


def hist_complete(coll_to_use="coll_data_feed"):

    ecb_hist_op()
    cw_hist_download_op()
    cw_hist_operation()
    exc_hist_op()
    hist_data_feed_op()
    index_hist_total(coll_to_use)


def hist_no_download(coll_to_use="coll_data_feed"):

    cw_hist_operation()
    exc_hist_op()
    hist_data_feed_op()
    index_hist_total(coll_to_use)


def daily_complete():

    ecb_daily_op()
    cw_daily_operation()
    exc_daily_op()
    # hist_data_feed_op()
    # data_feed_op()
    exc_daily_feed()
    index_daily()
