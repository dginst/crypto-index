import logging

from cryptoindex.cw_daily_func import cw_daily_operation
from cryptoindex.cw_hist_func import cw_hist_operation, cw_hist_download_op
from cryptoindex.ecb_daily_func import ecb_daily_op, ecb_hist_op, ecb_hist_no_download
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

    ecb_hist_no_download()
    cw_hist_operation()
    exc_hist_op()
    hist_data_feed_op()
    index_hist_total(coll_to_use)


def daily_complete():

    logging.info("ecb_daily_op function start")
    ecb_daily_op()
    logging.info("ecb_daily_op function end")

    logging.info("cw_daily_operation function start")
    cw_daily_operation()
    logging.info("cw_daily_operation function end")

    logging.info("exc_daily_op function start")
    exc_daily_op()
    logging.info("exc_daily_op function end")

    logging.info("exc_daily_feed function start")
    exc_daily_feed()
    logging.info("exc_daily_feed function end")

    logging.info("index_daily function start")
    index_daily()
    logging.info("index_daily function end")
