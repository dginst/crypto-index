from cryptoindex.ecb_daily_func import (
    ecb_hist_op, ecb_daily_op
)
from cryptoindex.cw_hist_func import (
    cw_hist_operation
)
from cryptoindex.index_hist import (
    index_hist_total
)
from cryptoindex.exc_func import (
    data_feed_op, exc_hist_op, hist_data_feed_op,
    exc_daily_op, exc_daily_feed
)
from cryptoindex.cw_daily_func import (
    cw_daily_operation
)
from cryptoindex.index_func import (
    index_daily
)


def hist_complete(coll_to_use="coll_data_feed"):

    # ecb_hist_op()
    # cw_hist_operation()
    # exc_hist_op()
    # hist_data_feed_op()
    index_hist_total(coll_to_use)

    return None


def hist_no_download(coll_to_use="coll_data_feed"):

    exc_hist_op()
    hist_data_feed_op()
    index_hist_total(coll_to_use)

    return None


def daily_complete():

    ecb_daily_op()
    cw_daily_operation()
    exc_daily_op()
    # hist_data_feed_op()
    # data_feed_op()
    exc_daily_feed()
    index_daily()

    return None
