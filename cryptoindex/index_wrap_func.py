from cryptoindex.ecb_daily_func import (
    ecb_hist_op
)
from cryptoindex.cw_hist_func import (
    cw_hist_operation
)
from cryptoindex.index_hist import (
    index_hist_total
)
from cryptoindex.exc_func import (
    data_feed_op, exc_hist_op
)


def hist_complete(coll_to_use="coll_data_feed"):

    ecb_hist_op()
    cw_hist_operation()
    exc_hist_op()
    data_feed_op()
    index_hist_total(coll_to_use)

    return None
