from cryptoindex.ecb_daily_func import (
    ecb_hist_op
)
from cryptoindex.cw_hist_func import (
    cw_hist_operation
)
from cryptoindex.index_hist import (
    index_hist_total
)


def hist_complete():

    ecb_hist_op()
    cw_hist_operation()
    index_hist_total()

    return None
