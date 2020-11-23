from cryptoindex.ecb_daily_func import (
    ecb_hist_op
)
from cryptoindex.cw_hist_func import (
    cw_hist_operation
)


def hist_complete():

    ecb_hist_op()
    cw_hist_operation()

    return None
