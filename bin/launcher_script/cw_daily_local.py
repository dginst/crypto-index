from cryptoindex.cw_daily_func import (
    cw_daily_operation
)
from cryptoindex.ecb_daily_func import (
    ecb_daily_op
)
from cryptoindex.index_func import (
    index_daily
)
from cryptoindex.exc_func import exc_daily_op


def daily_op(day=None):

    ecb_daily_op(day)
    cw_daily_operation(day)
    # exc_daily_op(day)
    index_daily(day)

    return None


daily_op("2020-09-18")
