from cryptoindex.cw_daily_func import (
    cw_daily_download, cw_daily_operation
)
from cryptoindex.ecb_daily_func import (
    ecb_daily_op
)
from cryptoindex.index_func import (
    index_daily
)
from cryptoindex.exc_func import exc_daily_operation

ecb_daily_op()
cw_daily_operation()
exc_daily_operation()
index_daily()