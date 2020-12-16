from cryptoindex.cw_daily_func import (
    cw_daily_operation
)
from cryptoindex.ecb_daily_func import (
    ecb_daily_op
)
from cryptoindex.index_func import (
    index_daily
)
from cryptoindex.exc_func import (
    exc_daily_op, data_feed_op
)

ecb_daily_op()
cw_daily_operation()
exc_daily_op()
data_feed_op()
# index daily on default use coll_to_use="coll_data_feed" as param,
# use "coll_cw_final" otherwise
# index_daily()
