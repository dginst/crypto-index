from cryptoindex.ecb_daily_func import (
    ecb_hist_op
)
from cryptoindex.cw_hist_func import (
    cw_hist_operation
)
from cryptoindex.data_setup import (
    date_gen_no_holiday
)

from cryptoindex.index_hist import (
    index_hist_total
)

from cryptoindex.index_wrap_func import (
    hist_complete
)

from cryptoindex.exc_func import (
    data_feed_op, exc_hist_op, hist_data_feed_op
)


# exc_hist_op()
# data_feed_op()
# hist_data_feed_op()

# ecb_hist_op()
# cw_hist_operation()

# index_hist_total(coll_to_use="coll_cw_final")
hist_complete()
# index_hist_total()
