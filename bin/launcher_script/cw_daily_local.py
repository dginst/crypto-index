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

from cryptoindex.mongo_setup import mongo_daily_delete, query_mongo

from cryptoindex.config import (
    MONGO_DICT, DB_NAME
)


def daily_op(day=None):

    if day is not None:

        mongo_daily_delete(day, "ecb")
        mongo_daily_delete(day, "cw")
        mongo_daily_delete(day, "index")

    ecb_daily_op(day)
    cw_daily_operation(day)
    # exc_daily_op(day)
    index_daily(coll_to_use="coll_cw_final", day=day)


# mongo_daily_delete("2020-09-23", "index")
daily_op("2020-09-22")
daily_op("2020-09-23")
daily_op("2020-09-24")

# daily_op()
# index_daily(coll_to_use="coll_cw_final")
