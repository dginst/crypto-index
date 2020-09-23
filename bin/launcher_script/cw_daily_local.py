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

    ecb_daily_op(day)
    cw_daily_operation(day)
    # exc_daily_op(day)
    index_daily(coll_to_use="coll_cw_final", day=day)

    return None


# daily_op("2020-09-18")
# mongo_daily_delete("2020-09-21", "index")
# daily_op()
# index_daily(coll_to_use="coll_cw_final")
volume_checked_tot = query_mongo(DB_NAME,
                                 MONGO_DICT.get("coll_vol_chk"))

last_day_with_val = max(volume_checked_tot.Time)

volume_checked_df = volume_checked_tot.loc[volume_checked_tot.Time
                                           == last_day_with_val]

print(volume_checked_df)
