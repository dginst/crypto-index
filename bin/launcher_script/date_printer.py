from cryptoindex.calc import (
    next_start, next_quarterly_period, start_q, stop_q, board_meeting_day, day_before_board,
    next_start
)
from cryptoindex.index_hist import hist_time_single_date
from cryptoindex.data_setup import (timestamp_to_human)

from cryptoindex.index_func import days_variable

day = None

start_q_list = next_start()
board_eve_list = day_before_board()
board_day = board_meeting_day()
start_q = start_q()
print(start_q_list)
print(board_eve_list)
print(board_day)
print(start_q)
print(timestamp_to_human(start_q_list[1: len(start_q_list)]))

day_TS, _ = days_variable(day)
# day_TS = day_TS + 86400
print(day_TS)

if day_TS in start_q_list:

    print("First day of the quarter")


elif day_TS in board_eve_list:

    print("Board day")

else:

    print("Normal day")


(last_reb_start, last_reb_stop, next_reb_stop,
 curr_board_eve) = hist_time_single_date()

print(timestamp_to_human([last_reb_start]))
print(timestamp_to_human([last_reb_stop]))
print(timestamp_to_human([next_reb_stop]))
print(timestamp_to_human([curr_board_eve]))

rebalance_period = next_quarterly_period(initial_val=1)
for start, stop in rebalance_period:
    print(timestamp_to_human([start]))
    print(timestamp_to_human([stop]))
