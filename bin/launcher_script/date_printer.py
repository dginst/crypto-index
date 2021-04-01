from cryptoindex.calc import (
    next_start, next_quarterly_period, start_q, stop_q, board_meeting_day, day_before_board,
    next_start
)

from cryptoindex.data_setup import (timestamp_to_human)

from cryptoindex.index_func import days_variable

day = None

start_q_list = next_start()
board_eve_list = day_before_board()
print(start_q_list)
print(board_eve_list)

day_TS, _ = days_variable(day)
day_TS = day_TS + 86400
print(day_TS)

if day_TS in start_q_list:

    print("First day of the quarter")


elif day_TS in board_eve_list:

    print("Board day")

else:

    print("Normal day")
