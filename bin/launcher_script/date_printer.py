from cryptoindex.calc import (
    next_start, next_quarterly_period, start_q, stop_q, board_meeting_day, day_before_board,
    next_start
)

from cryptoindex.data_setup import (timestamp_to_human)


print(timestamp_to_human(start_q()))
print(timestamp_to_human(next_start()))
print(timestamp_to_human(stop_q(next_start())))
print(timestamp_to_human(board_meeting_day()))
for x, y in next_quarterly_period(initial_val=0):
    print(timestamp_to_human([x]))
    print(timestamp_to_human([y]))

print(timestamp_to_human(start_q("03-23-2020")))
