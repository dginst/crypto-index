
import numpy as np
from datetime import datetime, timezone

from cryptoindex.data_setup import date_gen, timestamp_to_human, timestamp_vector


def test_date_gen():

    # testing the function with start_date and end_date
    # date mm-dd-yyy
    start_date = "01-01-2020"
    end_date = "01-06-2020"

    ts = np.array([1577836800, 1577923200, 1578009600, 1578096000, 1578182400])
    date = date_gen(start_date, end_date)

    assert np.array_equal(date, ts)

    # testing the function with only start_date
    day_in_sec = 86400
    today = datetime.now().strftime("%m-%d-%Y")
    today = datetime.strptime(today, "%m-%d-%Y")

    # without EOD defatult arg the fun create an array of date
    # utill yesterday
    yesterday_ts = int(today.replace(tzinfo=timezone.utc).timestamp()) - day_in_sec

    date2 = date_gen(start_date)

    assert yesterday_ts == date2[-1]

    # without a default EOD arg the function generate today date as well
    date = date_gen(start_date, EoD='N')
    today_ts = int(today.replace(tzinfo=timezone.utc).timestamp())

    assert today_ts == date[-1]

    # with timeST != 'Y' the function generates an array or list
    # of str date in 'mm-dd-yyyy' format
    # with clss != 'array', the function generates a list of dates
    date = date_gen(start_date, timeST='N', clss='list')

    assert type(date) is list
    assert date[0] == start_date


def test_timestamp_to_human():

    # date in format yyyy-mm-dd
    check_list = ['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04', '2020-01-05']

    # array of dates in timestamp format
    ts_array = np.array([1577836800, 1577923200, 1578009600, 1578096000, 1578182400])
    date = timestamp_to_human(ts_array)

    assert date == check_list


def test_timestamp_vector():

    # date in timestamp epoch format int()
    ts_date = np.array([1577750400, 1577836800, 1577923200])

    # 2019-12-31 in timestamp format
    start_date = 1577750400

    # 2020-01-03 in timestamp format
    stop_date = 1578009600

    # calling the function to test
    date = timestamp_vector(start_date, stop_date)

    assert np.array_equal(ts_date, date)
