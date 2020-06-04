"Tests for `cryptoindex.calc` module."

# standard library import
from datetime import datetime, timezone

# third party import
import numpy as np

# local import
from cryptoindex import calc


# ###########################################################################
# ######################## DATE SETTINGS FUNCTIONS ##########################


def test_Bday():

    # is Bday
    bday = "05-11-2020"
    assert calc.is_business_day(bday)

    # is not Bday
    not_bday = "05-10-2020"
    assert not calc.is_business_day(not_bday)


def test_date_validation():

    # verificare
    date = datetime(2020, 1, 1)
    assert calc.validate_date(date)
    # not datetime
    date2 = "05-11-2020"
    assert not calc.validate_date(date2)


def test_previous_business_day():

    # is Bday 'mm-dd-yyyy'
    bday = "05-08-2020"
    bday = datetime.strptime(bday, "%m-%d-%Y")
    test_day = calc.previous_business_day(bday)
    assert test_day == bday

    # is Saturday 'mm-dd-yyyy'
    sat = "05-09-2020"
    sat = datetime.strptime(sat, "%m-%d-%Y")
    assert calc.previous_business_day(sat) == bday

    # is Sunday
    sun = "05-10-2020"
    sun = datetime.strptime(sun, "%m-%d-%Y")
    assert calc.previous_business_day(sun) == bday


def test_perdelta():

    start_date = datetime.strptime("01-01-2019", "%m-%d-%Y")
    stop_date = datetime.strptime("01-01-2020", "%m-%d-%Y")
    check_range = ["01-01-2019", "04-01-2019", "07-01-2019", "10-01-2019"]

    test_range = calc.perdelta(start_date, stop_date)
    # creating a list from perdelta func
    test_range = [x.strftime("%m-%d-%Y") for x in test_range]

    assert test_range == check_range


def test_start_q_fix():

    str_date = ["01-01-2019", "04-01-2019", "07-01-2019", "10-01-2019"]

    # str_date in epoch timestamp
    ts_date = np.array([1.5463008e09, 1.5540696e09, 1.5619320e09, 1.5698808e09])

    # coverting string in datetime and then in epoch timestamp.
    ts_array = np.array(
        [int(datetime.strptime(x, "%m-%d-%Y").timestamp()) for x in str_date]
    )

    ts_gen = calc.start_q_fix(ts_array)

    assert np.array_equal(ts_date, ts_gen)


def test_start_q():

    # str_date = ['01-01-2019', '04-01-2019', '07-01-2019', '10-01-2019']
    # str_date in epoch timestamp
    ts_date = np.array([1.5463008e09, 1.5540768e09, 1.5619392e09, 1.5698880e09])

    ts_gen = calc.start_q("01-01-2019", "01-01-2020")

    assert np.array_equal(ts_date, ts_gen)

    # checking if without declaring the second positional argument of the
    # start_q function, the function is still working
    start = datetime.strptime("01-01-2019", "%m-%d-%Y")
    today = datetime.now().strftime("%m-%d-%Y")
    today = datetime.strptime(today, "%m-%d-%Y")

    ts_date = calc.perdelta(start, today)

    # converting the perdelta-generated array of dates tu utc-timestamp
    ts_date = np.array([x.replace(tzinfo=timezone.utc).timestamp() for x in ts_date])

    ts_gen = calc.start_q(start)

    assert np.array_equal(ts_date, ts_gen)


def test_stop_q():

    # dates in epoch timestamp of the dates:
    # respectively:  01-01-2019', '04-01-2019', '07-01-2019', '10-01-2019'
    # (mm:dd:yyyy format)
    ts_dates = np.array([1.5463008e09, 1.5540768e09, 1.5619392e09, 1.5698880e09])

    # dates in epoch timestamp of the dates:
    # respectively:  03-31-2019', '06-30-2019', '09-30-2019', '12-31-2019'
    # (mm:dd:yyyy format)
    ts_stopdates = np.array([1.5539904e09, 1.5618528e09, 1.5698016e09, 1.5777540e09])

    stop_date = calc.stop_q(ts_dates)

    assert np.array_equal(ts_stopdates, stop_date)


def test_board_meeting_day():

    start_date = "03-21-2019"
    stop_date = "01-01-2020"

    # dates in epoch timestamp of the dates:
    # respectively:  03-21-2019', '06-21-2019', '09-20-2019', '12-20-2019'
    # (mm:dd:yyyy format)
    ts_bmd_days = np.array([1.5531264e09, 1.5610752e09, 1.5689376e09, 1.5768000e09])

    ts_gen = calc.board_meeting_day(start_date, stop_date)

    assert np.array_equal(ts_bmd_days, ts_gen)

    # checking if without declaring the second positional argument of the
    # board_meeting_day function, the function is still working
    start = datetime.strptime("01-01-2019", "%m-%d-%Y")
    today = datetime.now().strftime("%m-%d-%Y")
    today = datetime.strptime(today, "%m-%d-%Y")

    ts_date = calc.perdelta(start, today)

    # converting the perdelta-generated array of dates tu utc-timestamp
    ts_date = np.array([x.replace(tzinfo=timezone.utc).timestamp() for x in ts_date])
    start = "01-01-2019"
    ts_gen = calc.board_meeting_day(start)

    assert np.array_equal(ts_date, ts_gen)


def test_day_before_board():

    start_date = "03-21-2019"
    stop_date = "01-01-2020"

    # dates in epoch timestamp of the dates:
    # respectively:  03-20-2019', '06-20-2019', '09-19-2019', '12-19-2019'
    # (mm:dd:yyyy format)
    ts_bm_days = np.array([1.5530400e09, 1.5609888e09, 1.5688512e09, 1.5767136e09])

    ts_gen = calc.day_before_board(start_date, stop_date)

    assert np.array_equal(ts_bm_days, ts_gen)


def test_next_quarterly_peiod():

    start_date = "01-01-2019"
    stop_date = "01-01-2020"

    # dates in epoch timestamp of the dates:
    # respectively:  (04-01-2019', '06-30-2019'),
    #                ('07-01-2019', '09-30-2019'),
    #                ('10-01-2019', '12-31-2019')
    # (mm:dd:yyyy format)

    check_range = [
        (1554076800, 1561852800),
        (1561939200, 1569801600),
        (1569888000, 1577754000),
    ]

    test_range = calc.next_quarterly_period(start_date, stop_date)
    # creating a list from next_quarterly_period
    test_range = [(x, y) for x, y in test_range]

    assert check_range == test_range


def test_next_start():

    start_date = "01-01-2019"
    stop_date = "01-01-2020"

    st_date = np.array(
        [1.5463008e09, 1.5540768e09, 1.5619392e09, 1.5698880e09, 1.5778404e09]
    )
    st_gen = calc.next_start(start_date, stop_date)

    assert np.array_equal(st_date, st_gen)

    # checking if without declaring the second positional argument of the
    # next_start function, the function is still working
    start = datetime.strptime("01-01-2019", "%m-%d-%Y")
    today = datetime.now().strftime("%m-%d-%Y")
    today = datetime.strptime(today, "%m-%d-%Y")

    day_in_sec = 86400
    # converting the perdelta-generated array of dates tu utc-timestamp
    start_quarter = calc.start_q_fix(calc.start_q(start, today))
    stop_quarter = calc.stop_q(start_quarter)

    next_start_date = int(stop_quarter[len(stop_quarter) - 1]) + day_in_sec

    ts_date = np.append(start_quarter, next_start_date)

    start = "01-01-2019"
    ts_gen = calc.next_start(start)

    assert np.array_equal(ts_date, ts_gen)


# ###########################################################################

# #################### FIRST LOGIC MATRIX ###################################
