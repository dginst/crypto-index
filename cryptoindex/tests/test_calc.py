"Tests for `cryptoindex.calc` module."

# standard library import
from datetime import datetime

# third party import
import numpy as np

# local import
from cryptoindex import calc


# ###########################################################################
# ######################## DATE SETTINGS FUNCTIONS ##########################


def test_Bday():

    # is Bday
    bday = '05-11-2020'
    assert calc.is_business_day(bday) == True

    # is not Bday
    not_bday = '05-10-2020'
    assert calc.is_business_day(not_bday) != True


def test_date_validation():

    # verificare
    date = datetime(2020, 1, 1)
    assert calc.validate_date(date) == True
    # not datetime
    date2 = '05-11-2020'
    assert calc.validate_date(date2) == False


def test_previous_business_day():

    # is Bday 'mm-dd-yyyy'
    bday = '05-08-2020'
    bday = datetime.strptime(bday, '%m-%d-%Y')
    test_day = calc.previous_business_day(bday)
    assert test_day == bday

    # is Saturday 'mm-dd-yyyy'
    sat = '05-09-2020'
    sat = datetime.strptime(sat, '%m-%d-%Y')
    assert calc.previous_business_day(sat) == bday

    # is Sunday
    sun = '05-10-2020'
    sun = datetime.strptime(sun, '%m-%d-%Y')
    assert calc.previous_business_day(sun) == bday


def test_perdelta():

    start_date = datetime.strptime('01-01-2019', '%m-%d-%Y')
    stop_date = datetime.strptime('01-01-2020', '%m-%d-%Y')
    check_range = ['01-01-2019', '04-01-2019', '07-01-2019', '10-01-2019']

    test_range = calc.perdelta(start_date, stop_date)
    # creating a list from perdelta func
    test_range = [x.strftime('%m-%d-%Y') for x in test_range]

    assert test_range == check_range


def test_start_q_fix():

    str_date = ['01-01-2019', '04-01-2019', '07-01-2019', '10-01-2019']

    # str_date in epoch timestamp
    ts_date = np.array([1.5463008e+09, 1.5540696e+09,
                        1.5619320e+09, 1.5698808e+09])

    # coverting string in datetime and then in epoch timestamp.
    ts_array = np.array([int(datetime.strptime(x, '%m-%d-%Y').timestamp())
                         for x in str_date])

    ts_gen = calc.start_q_fix(ts_array)

    assert np.array_equal(ts_date, ts_gen)


def test_start_q():

    str_date = ['01-01-2019', '04-01-2019', '07-01-2019', '10-01-2019']

    # str_date in epoch timestamp
    ts_date = np.array([1.5463008e+09, 1.5540768e+09,
                        1.5619392e+09, 1.5698880e+09])

    ts_gen = calc.start_q('01-01-2019', '01-01-2020')

    assert np.array_equal(ts_date, ts_gen)


def test_stop_q():

    # dates in epoch timestamp of the dates:
    # respectively:  01-01-2019', '04-01-2019', '07-01-2019', '10-01-2019'
    # (mm:dd:yyyy format)
    ts_dates = np.array([1.5463008e+09, 1.5540768e+09,
                         1.5619392e+09, 1.5698880e+09])

    # dates in epoch timestamp of the dates:
    # respectively:  03-31-2019', '06-30-2019', '09-30-2019', '12-31-2019'
    # (mm:dd:yyyy format)
    ts_stopdates = np.array(
        [1.5539904e+09, 1.5618528e+09, 1.5698016e+09, 1.5777540e+09])

    stop_date = calc.stop_q(ts_dates)

    assert np.array_equal(ts_stopdates, stop_date)


def test_board_meeting_day():

    start_date = '03-21-2019'
    stop_date = '01-01-2020'

    # dates in epoch timestamp of the dates:
    # respectively:  01-01-2019', '04-01-2019', '07-01-2019', '10-01-2019'
    # (mm:dd:yyyy format)
    ts_bmd_days = np.array([1.5531264e+09,  1.5610752e+09,
                            1.5689376e+09, 1.5768000e+09])

    ts_gen = calc.board_meeting_day(start_date, stop_date)

    assert np.array_equal(ts_bmd_days, ts_gen)
