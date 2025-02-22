import time as time_module

from aiothetadata.datetime import *


def test_datetime_now():
    assert datetime.now().tzinfo == MarketTimeZone


def test_datetime_default_tzinfo():
    dt = datetime(2025, 2, 22, 17, 59)
    assert dt.tzinfo == MarketTimeZone


def test_datetime_fromtimestamp():
    dt = datetime.fromtimestamp(time_module.time())
    assert dt.tzinfo == MarketTimeZone


def test_time_default_tzinfo():
    t = time(18, 1, 0)
    assert t.tzinfo == MarketTimeZone


def test_datetimetime_strptime():
    t = datetime.strptime('180100', '%H%M%S')
    assert t.tzinfo == MarketTimeZone
