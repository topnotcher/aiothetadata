import decimal
import datetime
import zoneinfo

import pytest

from aiothetadata.request import *
from aiothetadata.datetime import MarketTimeZone


Singapore = zoneinfo.ZoneInfo('Singapore')


PRICES = [
    (1337, 1337000),
    ('13.37', 13370),
    (13.37, 13370),
    (13.01 + .01 + .01, 13030),
    (decimal.Decimal('13.37'), 13370),
    (decimal.Decimal('13.3754'), 13375),
    (decimal.Decimal('13.3755'), 13376),
]
@pytest.mark.parametrize('value,result', PRICES)
def test_format_price(value, result):
    assert format_price(value) == result


DATES = [
    (20250211, '20250211'),
    ('20250211', '20250211'),
    (datetime.date(year=2025, month=2, day=11), '20250211'),
    (datetime.datetime(year=2025, month=2, day=11, hour=11, minute=12), '20250211'),
    (datetime.datetime(year=2025, month=2, day=11, hour=5, minute=12, tzinfo=Singapore), '20250210'),
]
@pytest.mark.parametrize('value,result', DATES)
def test_format_date(value, result):
    assert format_date(value) == result


def test_format_date_invalid():
    with pytest.raises(ValueError):
        format_date(None)


TIMES = [
    ('13:37:13', (13 * 3600 + 60 * 37 + 13) * 1000),
    (
        datetime.datetime(year=2025, month=2, day=11, hour=13, minute=37, second=13, microsecond=313000),
        (13 * 3600 + 60 * 37 + 13) * 1000 + 313
    ),
    (
        datetime.time(hour=13, minute=37, second=13, microsecond=313000),
        (13 * 3600 + 60 * 37 + 13) * 1000 + 313
    ),
    (
        datetime.time(hour=13, minute=37, second=13, microsecond=313000, tzinfo=MarketTimeZone),
        (13 * 3600 + 60 * 37 + 13) * 1000 + 313
    )
]
@pytest.mark.parametrize('value,result', TIMES)
def test_format_time(value, result):
    assert format_time(value) == result


def test_format_time_invalid():
    with pytest.raises(ValueError):
        format_time(None)

    with pytest.raises(ValueError):
        format_time(datetime.time(12, 13, 14, tzinfo=Singapore))


DATE_TIMES = [
    ('20250211 13:37:13', '20250211', '13:37:13'),
    (
        datetime.datetime(year=2025, month=2, day=11, hour=13, minute=37, second=13, microsecond=313000),
        '20250211',
        datetime.time(hour=13, minute=37, second=13, microsecond=313000),
    ),
    (
        datetime.datetime(year=2025, month=2, day=11, hour=5, minute=12, tzinfo=Singapore),
        '20250210',
        '16:12:00',
    ),
]
@pytest.mark.parametrize('value,date_value,time_value', DATE_TIMES)
def test_format_date_time(value, date_value, time_value):
    assert format_date_time(value) == (format_date(date_value), format_time(time_value))
