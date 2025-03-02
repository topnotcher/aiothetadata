from decimal import Decimal

import pytest

from aiothetadata.response import *
from aiothetadata.constants import *
from aiothetadata import datetime


@pytest.mark.asyncio
async def test_iter_csv():
    header = ['foo', 'bar', 'baz']
    data = (
        ('1', '2', '3'),
        ('4', '5', '6'),
        ('7', '8', '9'),
    )

    async def _async_gen():
        yield ','.join(header).encode()

        for row in data:
            yield ','.join(row).encode()


    expected_rows = [dict(zip(header, row)) for row in data]
    actual_rows = [row async for row in iter_csv(_async_gen())]

    assert actual_rows == expected_rows


def test_parse_date():
    assert parse_date('20250211') == datetime.date(2025, 2, 11)


def test_parse_time():
    assert parse_time(34513666) == datetime.time(9, 35, 13, 666000)


def test_parse_time_too_big():
    assert parse_time(86400000) == datetime.time(23, 59, 59, 999000)


def test_parse_date_time():
    data = (
        '20250211',
        34513666,
    )
    expected = datetime.datetime.combine(parse_date(data[0]), parse_time(data[1]))
    assert parse_date_time(*data) == expected


def test_parse_trade_fields():
    raw = {
        'ms_of_day': '35938270',
        'sequence': '1054514035',
        'ext_condition1': '17',
        'ext_condition2': '255',
        'ext_condition3': '255',
        'ext_condition4': '255',
        'condition': '130',
        'size': '1',
        'exchange': '5',
        'price': '4.6500', 
        'condition_flags': '0',
        'price_flags': '1',
        'volume_type': '0',
        'records_back': '7',
        'date': '20250218',
        'strike': '123456',
        'right': 'C',
    }
    parsed = {
        'price': Decimal('4.6500'),
        'sequence': 1054514035,
        'size': 1,
        'records_back': 7,
        'time': datetime.datetime(2025, 2, 18, 9, 58, 58, 270000),
        'exchange': Exchange.CBOE,
        'conditions': (TradeCondition.MULTI_LEG_AUTOELEC_TRADE, TradeCondition.POSIT),
        'right': OptionRight.CALL,
        'strike': Decimal('123.456'),
    }

    assert parse_trade_fields(raw) == parsed


def test_parse_quote_fields():
    raw = {
        'ms_of_day': '36000000',
        'bid_size': '169',
        'bid_exchange': '5',
        'bid': '5.0000',
        'bid_condition': '50',
        'ask_size': '30',
        'ask_exchange': '5',
        'ask': '5.2000',
        'ask_condition': '50',
        'date': '20250217',
        'strike': '123456',
        'right': 'C',
    }
    parsed = {
        'bid': Decimal('5.0000'),
        'ask': Decimal('5.2000'),
        'strike': Decimal('123.456'),
        'bid_size': 169,
        'ask_size': 30,
        'bid_condition': QuoteCondition.NATIONAL_BBO,
        'ask_condition': QuoteCondition.NATIONAL_BBO,
        'time': datetime.datetime(2025, 2, 17, 10, 0),
        'bid_exchange': Exchange.CBOE,
        'ask_exchange': Exchange.CBOE,
        'right': OptionRight.CALL,
    }

    assert parse_quote_fields(raw) == parsed


def test_parse_eod_report():
    raw = {
        'ms_of_day': '36000000',
        'ms_of_day2': '36061000',

        'bid_size': '169',
        'bid_exchange': '5',
        'bid': '5.0000',
        'bid_condition': '50',
        'ask_size': '30',
        'ask_exchange': '5',
        'ask': '5.2000',
        'ask_condition': '50',
        'date': '20250217',

        'open': '13.37',
        'high': '1337.13',
        'low': '9.15',
        'close': '100.12',
        'volume': '1337',
        'count': '10',
    }
    expected = {
        'bid': Decimal('5.0000'),
        'ask': Decimal('5.2000'),

        'bid_size': 169,
        'ask_size': 30,
        'bid_condition': QuoteCondition.NATIONAL_BBO,
        'ask_condition': QuoteCondition.NATIONAL_BBO,
        'time': datetime.datetime(2025, 2, 17, 10, 0),
        'bid_exchange': Exchange.CBOE,
        'ask_exchange': Exchange.CBOE,

        'last_trade': datetime.datetime(2025, 2, 17, 10, 1, 1),

        'open': Decimal('13.37'),
        'high': Decimal('1337.13'),
        'low': Decimal('9.15'),
        'close': Decimal('100.12'),
        'volume': 1337,
        'count': 10,
    }

    report = parse_eod_report(raw)

    assert report == expected


def test_parse_index_price_report():
    raw = {
        'ms_of_day': '36000000',
        'price': '313.3700',
        'date': '20250217',
    }
    parsed = {
        'price': Decimal('313.3700'),
        'time': datetime.datetime(2025, 2, 17, 10, 0),
    }

    assert parse_index_price_report(raw) == parsed
