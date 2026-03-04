import pytest
from decimal import Decimal

from aiothetadata.response import *
from aiothetadata import datetime
from aiothetadata.constants import *


@pytest.mark.asyncio
async def test_iter_csv():
    async def line_gen():
        yield b'foo,bar'
        yield b'"baz",qux'
        yield b'abc,"123"'

    gen = iter_csv(line_gen())

    rows = [row async for row in gen]

    assert rows[0] == {'foo': 'baz', 'bar': 'qux'}
    assert rows[1] == {'foo': 'abc', 'bar': '123'}


def test_parse_date():
    assert parse_date('20250221') == datetime.date(2025, 2, 21)
    assert parse_date('2025-02-21') == datetime.date(2025, 2, 21)


def test_parse_time():
    assert parse_time('36000000') == datetime.time(10, 0)


def test_parse_time_too_big():
    # $0.02 my ass
    assert parse_time('86400000') == datetime.time(23, 59, 59, 999000)


def test_parse_date_time():
    assert parse_date_time('20250221', '36000000') == datetime.datetime(2025, 2, 21, 10, 0)


def test_parse_strike():
    assert parse_strike('123.456') == Decimal('123.456')


def test_parse_trade_fields():
    raw = {
        'timestamp': '2025-02-18T09:58:58.270',
        'sequence': '1054514035',
        'ext_condition1': '17',
        'ext_condition2': '255',
        'ext_condition3': '255',
        'ext_condition4': '255',
        'condition': '130',
        'size': '1',
        'exchange': '5',
        'price': '4.6500',
        'strike': '123.456',
        'right': 'CALL',
        'symbol': 'SPX',
    }
    parsed = {
        'price': Decimal('4.6500'),
        'sequence': 1054514035,
        'size': 1,
        'time': datetime.datetime(2025, 2, 18, 9, 58, 58, 270000, tzinfo=datetime.MarketTimeZone),
        'exchange': Exchange.CBOE,
        'conditions': (TradeCondition.MULTI_LEG_AUTOELEC_TRADE, TradeCondition.POSIT),
        'right': OptionRight.CALL,
        'strike': Decimal('123.456'),
        'symbol': 'SPX',
    }

    assert parse_trade_fields(raw) == parsed


def test_parse_quote_fields():
    raw = {
        'timestamp': '2025-02-17T10:00:00',
        'bid_size': '169',
        'bid_exchange': '5',
        'bid': '5.0000',
        'bid_condition': '50',
        'ask_size': '30',
        'ask_exchange': '5',
        'ask': '5.2000',
        'ask_condition': '50',
        'strike': '123.456',
        'right': 'CALL',
        'symbol': 'SPX',
    }
    parsed = {
        'bid': Decimal('5.0000'),
        'ask': Decimal('5.2000'),
        'strike': Decimal('123.456'),
        'bid_size': 169,
        'ask_size': 30,
        'bid_condition': QuoteCondition.NATIONAL_BBO,
        'ask_condition': QuoteCondition.NATIONAL_BBO,
        'time': datetime.datetime(2025, 2, 17, 10, 0, tzinfo=datetime.MarketTimeZone),
        'bid_exchange': Exchange.CBOE,
        'ask_exchange': Exchange.CBOE,
        'right': OptionRight.CALL,
        'symbol': 'SPX',
    }

    assert parse_quote_fields(raw) == parsed


def test_parse_eod_report():
    raw = {
        'created': '2025-02-17T10:00:00',
        'last_trade': '2025-02-17T10:01:01',

        'bid_size': '169',
        'bid_exchange': '5',
        'bid': '5.0000',
        'bid_condition': '50',
        'ask_size': '30',
        'ask_exchange': '5',
        'ask': '5.2000',
        'ask_condition': '50',

        'open': '13.37',
        'high': '1337.13',
        'low': '9.15',
        'close': '100.12',
        'volume': '1337',
        'count': '10',
        'symbol': 'SPX',
        'strike': '123.456',
        'right': 'CALL',
    }
    expected = {
        'bid': Decimal('5.0000'),
        'ask': Decimal('5.2000'),

        'bid_size': 169,
        'ask_size': 30,
        'bid_condition': QuoteCondition.NATIONAL_BBO,
        'ask_condition': QuoteCondition.NATIONAL_BBO,
        'time': datetime.datetime(2025, 2, 17, 10, 0, tzinfo=datetime.MarketTimeZone),
        'bid_exchange': Exchange.CBOE,
        'ask_exchange': Exchange.CBOE,

        'last_trade': datetime.datetime(2025, 2, 17, 10, 1, 1, tzinfo=datetime.MarketTimeZone),

        'open': Decimal('13.37'),
        'high': Decimal('1337.13'),
        'low': Decimal('9.15'),
        'close': Decimal('100.12'),
        'volume': 1337,
        'count': 10,
        'symbol': 'SPX',
        'strike': Decimal('123.456'),
        'right': OptionRight.CALL,
    }

    report = parse_eod_report(raw)

    assert report == expected


def test_parse_index_price_report():
    raw = {
        'timestamp': '2025-02-17T10:00:00',
        'price': '313.3700',
    }
    parsed = {
        'price': Decimal('313.3700'),
        'time': datetime.datetime(2025, 2, 17, 10, 0, tzinfo=datetime.MarketTimeZone),
    }

    assert parse_index_price_report(raw) == parsed


def test_parse_first_order_greeks():
    # Field names and values taken from a live greeks_first_order response.
    raw = {
        'timestamp': '2026-03-04T08:49:44.910',
        'underlying_price': '6816.6300',
        'underlying_timestamp': '2026-03-03T16:03:53',
        'implied_vol': '0.1711',
        'iv_error': '0.0000',
        'delta': '-0.5588',
        'theta': '-3.8653',
        'vega': '398.2166',
        'rho': '-85.3322',
        'epsilon': '83.4944',
        'lambda': '-45.4320',
        'bid': '83.4000',
        'ask': '84.3000',
        'strike': '6850.000',
        'right': 'PUT',
        'symbol': 'SPXW',
    }
    expected = {
        'time': datetime.datetime(2026, 3, 4, 8, 49, 44, 910000, tzinfo=datetime.MarketTimeZone),
        'underlying_price': Decimal('6816.6300'),
        'iv': Decimal('0.1711'),
        'iv_error': Decimal('0.0000'),
        'delta': Decimal('-0.5588'),
        'theta': Decimal('-3.8653'),
        'vega': Decimal('398.2166'),
        'rho': Decimal('-85.3322'),
        'epsilon': Decimal('83.4944'),
        'leverage': Decimal('-45.4320'),
        'bid': Decimal('83.4000'),
        'ask': Decimal('84.3000'),
        'strike': Decimal('6850.000'),
        'right': OptionRight.PUT,
        'symbol': 'SPXW',
    }

    assert parse_first_order_greeks(raw) == expected
