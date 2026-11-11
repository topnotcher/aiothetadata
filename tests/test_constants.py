import pytest

from aiothetadata.constants import *
from aiothetadata.constants import _EXCHANGE_NAMES
from aiothetadata._trade_conditions import _TRADE_CONDITIONS


@pytest.mark.parametrize('prop', ('cancel', 'late_report', 'auto_executed', 'volume', 'high', 'low', 'last'))
def test_trade_condition_properties(prop):
    for condition in TradeCondition:
        assert isinstance(getattr(condition, prop), bool)


def test_trade_conditions_metadata():
    enum_values = {int(v) for v in TradeCondition}
    meta_values = set(_TRADE_CONDITIONS.keys())

    assert enum_values == meta_values


def test_exchange_codes_match_names():
    # https://thetadata.net/docs/Articles/Errors-Exchanges-Conditions/Exchanges.html lists codes 0-79 (2026-11-10).
    assert {int(e) for e in Exchange} == set(_EXCHANGE_NAMES.keys()) == set(range(80))
    for exchange in Exchange:
        assert exchange.description


@pytest.mark.parametrize('code, symbol', ((74, 'CGI'), (76, 'SPHR'), (77, '24X'), (78, 'TXSE'), (79, 'MX2'),
                                          (56, 'CME Floor')))
def test_exchange_from_symbol(code, symbol):
    assert Exchange.from_symbol(symbol) == Exchange(code)
