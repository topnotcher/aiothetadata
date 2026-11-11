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


def test_exchange_from_code_known():
    assert Exchange.from_code(5) is Exchange.CBOE


def test_exchange_from_code_unknown_quacks_like_a_member(caplog):
    with caplog.at_level('WARNING', logger='aiothetadata.constants'):
        unknown = Exchange.from_code(250)
        again = Exchange.from_code(250)

    assert isinstance(unknown, UnknownExchange)
    assert isinstance(unknown, int)
    assert unknown == 250
    assert unknown.value == 250
    assert unknown.name == 'UNKNOWN_250'
    assert unknown.description == 'Unknown exchange (250)'
    assert repr(unknown) == '<UnknownExchange.UNKNOWN_250: 250>'
    assert str(unknown) == str(Exchange.CBOE).replace('5', '250') == '250'
    assert again == unknown
    assert len([r for r in caplog.records if '250' in r.getMessage()]) == 1


def test_quote_condition_from_code_unknown_quacks_like_a_member(caplog):
    assert QuoteCondition.from_code(50) is QuoteCondition.NATIONAL_BBO

    with caplog.at_level('WARNING', logger='aiothetadata.constants'):
        unknown = QuoteCondition.from_code(252)
        QuoteCondition.from_code(252)

    assert isinstance(unknown, UnknownQuoteCondition)
    assert unknown == 252
    assert unknown.name == 'UNKNOWN_252'
    assert unknown.description == 'Unknown quote condition (252)'
    assert repr(unknown) == '<UnknownQuoteCondition.UNKNOWN_252: 252>'
    assert len([r for r in caplog.records if '252' in r.getMessage()]) == 1
