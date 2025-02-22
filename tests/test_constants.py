import pytest

from aiothetadata.constants import *
from aiothetadata._trade_conditions import _TRADE_CONDITIONS


@pytest.mark.parametrize('prop', ('cancel', 'late_report', 'auto_executed', 'volume', 'high', 'low', 'last'))
def test_trade_condition_properties(prop):
    for condition in TradeCondition:
        assert isinstance(getattr(condition, prop), bool)


def test_trade_conditions_metadata():
    enum_values = {int(v) for v in TradeCondition}
    meta_values = set(_TRADE_CONDITIONS.keys())

    assert enum_values == meta_values
