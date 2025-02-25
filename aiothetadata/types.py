import decimal
import datetime
from dataclasses import dataclass
from typing import Union, Tuple

from .constants import Exchange, QuoteCondition, TradeCondition, OptionRight


__all__ = (
    'PriceValue',
    'DateValue',
    'TimeValue',
    'DateTimeValue',
    'Quote',
    'OptionQuote',
    'StockQuote',
    'Trade',
    'OptionTrade',
    'StockTrade',
    'EodReport',
    'OptionEodReport',
    'StockEodReport',
)


#: A Value that represents a price. See :func:`~.format_price`.
PriceValue = Union[decimal.Decimal, int, str]

#: A value that represents a date. See :func:`~.format_date`.
DateValue = Union[int, str, datetime.date, datetime.datetime]

#: A value that represents a time. See :func:`~.format_time`.
TimeValue = Union[datetime.time, datetime.datetime]

#: A value that represents a date and time. See :func:`~.format_date_time`.
DateTimeValue = Union[datetime.datetime, str]


@dataclass(slots=True, frozen=True)
class Quote:
    symbol: str
    time: datetime.datetime

    bid: decimal.Decimal
    bid_size: int
    bid_exchange: Exchange
    bid_condition: QuoteCondition

    ask: decimal.Decimal
    ask_size: int
    ask_exchange: Exchange
    ask_condition: QuoteCondition


@dataclass(slots=True, frozen=True)
class OptionQuote(Quote):
    strike: decimal.Decimal
    expiration: datetime.date
    right: OptionRight


@dataclass(slots=True, frozen=True)
class StockQuote(Quote):
    pass


@dataclass(slots=True, frozen=True)
class Trade:
    symbol: str
    time: datetime.datetime

    exchange: Exchange
    conditions: Tuple[QuoteCondition]
    price: decimal.Decimal
    sequence: int
    size: int
    records_back: int

    @property
    def condition(self) -> QuoteCondition:
        return self.conditions[0]


@dataclass(slots=True, frozen=True)
class OptionTrade(Trade):
    strike: decimal.Decimal
    expiration: datetime.date
    right: OptionRight


@dataclass(slots=True, frozen=True)
class StockTrade(Trade):
    pass


@dataclass(slots=True, frozen=True)
class EodReport:
    symbol: str
    time: datetime.datetime
    last_trade: datetime.datetime

    bid: decimal.Decimal
    bid_size: int
    bid_exchange: Exchange
    bid_condition: QuoteCondition

    ask: decimal.Decimal
    ask_size: int
    ask_exchange: Exchange
    ask_condition: QuoteCondition

    open: decimal.Decimal
    high: decimal.Decimal
    low: decimal.Decimal
    close: decimal.Decimal

    volume: int
    count: int


# TODO: A lot of these might be nicer if symbol, strike, right are all in one
# field. Then quote, trade, etc could be the same...
@dataclass(slots=True, frozen=True)
class OptionEodReport(EodReport):
    strike: decimal.Decimal
    expiration: datetime.date
    right: OptionRight


@dataclass(slots=True, frozen=True)
class StockEodReport(EodReport):
    pass
