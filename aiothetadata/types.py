import enum
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
    'Trade',
    'EodReport',

    'FinancialEntityType',
    'FinancialEntity',
    'Stock',
    'Option',
    'Index',
)


#: A Value that represents a price. See :func:`~.format_price`.
PriceValue = Union[decimal.Decimal, int, str]

#: A value that represents a date. See :func:`~.format_date`.
DateValue = Union[int, str, datetime.date, datetime.datetime]

#: A value that represents a time. See :func:`~.format_time`.
TimeValue = Union[datetime.time, datetime.datetime]

#: A value that represents a date and time. See :func:`~.format_date_time`.
DateTimeValue = Union[datetime.datetime, str]


class FinancialEntityType(enum.Enum):
    STOCK = enum.auto()
    OPTION = enum.auto()
    INDEX = enum.auto()


@dataclass(slots=True, frozen=True, kw_only=True)
class FinancialEntity:
    type: FinancialEntityType
    symbol: str

    def __str__(self):
        return f'{self.type.name}: {self.symbol}'


@dataclass(slots=True, frozen=True, kw_only=True)
class Option(FinancialEntity):
    strike: decimal.Decimal
    expiration: datetime.date
    right: OptionRight

    @classmethod
    def create(cls, **kwargs):
        return cls(type=FinancialEntityType.OPTION, **kwargs)

    def __str__(self):
        exp = self.expiration.strftime('%y%m%d')
        return f'{self.type.name}: {self.symbol} {exp} ${self.strike} {self.right.name}'


@dataclass(slots=True, frozen=True, kw_only=True)
class Stock(FinancialEntity):

    @classmethod
    def create(cls, **kwargs):
        return cls(type=FinancialEntityType.STOCK, **kwargs)


@dataclass(slots=True, frozen=True, kw_only=True)
class Index(FinancialEntity):

    @classmethod
    def create(cls, **kwargs):
        return cls(type=FinancialEntityType.INDEX, **kwargs)


@dataclass(slots=True, frozen=True)
class BaseFinancialInfo:
    entity: FinancialEntity

    @property
    def symbol(self) -> str:
        return self.entity.symbol

    @property
    def type(self) -> FinancialEntityType:
        return self.entity.type


@dataclass(slots=True, frozen=True)
class Quote(BaseFinancialInfo):
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
class Trade(BaseFinancialInfo):
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
class EodReport(BaseFinancialInfo):
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
