import enum
import decimal
import datetime
from dataclasses import dataclass
from typing import Optional, Union, Tuple

from .constants import Exchange, QuoteCondition, TradeCondition, OptionRight


__all__ = (
    'PriceValue',
    'DateValue',
    'TimeValue',
    'DateTimeValue',
    'Quote',
    'Trade',
    'OhlcReport',
    'EodReport',
    'IndexPriceReport',
    'FirstOrderGreeks',
    'OpenInterest',
    'EodGreeks',

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
TimeValue = Union[datetime.time, datetime.datetime, str]

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
        exp = self.expiration.strftime('%Y%m%d')
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

    @property
    def mid(self) -> decimal.Decimal:
        return round((self.bid + self.ask) / 2, 2)


@dataclass(slots=True, frozen=True)
class Trade(BaseFinancialInfo):
    time: datetime.datetime

    conditions: Tuple[QuoteCondition]
    price: decimal.Decimal
    sequence: int
    size: int
    exchange: Exchange | None = None

    @property
    def condition(self) -> QuoteCondition:
        return self.conditions[0]


@dataclass(slots=True, frozen=True)
class OhlcReport(BaseFinancialInfo):
    """Open/high/low/close report for a financial entity over a time period."""

    #: Timestamp of the bar (start of interval for historical; report time for current).
    time: datetime.datetime

    open: decimal.Decimal
    high: decimal.Decimal
    low: decimal.Decimal
    close: decimal.Decimal
    volume: int
    count: int

    #: Volume-weighted average price. May be zero for index OHLC.
    vwap: decimal.Decimal | None = None


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


@dataclass(slots=True, frozen=True)
class IndexPriceReport(BaseFinancialInfo):
    time: datetime.datetime
    price: decimal.Decimal


@dataclass(slots=True, frozen=True)
class FirstOrderGreeks(BaseFinancialInfo):
    #: Timestamp of the greeks calculation.
    time: datetime.datetime
    #: Underlying price used for the calculation.
    underlying_price: decimal.Decimal
    #: Implied volatility.
    iv: decimal.Decimal
    #: Model error on the implied volatility calculation.
    iv_error: decimal.Decimal
    #: Rate of change of option price with respect to underlying price.
    delta: decimal.Decimal
    #: Rate of change of option price with respect to time.
    theta: decimal.Decimal
    #: Rate of change of option price with respect to volatility.
    vega: decimal.Decimal
    #: Rate of change of option price with respect to interest rate.
    rho: decimal.Decimal
    #: Rate of change of option price with respect to dividend yield.
    epsilon: decimal.Decimal
    #: Option leverage (``lambda`` in ThetaData; Python reserved keyword).
    leverage: decimal.Decimal
    #: Current bid price of the option.
    bid: decimal.Decimal
    #: Current ask price of the option.
    ask: decimal.Decimal


@dataclass(slots=True, frozen=True)
class OpenInterest(BaseFinancialInfo):
    """Open interest for an option contract.

    OPRA reports open interest once per trading day, at about 06:30 ET. The
    figure reflects open contracts at the end of the *previous* trading day:
    an :class:`OpenInterest` stamped on the morning of day ``D`` is the
    position count after day ``D-1`` closed.
    """

    #: Time the open interest message was received (about 06:30 ET).
    time: datetime.datetime
    #: Number of outstanding contracts.
    open_interest: int


@dataclass(slots=True, frozen=True)
class EodGreeks(BaseFinancialInfo):
    """End-of-day greeks for an option contract.

    ThetaData computes these once per day after the close from the closing
    quote and underlying price. Unlike the first-order snapshot and history
    endpoints, the EOD greeks endpoint includes second-order greeks such as
    ``gamma`` on a standard subscription.
    """

    #: Timestamp of the EOD record as reported by ThetaData.
    time: datetime.datetime
    #: Underlying price used for the calculation.
    underlying_price: decimal.Decimal
    #: Implied volatility.
    iv: decimal.Decimal
    #: Model error on the implied volatility calculation.
    iv_error: decimal.Decimal
    #: Closing bid price of the option.
    bid: decimal.Decimal
    #: Closing ask price of the option.
    ask: decimal.Decimal
    #: Last trade price of the day (zero if no trades).
    close: decimal.Decimal
    #: Contracts traded during the day.
    volume: int
    delta: decimal.Decimal
    gamma: decimal.Decimal
    theta: decimal.Decimal
    vega: decimal.Decimal
    rho: decimal.Decimal
    vanna: decimal.Decimal
    charm: decimal.Decimal
    vomma: decimal.Decimal
