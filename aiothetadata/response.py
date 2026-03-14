import decimal
import datetime
import csv
from typing import Dict, AsyncGenerator, Any

from . import datetime as _datetime
from .constants import QuoteCondition, Exchange, TradeCondition, OptionRight


__all__ = (
    'iter_csv',
    'parse_date',
    'parse_date_time',
    'parse_time',
    'parse_quote_fields',
    'parse_trade_fields',
    'parse_strike',
    'parse_eod_report',
    'parse_ohlc_report',
    'parse_index_price_report',
    'parse_first_order_greeks',
)


async def iter_csv(line_gen: AsyncGenerator[bytes, None]) -> AsyncGenerator[str, None]:
    """
    Generate dicts of CSV data from an asynchronous generator of UTF-8 encoded
    lines.
    """
    header = None
    async for line in line_gen:
        line = line.decode('utf-8').strip()

        # Skip empty lines
        if not line:
            continue

        values = next(csv.reader([line]))

        if header is None:
            header = values

        else:
            yield dict(zip(header, values))


def parse_date(date: str) -> _datetime.date:
    """
    Parse a ``date`` out of the ``date`` field of a ThetaData response.
    """
    # TODO: Why are some of these in YYYY-MM-DD format and some in YYYYMMDD
    # format. It's past bedtime and I'm not figuring that out now.
    if '-' in date:
        year, month, day = (int(x) for x in date.split('-'))

    else:
        year = int(date[:4])
        month = int(date[4:6])
        day = int(date[6:8])

    return _datetime.date(year, month, day)


def parse_time(time: str) -> _datetime.time:
    """
    Parse a time out of a the ``ms_of_day`` field of a ThetaData response. The
    returned ``time`` object will have the timezone set to eastern time.
    """
    milliseconds = int(time)

    # I am getting index prices with 86400000. This makes no sense: there are
    # 86400000ms in a day and they start at 0, thus have a maximum of 86399999.
    milliseconds = min(milliseconds, 86399999)

    args = {}
    conv = (
        ('hour', 3600000),
        ('minute', 60000),
        ('second', 1000),
    )

    for key, div in conv:
        args[key] = milliseconds // div
        milliseconds %= div

    args['microsecond'] = milliseconds * 1000

    return _datetime.time(**args)


def parse_date_time(date: str, time: str) -> _datetime.datetime:
    """
    Parse a date and time out of a ThetaData response. See :func:`~.parse_time`
    and :func:`~.parse_date`.
    """
    return _datetime.datetime.combine(
        parse_date(date),
        parse_time(time),
    )


def parse_strike(strike: str) -> decimal.Decimal:
    """
    Parse the strike (``int`` or ``str``) in 1/10 cent into a decimal.
    """
    return decimal.Decimal(strike)


def parse_quote_fields(data: Dict[str, str]) -> Dict[str, Any]:
    """
    Parse quote fields in responses.
    """
    parsed = {}

    for field in ('bid', 'ask'):
        parsed[field] = decimal.Decimal(data[field])

    if 'strike' in data:
        parsed['strike'] = parse_strike(data['strike'])

    if 'right' in data:
        parsed['right'] = OptionRight(data['right'])

    if 'symbol' in data:
        parsed['symbol'] = data['symbol']

    for field in ('bid_size', 'ask_size'):
        parsed[field] = int(data[field])

    for field in ('bid_condition', 'ask_condition'):
        parsed[field] = QuoteCondition.from_code(int(data[field]))

    parsed['time'] = parse_date_time(data['date'], data['ms_of_day'])

    for field in ('bid_exchange', 'ask_exchange'):
        parsed[field] = Exchange(int(data[field]))

    return parsed


def parse_trade_fields(data: Dict[str, str]) -> Dict[str, Any]:
    """
    Parse trade fields from API responses.
    """
    parsed = {}

    for field in ('price', ):
        parsed[field] = decimal.Decimal(data[field])

    for field in ('sequence', 'size'):
        parsed[field] = int(data[field])

    parsed['time'] = parse_timestamp(data['timestamp'])

    if 'exchange' in data:
        parsed['exchange'] = Exchange(int(data['exchange']))

    if 'strike' in data:
        parsed['strike'] = parse_strike(data['strike'])

    if 'right' in data:
        parsed['right'] = OptionRight(data['right'])

    if 'symbol' in data:
        parsed['symbol'] = data['symbol']

    conditions = []
    condition_fields = ('condition', 'ext_condition1', 'ext_condition2', 'ext_condition3', 'ext_condition4')
    for field in condition_fields:
        if field in data:
            value = int(data[field])
            if value != 255:
                conditions.append(TradeCondition(value))

    parsed['conditions'] = tuple(conditions)

    return parsed


def parse_eod_report(data: Dict[str, str]) -> Dict[str, Any]:
    """
    Parse EOD report fields.
    """
    parsed = {}
    parsed.update(parse_quote_fields(data))

    for field in ('volume', 'count'):
        parsed[field] = int(data[field])

    for field in ('open', 'high', 'low', 'close'):
        parsed[field] = decimal.Decimal(data[field])

    # v3 has last_trade as ISO timestamp
    if 'last_trade' in data:
        parsed['last_trade'] = parse_timestamp(data['last_trade'])

    return parsed


def parse_ohlc_report(data: Dict[str, str]) -> Dict[str, Any]:
    """Parse OHLC report fields from snapshot or history responses.

    Handles both option/stock/index OHLC responses. The ``vwap`` field is
    optional — present in stock and option history but absent in index OHLC.

    :param data: Raw response row as a string-to-string mapping.
    :returns: Parsed field dict suitable for constructing an
        :class:`~aiothetadata.types.OhlcReport`.
    """
    parsed: Dict[str, Any] = {}

    if 'timestamp' in data:
        parsed['time'] = parse_timestamp(data['timestamp'])
    elif 'created' in data:
        parsed['time'] = parse_timestamp(data['created'])

    for field in ('open', 'high', 'low', 'close'):
        parsed[field] = decimal.Decimal(data[field])

    parsed['volume'] = int(data['volume'])
    parsed['count'] = int(data['count'])

    if 'vwap' in data:
        parsed['vwap'] = decimal.Decimal(data['vwap'])

    if 'strike' in data:
        parsed['strike'] = parse_strike(data['strike'])

    if 'right' in data:
        parsed['right'] = OptionRight(data['right'])

    if 'symbol' in data:
        parsed['symbol'] = data['symbol']

    if 'expiration' in data:
        parsed['expiration'] = parse_date(data['expiration'])

    return parsed


def parse_index_price_report(data: Dict[str, str]) -> Dict[str, Any]:
    """
    Parse index price report fields.
    """
    return {
        'price': decimal.Decimal(data['price']),
        'time': parse_timestamp(data['timestamp']),
    }


def parse_timestamp(timestamp: str) -> _datetime.datetime:
    """
    Parse ISO 8601 timestamp (e.g., '2025-02-20T10:00:00').
    Assumes eastern time.
    """
    # Parse ISO format without timezone
    dt = datetime.datetime.fromisoformat(timestamp)
    # Assume eastern time
    return dt.replace(tzinfo=_datetime.MarketTimeZone)


def parse_strike(strike: str) -> decimal.Decimal:
    """
    Parse strike from string (in dollars).
    """
    return decimal.Decimal(strike)


def parse_quote_fields(data: Dict[str, str]) -> Dict[str, Any]:
    """
    Parse quote fields from API responses.
    """
    parsed = {}

    for field in ('bid', 'ask'):
        parsed[field] = decimal.Decimal(data[field])

    if 'strike' in data:
        parsed['strike'] = parse_strike(data['strike'])

    if 'right' in data:
        parsed['right'] = OptionRight(data['right'])

    if 'symbol' in data:
        parsed['symbol'] = data['symbol']

    for field in ('bid_size', 'ask_size'):
        parsed[field] = int(data[field])

    if 'bid_condition' in data and 'ask_condition' in data:
        for field in ('bid_condition', 'ask_condition'):
            parsed[field] = QuoteCondition.from_code(int(data[field]))

    # EOD reports use 'created' field instead of 'timestamp'
    if 'timestamp' in data:
        parsed['time'] = parse_timestamp(data['timestamp'])
    elif 'created' in data:
        parsed['time'] = parse_timestamp(data['created'])

    if 'bid_exchange' in data and 'ask_exchange' in data:
        for field in ('bid_exchange', 'ask_exchange'):
            parsed[field] = Exchange(int(data[field]))

    return parsed


def parse_first_order_greeks(data: Dict[str, str]) -> Dict[str, Any]:
    """Parse first-order greeks fields from a ``greeks_first_order`` API response.

    :param data: Raw response row as a string-to-string mapping.
    :returns: Parsed field dict suitable for constructing a
        :class:`~aiothetadata.types.FirstOrderGreeks`.
    """
    parsed = {}

    parsed['time'] = parse_timestamp(data['timestamp'])
    parsed['underlying_price'] = decimal.Decimal(data['underlying_price'])

    # implied_vol in the API maps to iv in the type; lambda is a Python reserved word.
    parsed['iv'] = decimal.Decimal(data['implied_vol'])
    parsed['leverage'] = decimal.Decimal(data['lambda'])

    for field in ('delta', 'theta', 'vega', 'rho', 'epsilon', 'iv_error', 'bid', 'ask'):
        parsed[field] = decimal.Decimal(data[field])

    if 'strike' in data:
        parsed['strike'] = parse_strike(data['strike'])

    if 'right' in data:
        parsed['right'] = OptionRight(data['right'])

    if 'symbol' in data:
        parsed['symbol'] = data['symbol']

    return parsed
