import decimal
import datetime
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
)


async def iter_csv(line_gen: AsyncGenerator[bytes, None]) -> AsyncGenerator[str, None]:
    """
    Generate dicts of CSV data from an asynchronous generator of UTF-8 encoded
    lines.
    """
    header = None
    async for line in line_gen:
        line = line.decode('utf-8').strip()

        values = line.split(',')

        if header is None:
            header = values

        else:
            yield dict(zip(header, values))


def parse_date(date: str) -> _datetime.date:
    """
    Parse a ``date`` out of the ``date`` field of a ThetaData response.
    """
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


def parse_strike(strike: int | str) -> decimal.Decimal:
    """
    Parse the strike (``int`` or ``str``) in 1/10 cent into a decimal.
    """
    return decimal.Decimal(strike) / 1000


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

    if 'root' in data:
        parsed['symbol'] = data['root']

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
    Parse trade fields in responses.
    """
    parsed = {}

    for field in ('price', ):
        parsed[field] = decimal.Decimal(data[field])

    for field in ('sequence', 'size', 'records_back'):
        parsed[field] = int(data[field])

    parsed['time'] = parse_date_time(data['date'], data['ms_of_day'])

    for field in ('exchange', ):
        parsed[field] = Exchange(int(data[field]))

    if 'strike' in data:
        parsed['strike'] = parse_strike(data['strike'])

    if 'right' in data:
        parsed['right'] = OptionRight(data['right'])

    if 'root' in data:
        parsed['symbol'] = data['root']

    conditions = []
    condition_fields = ('condition', 'ext_condition1', 'ext_condition2', 'ext_condition3', 'ext_condition4')
    for field in condition_fields:
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

    parsed['last_trade'] = parse_date_time(data['date'], data['ms_of_day2'])

    return parsed
