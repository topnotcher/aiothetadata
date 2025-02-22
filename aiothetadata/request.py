import decimal
import datetime
from typing import Tuple, Generator

from .types import *
from .datetime import MarketTimeZone


__all__ = (
    'format_price',
    'format_date',
    'format_time',
    'format_date_time',
    'range_of_days',
)


def format_price(value: PriceValue) -> int:
    """
    Format a price value for a use in a ThetaData request.

    Several types are supported:

        * ``int``: An exact price value in whole dollars.
        * ``float``: A floating-point price value, which will be rounded to the
            nearest 1/10 cent.
        * ``decimal.Decimal``: An exact decimal price value. The precision is
          1/10 cent.
        * ``str``: A string suitable for passing to ``decimal.Decimal``.

        :param value: The price value.
        :return: The price in 1/10 cent.
    """
    if isinstance(value, int):
        return value * 1000

    if isinstance(value, float):
        return int(round(value, 3) * 1000)

    if isinstance(value, str):
        value = decimal.Decimal(value)

    if isinstance(value, decimal.Decimal):
        return round(value * 1000)

    raise ValueError(f'Invalid price: {value}')


def format_date(value: DateValue) -> str:
    """
    Format a date value for use in a ThetaData request.

    Several types are supported:

        * ``int``: A date in ``YYYYMMDD`` format.
        * ``str``: A date in ``YYYYMMDD`` format.
        * ``datetime.date``: A date.
        * ``datetime.datetime``: The ``datetime.date`` corresponding to
          ``value.date()``. If the ``datetime`` is timezone aware, it will be
          converted to eastern time.

    :param value: The date value.
    :return: A string date in ``YYYYMMDD`` format.
    """
    if isinstance(value, datetime.datetime):
        if value.tzinfo:
            value = value.astimezone(MarketTimeZone)

        value = value.date()

    if isinstance(value, datetime.date):
        return value.strftime('%Y%m%d')

    if isinstance(value, int):
        return str(value)

    if isinstance(value, str):
        return value

    raise ValueError(f'Invalid date: {value}')


def format_time(value: TimeValue) -> int:
    """
    Format a time value for use in a ThetaData request.

    Several types are supported:

        * ``datetime.time``: A time with resolution up to milliseconds. If this
            is timezone aware, the timezone must be :attr:`~.MarketTimeZone`.
            If it is naive, it is assumed to be eastern.
        * ``datetime.datetime``: The ``datetime.time`` corresponding to
          ``value.time()``. If the ``datetime`` is timezone aware, it is first
          converted to eastern time. If it is naive, it is assumed to be eastern.
        * ``str``: ``HH:MM:SS`` in 24-hour time. Assumed to be eastern time.

    :param value: The time value.
    :return: The number of milliseconds since midnight eastern.
    """
    time_value = value

    if isinstance(time_value, str):
        time_value = datetime.datetime.strptime(time_value, '%H:%M:%S')

    if isinstance(time_value, datetime.datetime):
        if time_value.tzinfo:
            time_value = time_value.astimezone(MarketTimeZone)

        time_value = time_value.time()

    if not isinstance(time_value, datetime.time):
        raise ValueError(f'Invalid time: {value}')

    # TODO: tzinfo does not have __eq__, so this might require it to be
    # _exactly_ MarketTimeZone?
    if time_value.tzinfo and time_value.tzinfo != MarketTimeZone:
        raise ValueError('datetime.time must be naive or in eastern time!')

    milliseconds = 0
    milliseconds += time_value.hour * 3600000
    milliseconds += time_value.minute * 60000
    milliseconds += time_value.second * 1000
    milliseconds += time_value.microsecond // 1000

    return milliseconds


def format_date_time(value: DateTimeValue) -> Tuple[str, int]:
    """
    Format a date/time value for use in a ThetaData request.

    Several types are supported:

        * ``datetime.datetime``: A ``datetime.datetime`` with resolution up to
            milliseconds. If the ``datetime`` is naive, it is assumed to be
            eastern time. If it is timezone-aware, it is converted to eastern
            time.
        * ``str``: ``YYYYMMDD HH:MM:SS`` in 24-hour time. Assumed to be eastern
            time.

    :param value: The time value.
    :return: ``(date str, time int)``
    """
    if isinstance(value, str):
        date, time = value.split(' ')

    elif isinstance(value, datetime.datetime):
        if value.tzinfo:
            value = value.astimezone(MarketTimeZone)

        date = value.date()
        time = value.time()

    else:
        raise ValueError(f'Invalid date time: {value}')

    return format_date(date), format_time(time)


def range_of_days(start: str, end: str, split: int) -> Generator[Tuple[str, str], None, None]:
    start_date = datetime.datetime.strptime(start, '%Y%m%d').date()
    end_date = datetime.datetime.strptime(end, '%Y%m%d').date()

    total_days = (end_date - start_date).days + 1
    split_days = datetime.timedelta(days=split - 1)

    for offset in range(0, total_days, split):
        chunk_start = start_date + datetime.timedelta(days=offset)
        chunk_end = min(chunk_start + split_days, end_date)

        yield chunk_start.strftime('%Y%m%d'), chunk_end.strftime('%Y%m%d')
