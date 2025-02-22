import zoneinfo
import datetime as _datetime
from typing import Union


__all__ = (
    'date',
    'time',
    'datetime',
    'timedelta',
    'date_at_time',

    'MarketTimeZone',
    'MarketOpen',
    'MarketClose',
)


MarketTimeZone = zoneinfo.ZoneInfo('America/New_York')


timedelta = _datetime.timedelta


def date_at_time(date_value: _datetime.date, time_value: _datetime.time) -> _datetime.datetime:
    """
    Combine a date and time into a datetime.
    """
    return datetime.combine(date_value, time_value)


class datetime(_datetime.datetime):
    """
    A subclass of ``datetime.datetime`` that creates datetimes with Eastern
    time by default.
    """
    def __new__(
        cls, year: int, month: int=None, day: int=None, hour: int=0, minute: int=0, second: int=0,
        microsecond: int=0, tzinfo: _datetime.tzinfo=MarketTimeZone, *, fold: int=0,
    ):
        return super().__new__(cls, year, month, day, hour, minute, second, microsecond, tzinfo, fold=fold)

    @classmethod
    def now(cls, tz: _datetime.tzinfo=MarketTimeZone) -> _datetime.datetime:
        """
        Construct a datetime from ``time.time()`` and optional time zone info.
        Defaults to eastern time.
        """
        return super().now(tz)

    @classmethod
    def fromtimestamp(cls, t: int, tz: _datetime.tzinfo=MarketTimeZone) -> _datetime.datetime:
        """
        Construct a ``datetime`` from a POSIX timestamp (like
        ``time.time()``).

        :param t: POSIX timestamp
        :param tz: Timezone. Defaults to Eastern.
        """
        return super().fromtimestamp(t, tz=tz)


class time(_datetime.time):
    """
    A subclass of ``datetime.time`` that creates times with Eastern time by
    default.
    """
    def __new__(
        cls, hour: int=0, minute: int=0, second: int=0, microsecond: int=0,
        tzinfo: _datetime.tzinfo=MarketTimeZone, *, fold: int=0,
    ):
        return super().__new__(cls, hour, minute, second, microsecond, tzinfo, fold=fold)


class date(_datetime.date):
    """
    A subclass of ``datetime.time`` that attempts to return dates in eastern
    time. Note that while ``date`` objects are not timezone aware, certain
    methods can be influenced to return the date in a specific timezone.
    """

    @classmethod
    def fromtimestamp(cls, t: int) -> _datetime.date:
        """
        Construct a ``date`` from a POSIX timestamp (like ``time.time()``). The
        returned date is the date in Eastern time.

        :param t: POSIX timestamp
        """
        return datetime.fromtimestamp(t).date()

    @classmethod
    def today(cls) -> _datetime.date:
        """
        The current date in eastern time.
        """
        return datetime.now().date()


MarketOpen = time(9, 30, 0)
MarketClose = time(16, 0, 0)
Minutes = timedelta(minutes=1)
