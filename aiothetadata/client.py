import asyncio
import decimal
import logging

from typing import Optional, List, Coroutine, AsyncGenerator, Dict, Any, Tuple, Generator, Callable
from urllib.parse import urljoin

import aiohttp

from .request import *
from .types import *
from .constants import *
from .response import *


log = logging.getLogger(__name__)


ParamsGenerator = Generator[Dict[str, Any], None, None]
ParamsGeneratorCallback = Callable[[Dict[str, Any]], ParamsGenerator]


def _filter_symbols(symbols: List[str]) -> List[str]:
    """Filter out empty or blank symbols from a list.

    ThetaData occasionally returns empty strings in symbol lists.
    These are removed here so callers always receive well-formed data.
    """
    filtered = [s for s in symbols if s and s.strip()]
    dropped = len(symbols) - len(filtered)
    if dropped:
        log.warning('Filtered %d empty/blank symbol(s) from ThetaData response', dropped)
    return filtered


class ThetaDataError(Exception):
    pass


class ThetaDataHttpError(ThetaDataError):
    def __init__(self, status: 200, msg: str):
        self.status = status

        super().__init__(f'ThetaData returned HTTP {status}: {msg}')


class _PagedRequest:
    """
    A request that may be split accross multiple "pages". Paging is either
    server-side or client side. On the client side, the client may opt to split
    a request into multilpe requests.

    :param: url: The URL to get.
    :param params: A generator that yields query params for successive
        requests.
    """
    __slots__ = ('url', 'params', 'session', 'task', 'queue', 'last_resp')

    def __init__(self, session: aiohttp.ClientSession, url: str, params: ParamsGenerator):
        self.session = session
        self.url = url
        self.params = params
        self.task = None
        self.last_resp = None
        self.queue = asyncio.Queue()

    async def get_pages(self) -> None:
        try:
            next_get = self.get_next_request()
            while next_get:
                resp = await next_get
                self.queue.put_nowait(resp)
                next_get = self.get_next_page(resp)

            self.queue.shutdown()

        except asyncio.CancelledError:
            pass

        # TODO: can I move passing the URL/params into the task that calls this?
        # Maybe a list instead of a generator...
        except Exception:
            log.exception('Error in paged request')
            self.queue.shutdown()

    def get_next_page(self, resp) -> Optional[Coroutine[None, None, aiohttp.ClientResponse]]:
        if resp.status != 200:
            return None

        if resp.headers.get('Next-Page', 'null') != 'null':
            return self.session.get(resp.headers['Next-Page'])

        return self.get_next_request()

    def get_next_request(self) -> Optional[Coroutine[None, None, aiohttp.ClientResponse]]:
        try:
            return self.session.get(self.url, params=next(self.params))

        except StopIteration:
            return None

        except Exception:
            log.exception("Error getting next request")
            return None

    def __aiter__(self):
        self.task = asyncio.create_task(self.get_pages())
        return self

    async def __anext__(self):
        self._close_resp()

        try:
            self.last_resp = await self.queue.get()

        except asyncio.QueueShutDown:
            await self.close()

            raise StopAsyncIteration from None

        return self.last_resp

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_, **__):
        await self.close()

    def _close_resp(self) -> None:
        last_resp = self.last_resp
        self.last_resp = None

        if last_resp is not None:
            last_resp.close()

    async def close(self) -> None:
        self._close_resp()

        task = self.task
        self.task = None

        if task is not None and not task.done():
            task.cancel()
            try:
                await task

            except asyncio.CancelledError:
                pass

        while not self.queue.empty():
            resp = self.queue.get_nowait()
            resp.close()


class _ThetaClient:
    DEFAULT_URL = 'http://127.0.0.1:25503/'

    def __init__(self, url: Optional[str]=None):
        if url is None:
            url = self.DEFAULT_URL

        self._base = url
        self._path = ('v3',)

        self._session = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()

        return self._session

    def _build_url(self, *pcs: str) -> str:
        path = list(self._path)
        path.extend(pcs)
        b = urljoin(self._base, '/'.join(path))
        return b

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_, **__):
        await self.close()

    async def close(self) -> None:
        session = self._session
        self._session = None
        if session:
            await session.close()

    async def stream_data(
        self, *pcs: str, params_gen: Optional[ParamsGenerator]=None, **params: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, str], None]:

        get_params = {}
        get_params.update(params)
        get_params['format'] = 'csv'

        if params_gen is None:
            params_gen = self._single_params(get_params)
        else:
            params_gen = params_gen(get_params)

        async with _PagedRequest(self.session, self._build_url(*pcs), params_gen) as pages:
            async for resp in pages:

                if resp.status == 472:
                    continue
                if resp.status != 200:
                    await self._handle_http_error(resp)

                async for line in iter_csv(resp.content):
                    yield line

    async def get_data(self, *args, **kwargs) -> List[Dict[str, str]]:
        data = []
        async for value in self.stream_data(*args, **kwargs):
            data.append(value)

        return data

    @staticmethod
    async def _handle_http_error(resp: aiohttp.ClientResponse) -> None:
        # TODO: https://http-docs.thetadata.us/Articles/Data-And-Requests/Values/Error-Codes.html
        raise ThetaDataHttpError(resp.status, await resp.text())

    @staticmethod
    def date_range_params(days: int) -> ParamsGeneratorCallback:
        # TODO: I dislike this API. I did it because stream_data modifies the params.
        # TODO: start_date/end_date are user input and could be invalid. This
        # ends up being evaluated in the _PagedRequest class, so an exception
        # is not raised :o
        def _gen_params(params: Dict[str, Any]):
            for start, end in range_of_days(params['start_date'], params['end_date'], days):
                chunk = {}
                chunk.update(params)
                chunk['start_date'] = start
                chunk['end_date'] = end

                yield chunk

        return _gen_params

    @staticmethod
    def _single_params(params: Dict[str, Any]) -> ParamsGenerator:
        yield params


class ThetaOptionClient(_ThetaClient):
    """Client for ThetaData option endpoints."""

    # ── Internal helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _populate_entity_params(request: Dict[str, Any], params: Dict[str, Any]) -> None:
        entity_params = {}
        for param in ('symbol', 'right', 'strike', 'expiration'):
            entity_params[param] = params.pop(param, None)

        if not entity_params.get('symbol'):
            entity_params['symbol'] = request.get('symbol')

        if not entity_params.get('right'):
            entity_params['right'] = request.get('right')

        if not entity_params.get('strike'):
            entity_params['strike'] = parse_strike(request.get('strike', '0'))

        exp_str = request.get('expiration', '')
        if exp_str:
            entity_params['expiration'] = parse_date(exp_str)

        params['entity'] = Option.create(**entity_params)

    def _make_quote(self, request: Dict[str, Any], response: Dict[str, Any]) -> Quote:
        params = parse_quote_fields(response)
        self._populate_entity_params(request, params)
        return Quote(**params)

    def _make_trade(self, request: Dict[str, Any], response: Dict[str, Any]) -> Trade:
        params = parse_trade_fields(response)
        self._populate_entity_params(request, params)
        return Trade(**params)

    def _make_ohlc(self, request: Dict[str, Any], response: Dict[str, Any]) -> OhlcReport:
        params = parse_ohlc_report(response)
        self._populate_entity_params(request, params)
        return OhlcReport(**params)

    async def _gen_quotes(
        self, params: Dict[str, Any], gen: AsyncGenerator[Dict[str, str], None]
    ) -> AsyncGenerator[Quote, None]:
        async for row in gen:
            yield self._make_quote(params, row)

    async def _gen_trades(
        self, params: Dict[str, Any], gen: AsyncGenerator[Dict[str, str], None]
    ) -> AsyncGenerator[Trade, None]:
        async for row in gen:
            yield self._make_trade(params, row)

    async def _gen_ohlc(
        self, params: Dict[str, Any], gen: AsyncGenerator[Dict[str, str], None]
    ) -> AsyncGenerator[OhlcReport, None]:
        async for row in gen:
            yield self._make_ohlc(params, row)

    def _at_time_params(
        self, request: str, *, symbol: str, expiration: DateValue,
        start_date: str, end_date: str, time: str,
        strike: Optional[PriceValue] = None, right: Optional[OptionRight] = None,
        limit: Optional[int] = None,
    ) -> Tuple[Dict[str, Any], AsyncGenerator[Dict[str, str], None]]:
        params: Dict[str, Any] = {
            'symbol': symbol,
            'expiration': format_date(expiration),
            'start_date': start_date,
            'end_date': end_date,
            'time_of_day': time,
        }
        if strike is not None and right is not None:
            params['strike'] = format_price(strike)
            params['right'] = right
            days = 30
        elif strike is not None or right is not None:
            raise ValueError('strike and right must both be provided or both omitted')
        else:
            days = 5
        if limit is not None:
            params['strike_range'] = limit
        split_days = self.date_range_params(days)
        return params, self.stream_data('option', 'at_time', request,  params_gen=split_days, **params)

    # ── Discovery ─────────────────────────────────────────────────────────────

    async def get_symbols(self) -> List[str]:
        """Return all available option root symbols.

        :return: List of symbol strings (e.g. ``['SPXW', 'AMD', ...]``).
        """
        symbols = [r['symbol'] async for r in self.stream_data('option', 'list', 'symbols')]
        return _filter_symbols(symbols)

    async def get_expirations(self, symbol: str) -> AsyncGenerator[DateValue, None]:
        """Yield all available expiration dates for an option root.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :yields: :class:`datetime.date` objects in ascending order.
        """
        gen = self.stream_data('option', 'list', 'expirations', symbol=symbol)
        async for row in gen:
            yield parse_date(row['expiration'])

    async def get_strikes(self, symbol: str, expiration: DateValue) -> AsyncGenerator[decimal.Decimal, None]:
        """Yield all available strikes for a given symbol and expiration.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :yields: Strike prices as :class:`decimal.Decimal`.
        """
        gen = self.stream_data(
            'option', 'list', 'strikes',
            symbol=symbol,
            expiration=format_date(expiration),
        )
        async for row in gen:
            yield parse_strike(row['strike'])

    # ── Chain methods (base implementations) ─────────────────────────────────

    def get_chain_quotes(
        self,
        symbol: str,
        expiration: DateValue,
        *,
        strike: Optional[PriceValue] = None,
        right: Optional[OptionRight] = None,
        limit: Optional[int] = None,
        start_date: Optional[DateValue] = None,
        end_date: Optional[DateValue] = None,
        time: Optional[TimeValue] = None,
    ) -> AsyncGenerator[Quote, None]:
        """Get quotes for option contracts.

        Can be invoked in two ways:

        - Without ``start_date``, ``end_date``, or ``time``: returns the
          current snapshot quote for each matching contract.
        - With ``start_date``, ``end_date``, and ``time``: returns historical
          point-in-time quotes, one result per contract per trading day.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :param strike: Filter to a specific strike. Optional.
        :param right: Filter to a specific right. Optional.
        :param limit: Limit results to N strikes around ATM. Optional.
        :param start_date: Start of historical date range. Requires ``time``.
        :param end_date: End of historical date range. Requires ``time``.
        :param time: Time of day for historical lookup. If omitted, returns
            current snapshot data.
        :return: Async generator of :class:`~.Quote` objects.
        """
        time_params = (start_date, end_date, time)
        if any(time_params):
            if not all(time_params):
                raise ValueError('start_date, end_date, and time must all be provided together')
            params, gen = self._at_time_params(
                'quote',
                symbol=symbol, expiration=expiration,
                start_date=format_date(start_date),
                end_date=format_date(end_date),
                time=format_time(time),
                strike=strike, right=right, limit=limit,
            )
        else:
            params: Dict[str, Any] = {
                'symbol': symbol,
                'expiration': format_date(expiration),
            }
            if strike is not None:
                params['strike'] = format_price(strike)
            if right is not None:
                params['right'] = right
            if limit is not None:
                params['strike_range'] = limit
            gen = self.stream_data('option', 'snapshot', 'quote', **params)

        return self._gen_quotes(params, gen)

    def get_chain_trades(
        self,
        symbol: str,
        expiration: DateValue,
        *,
        strike: Optional[PriceValue] = None,
        right: Optional[OptionRight] = None,
        start_date: Optional[DateValue] = None,
        end_date: Optional[DateValue] = None,
        time: Optional[TimeValue] = None,
    ) -> AsyncGenerator[Trade, None]:
        """Get trades for option contracts.

        Can be invoked in two ways:

        - Without ``start_date``, ``end_date``, or ``time``: returns the
          current snapshot last trade for each matching contract.
        - With ``start_date``, ``end_date``, and ``time``: returns historical
          point-in-time trades, one result per contract per trading day.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :param strike: Filter to a specific strike. Optional.
        :param right: Filter to a specific right. Optional.
        :param start_date: Start of historical date range. Requires ``time``.
        :param end_date: End of historical date range. Requires ``time``.
        :param time: Time of day for historical lookup. If omitted, returns
            current snapshot data.
        :return: Async generator of :class:`~.Trade` objects.
        """
        time_params = (start_date, end_date, time)
        if any(time_params):
            if not all(time_params):
                raise ValueError('start_date, end_date, and time must all be provided together')
            params, gen = self._at_time_params(
                'trade',
                symbol=symbol, expiration=expiration,
                start_date=format_date(start_date),
                end_date=format_date(end_date),
                time=format_time(time),
                strike=strike, right=right,
            )
        else:
            params = {
                'symbol': symbol,
                'expiration': format_date(expiration),
            }
            if strike is not None:
                params['strike'] = format_price(strike)
            if right is not None:
                params['right'] = right
            gen = self.stream_data('option', 'snapshot', 'trade', **params)

        return self._gen_trades(params, gen)

    def get_chain_greeks(
        self,
        symbol: str,
        expiration: DateValue,
        *,
        strike: Optional[PriceValue] = None,
        right: Optional[OptionRight] = None,
        limit: Optional[int] = None,
        order: GreeksOrder = GreeksOrder.FIRST,
    ) -> AsyncGenerator[FirstOrderGreeks, None]:
        """Get current greeks for option contracts.

        .. note::
            Only :attr:`~.GreeksOrder.FIRST` is available on standard
            subscriptions. Higher orders require a professional subscription.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :param strike: Filter to a specific strike. Optional.
        :param right: Filter to a specific right. Optional.
        :param limit: Limit results to N strikes around ATM. Optional.
        :param order: The order of greeks to retrieve.
        :return: Async generator of :class:`~.FirstOrderGreeks` objects.
        """
        params: Dict[str, Any] = {
            'symbol': symbol,
            'expiration': format_date(expiration),
        }
        if strike is not None:
            params['strike'] = format_price(strike)
        if right is not None:
            params['right'] = right
        if limit is not None:
            params['strike_range'] = limit

        gen = self.stream_data('option', 'snapshot', 'greeks', order.value, **params)

        async def _gen() -> AsyncGenerator[FirstOrderGreeks, None]:
            async for row in gen:
                parsed = parse_first_order_greeks(row)
                self._populate_entity_params(params, parsed)
                yield FirstOrderGreeks(**parsed)

        return _gen()

    # ── Single-contract convenience methods (Optional[T]) ────────────────────

    async def get_quote(
        self,
        symbol: str,
        expiration: DateValue,
        strike: PriceValue,
        right: OptionRight,
        *,
        time: Optional[DateTimeValue] = None,
    ) -> Optional[Quote]:
        """Get the quote for a specific option contract.

        Delegates to :meth:`get_chain_quotes` with ``strike`` and ``right``
        fixed, returning the first result. See that method for full parameter
        and overload documentation.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :param strike: Strike price.
        :param right: Option right.
        :param time: If provided, returns the historical quote at this time.
            If omitted, returns the current snapshot quote.
        :return: :class:`~.Quote`, or ``None`` if no data.
        """
        if time is not None:
            date_str, time_str = format_date_time(time)
            params, raw_gen = self._at_time_params(
                'quote',
                symbol=symbol, expiration=expiration,
                start_date=date_str, end_date=date_str, time=time_str,
                strike=strike, right=right,
            )
            gen = self._gen_quotes(params, raw_gen)
        else:
            gen = self.get_chain_quotes(symbol, expiration, strike=strike, right=right)
        return await anext(gen, None)

    async def get_last_trade(
        self,
        symbol: str,
        expiration: DateValue,
        strike: PriceValue,
        right: OptionRight,
        *,
        time: Optional[DateTimeValue] = None,
    ) -> Optional[Trade]:
        """Get the last trade for a specific option contract.

        Delegates to :meth:`get_chain_trades` with ``strike`` and ``right``
        fixed, returning the first result.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :param strike: Strike price.
        :param right: Option right.
        :param time: If provided, returns the historical last trade at this
            time. If omitted, returns the current snapshot last trade.
        :return: :class:`~.Trade`, or ``None`` if no data.
        """
        if time is not None:
            date_str, time_str = format_date_time(time)
            params, raw_gen = self._at_time_params(
                'trade',
                symbol=symbol, expiration=expiration,
                start_date=date_str, end_date=date_str, time=time_str,
                strike=strike, right=right,
            )
            gen = self._gen_trades(params, raw_gen)
        else:
            gen = self.get_chain_trades(symbol, expiration, strike=strike, right=right)
        return await anext(gen, None)

    async def get_greeks(
        self,
        symbol: str,
        expiration: DateValue,
        strike: PriceValue,
        right: OptionRight,
        *,
        order: GreeksOrder = GreeksOrder.FIRST,
    ) -> Optional[FirstOrderGreeks]:
        """Get current greeks for a specific option contract.

        Delegates to :meth:`get_chain_greeks` with ``strike`` and ``right``
        fixed, returning the first result.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :param strike: Strike price.
        :param right: Option right.
        :param order: The order of greeks to retrieve.
        :return: :class:`~.FirstOrderGreeks`, or ``None`` if no data.
        """
        return await anext(
            self.get_chain_greeks(symbol, expiration, strike=strike, right=right, order=order),
            None,
        )

    async def get_ohlc(
        self,
        symbol: str,
        expiration: DateValue,
        strike: PriceValue,
        right: OptionRight,
    ) -> Optional[OhlcReport]:
        """Get the current day OHLC report for a specific option contract.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :param strike: Strike price.
        :param right: Option right.
        :return: :class:`~.OhlcReport`, or ``None`` if no data.
        """
        params: Dict[str, Any] = {
            'symbol': symbol,
            'expiration': format_date(expiration),
            'strike': format_price(strike),
            'right': right,
        }
        gen = self._gen_ohlc(params, self.stream_data('option', 'snapshot', 'ohlc', **params))
        return await anext(gen, None)

    # ── Multi-day at-time series ──────────────────────────────────────────────

    def get_quotes(
        self,
        symbol: str,
        expiration: DateValue,
        strike: PriceValue,
        right: OptionRight,
        *,
        start_date: DateValue,
        end_date: DateValue,
        time: TimeValue,
    ) -> AsyncGenerator[Quote, None]:
        """Get quotes for a specific contract at the same time across a date range.

        Returns one :class:`~.Quote` per trading day — the quote nearest to
        but not after ``time`` on each day.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :param strike: Strike price.
        :param right: Option right.
        :param start_date: First date in the range.
        :param end_date: Last date in the range.
        :param time: Time of day to sample on each date.
        :return: Async generator of :class:`~.Quote` objects.
        """
        return self.get_chain_quotes(
            symbol, expiration,
            strike=strike, right=right,
            start_date=start_date, end_date=end_date, time=time,
        )

    def get_trades(
        self,
        symbol: str,
        expiration: DateValue,
        strike: PriceValue,
        right: OptionRight,
        *,
        start_date: DateValue,
        end_date: DateValue,
        time: TimeValue,
    ) -> AsyncGenerator[Trade, None]:
        """Get trades for a specific contract at the same time across a date range.

        Returns one :class:`~.Trade` per trading day — the trade nearest to
        but not after ``time`` on each day.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :param strike: Strike price.
        :param right: Option right.
        :param start_date: First date in the range.
        :param end_date: Last date in the range.
        :param time: Time of day to sample on each date.
        :return: Async generator of :class:`~.Trade` objects.
        """
        return self.get_chain_trades(
            symbol, expiration,
            strike=strike, right=right,
            start_date=start_date, end_date=end_date, time=time,
        )

    # ── Historical interval-based generators ─────────────────────────────────

    def get_historical_quotes(
        self,
        symbol: str,
        expiration: DateValue,
        strike: PriceValue,
        right: OptionRight,
        interval: int | str | Interval,
        *,
        start_date: DateValue,
        end_date: DateValue,
        start_time: Optional[TimeValue] = None,
        end_time: Optional[TimeValue] = None,
    ) -> AsyncGenerator[Quote, None]:
        """Get interval-sampled historical quotes for a specific option contract.

        Returns quotes aggregated at ``interval`` frequency over the specified
        date and time range. Use :attr:`~.Interval.TICK` for tick-level data.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :param strike: Strike price.
        :param right: Option right.
        :param interval: Sampling interval accepted by :meth:`~.Interval.parse`.
        :param start_date: First date to include.
        :param end_date: Last date to include.
        :param start_time: Earliest time of day to include. Optional.
        :param end_time: Latest time of day to include. Optional.
        :return: Async generator of :class:`~.Quote` objects.
        """
        interval_obj = Interval.parse(interval)
        params: Dict[str, Any] = {
            'symbol': symbol,
            'expiration': format_date(expiration),
            'strike': format_price(strike),
            'right': right,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'interval': interval_obj,
        }
        if start_time is not None:
            params['start_time'] = format_time(start_time)
        if end_time is not None:
            params['end_time'] = format_time(end_time)

        ms = interval_obj.to_milliseconds()
        split_days = self.date_range_params(3 if ms < 2 * 60 * 1000 else 7)
        return self._gen_quotes(
            params, self.stream_data('option', 'history', 'quote', params_gen=split_days, **params)
        )

    def get_historical_ohlc(
        self,
        symbol: str,
        expiration: DateValue,
        strike: PriceValue,
        right: OptionRight,
        interval: int | str | Interval,
        *,
        start_date: DateValue,
        end_date: DateValue,
        start_time: Optional[TimeValue] = None,
        end_time: Optional[TimeValue] = None,
    ) -> AsyncGenerator[OhlcReport, None]:
        """Get historical OHLC bars for a specific option contract.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :param strike: Strike price.
        :param right: Option right.
        :param interval: Sampling interval accepted by :meth:`~.Interval.parse`.
        :param start_date: First date to include.
        :param end_date: Last date to include.
        :param start_time: Earliest time of day to include. Optional.
        :param end_time: Latest time of day to include. Optional.
        :return: Async generator of :class:`~.OhlcReport` objects.
        """
        interval_obj = Interval.parse(interval)
        params: Dict[str, Any] = {
            'symbol': symbol,
            'expiration': format_date(expiration),
            'strike': format_price(strike),
            'right': right,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'interval': interval_obj,
        }
        if start_time is not None:
            params['start_time'] = format_time(start_time)
        if end_time is not None:
            params['end_time'] = format_time(end_time)

        split_days = self.date_range_params(7)
        return self._gen_ohlc(
            params, self.stream_data('option', 'history', 'ohlc', params_gen=split_days, **params)
        )

    def get_historical_greeks(
        self,
        symbol: str,
        expiration: DateValue,
        interval: int | str | Interval,
        *,
        strike: Optional[PriceValue] = None,
        right: Optional[OptionRight] = None,
        start_date: Optional[DateValue] = None,
        end_date: Optional[DateValue] = None,
        date: Optional[DateValue] = None,
        start_time: Optional[TimeValue] = None,
        end_time: Optional[TimeValue] = None,
        order: GreeksOrder = GreeksOrder.FIRST,
    ) -> AsyncGenerator[FirstOrderGreeks, None]:
        """Get historical greeks for option contracts at a given interval.

        Specify either ``date`` for a single day or ``start_date`` +
        ``end_date`` for a range. ``strike`` and ``right`` are optional;
        omitting them returns greeks for all contracts.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :param interval: Sampling interval accepted by :meth:`~.Interval.parse`.
        :param strike: Strike price. Optional.
        :param right: Option right. Optional.
        :param start_date: First date. Mutually exclusive with ``date``.
        :param end_date: Last date. Requires ``start_date``.
        :param date: Single date. Mutually exclusive with ``start_date``.
        :param start_time: Earliest time of day to include. Optional.
        :param end_time: Latest time of day to include. Optional.
        :param order: Greeks order (first, second, third). Standard subscriptions
            support first order only.
        :return: Async generator of :class:`~.FirstOrderGreeks` objects.
        """
        interval_obj = Interval.parse(interval)
        params: Dict[str, Any] = {
            'symbol': symbol,
            'expiration': format_date(expiration),
            'interval': interval_obj,
        }
        if date:
            params['date'] = format_date(date)
        elif start_date and end_date:
            params['start_date'] = format_date(start_date)
            params['end_date'] = format_date(end_date)
        else:
            raise ValueError('Must specify either date or start_date and end_date')
        if strike:
            params['strike'] = format_price(strike)
        if right:
            params['right'] = right
        if start_time:
            params['start_time'] = format_time(start_time)
        if end_time:
            params['end_time'] = format_time(end_time)

        gen = self.stream_data('option', 'history', 'greeks', order.value, **params)

        async def _gen() -> AsyncGenerator[FirstOrderGreeks, None]:
            async for row in gen:
                parsed = parse_first_order_greeks(row)
                self._populate_entity_params(params, parsed)
                yield FirstOrderGreeks(**parsed)

        return _gen()

    # ── EOD ───────────────────────────────────────────────────────────────────

    def get_eod(
        self,
        symbol: str,
        expiration: DateValue,
        *,
        strike: Optional[PriceValue] = None,
        right: Optional[OptionRight] = None,
        start_date: DateValue,
        end_date: DateValue,
    ) -> AsyncGenerator[EodReport, None]:
        """Get end-of-day reports for option contracts over a date range.

        If ``strike`` and ``right`` are provided, returns EOD data for that
        specific contract. Otherwise returns EOD data for all contracts in
        the expiration.

        :param symbol: Option symbol (e.g. ``'SPXW'``).
        :param expiration: Option expiration date.
        :param strike: Strike price. Optional — omit for entire chain.
        :param right: Option right. Optional — omit for entire chain.
        :param start_date: First date to include.
        :param end_date: Last date to include.
        :return: Async generator of :class:`~.EodReport` objects.
        """
        params: Dict[str, Any] = {
            'symbol': symbol,
            'expiration': format_date(expiration),
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
        }
        if strike is not None:
            params['strike'] = format_price(strike)
        if right is not None:
            params['right'] = right

        split_days = self.date_range_params(30)
        raw = self.stream_data('option', 'history', 'eod', params_gen=split_days, **params)

        async def _gen() -> AsyncGenerator[EodReport, None]:
            async for row in raw:
                parsed = parse_eod_report(row)
                self._populate_entity_params(params, parsed)
                yield EodReport(**parsed)

        return _gen()


class ThetaStockClient(_ThetaClient):
    """Client for ThetaData stock endpoints."""

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _make_quote(self, symbol: str, row: Dict[str, str]) -> Quote:
        fields = parse_quote_fields(row)
        fields.pop('symbol', None)  # symbol is on the entity, not a Quote kwarg
        return Quote(Stock.create(symbol=symbol), **fields)

    def _make_ohlc(self, symbol: str, row: Dict[str, str]) -> OhlcReport:
        fields = parse_ohlc_report(row)
        fields.pop('symbol', None)
        return OhlcReport(Stock.create(symbol=symbol), **fields)

    def _at_time_stream(
        self, request: str, symbol: str, start_date: str, end_date: str, time: str,
    ) -> AsyncGenerator[Dict[str, str], None]:
        params = {
            'symbol': symbol,
            'start_date': start_date,
            'end_date': end_date,
            'time_of_day': time,
        }
        return self.stream_data('stock', 'at_time', request, params_gen=self.date_range_params(30), **params)

    # ── Discovery ─────────────────────────────────────────────────────────────

    async def get_symbols(self) -> List[str]:
        """Return all available stock symbols.

        :return: List of symbol strings.
        """
        symbols = [r['symbol'] async for r in self.stream_data('stock', 'list', 'symbols')]
        return _filter_symbols(symbols)

    # ── Single-item methods (Optional[T]; overloaded on time) ────────────────

    async def get_quote(
        self,
        symbol: str,
        *,
        venue: Optional[str] = None,
        time: Optional[DateTimeValue] = None,
    ) -> Optional[Quote]:
        """Get the quote for a stock.

        Returns the current snapshot quote when ``time`` is omitted.
        Returns the historical quote at the specified time when ``time`` is given.

        :param symbol: Stock symbol (e.g. ``'ZBRA'``).
        :param venue: Optional venue override (e.g. ``'utp_cta'`` for 15-min
            delayed data on the Value subscription). Only used for current quotes.
        :param time: If provided, returns the historical quote at this time.
            If omitted, returns the current snapshot quote.
        :return: :class:`~.Quote`, or ``None`` if no data.
        """
        if time is not None:
            date_str, time_str = format_date_time(time)
            gen = self._at_time_stream('quote', symbol, date_str, date_str, time_str)
            async for row in gen:
                return self._make_quote(symbol, row)
            return None

        params: Dict[str, Any] = {'symbol': symbol}
        if venue is not None:
            params['venue'] = venue
        async for row in self.stream_data('stock', 'snapshot', 'quote', **params):
            return self._make_quote(symbol, row)
        return None

    async def get_last_trade(
        self,
        symbol: str,
        *,
        time: Optional[DateTimeValue] = None,
    ) -> Optional[Trade]:
        """Get the last trade for a stock.

        Returns the current snapshot last trade when ``time`` is omitted.
        Returns the historical last trade at the specified time when ``time`` is given.

        :param symbol: Stock symbol (e.g. ``'ZBRA'``).
        :param time: If provided, returns the historical trade at this time.
            If omitted, returns the current snapshot trade.
        :return: :class:`~.Trade`, or ``None`` if no data.
        """
        if time is not None:
            date_str, time_str = format_date_time(time)
            gen = self._at_time_stream('trade', symbol, date_str, date_str, time_str)
        else:
            gen = self.stream_data('stock', 'snapshot', 'trade', symbol=symbol)

        async for row in gen:
            return Trade(Stock.create(symbol=symbol), **parse_trade_fields(row))
        return None

    async def get_ohlc(
        self,
        symbol: str,
        *,
        venue: Optional[str] = None,
    ) -> Optional[OhlcReport]:
        """Get the current day OHLC report for a stock.

        :param symbol: Stock symbol (e.g. ``'ZBRA'``).
        :param venue: Optional venue override (e.g. ``'utp_cta'`` for 15-min
            delayed data on the Value subscription).
        :return: :class:`~.OhlcReport`, or ``None`` if no data.
        """
        params: Dict[str, Any] = {'symbol': symbol}
        if venue is not None:
            params['venue'] = venue
        async for row in self.stream_data('stock', 'snapshot', 'ohlc', **params):
            return self._make_ohlc(symbol, row)
        return None

    # ── Multi-day at-time series ──────────────────────────────────────────────

    async def get_quotes(
        self,
        symbol: str,
        *,
        start_date: DateValue,
        end_date: DateValue,
        time: TimeValue,
    ) -> AsyncGenerator[Quote, None]:
        """Get quotes for a stock at the same time across a date range.

        Returns one :class:`~.Quote` per trading day — the quote nearest to
        but not after ``time`` on each day.

        :param symbol: Stock symbol (e.g. ``'ZBRA'``).
        :param start_date: First date in the range.
        :param end_date: Last date in the range.
        :param time: Time of day to sample on each date.
        :return: Async generator of :class:`~.Quote` objects.
        """
        gen = self._at_time_stream(
            'quote', symbol,
            format_date(start_date), format_date(end_date), format_time(time),
        )
        async for row in gen:
            yield self._make_quote(symbol, row)

    async def get_trades(
        self,
        symbol: str,
        *,
        start_date: DateValue,
        end_date: DateValue,
        time: TimeValue,
    ) -> AsyncGenerator[Trade, None]:
        """Get trades for a stock at the same time across a date range.

        Returns one :class:`~.Trade` per trading day — the trade nearest to
        but not after ``time`` on each day.

        :param symbol: Stock symbol (e.g. ``'ZBRA'``).
        :param start_date: First date in the range.
        :param end_date: Last date in the range.
        :param time: Time of day to sample on each date.
        :return: Async generator of :class:`~.Trade` objects.
        """
        gen = self._at_time_stream(
            'trade', symbol,
            format_date(start_date), format_date(end_date), format_time(time),
        )
        async for row in gen:
            yield Trade(Stock.create(symbol=symbol), **parse_trade_fields(row))

    # ── Historical interval-based generators ─────────────────────────────────

    async def get_historical_ohlc(
        self,
        symbol: str,
        interval: int | str | Interval,
        *,
        start_date: DateValue,
        end_date: DateValue,
        start_time: Optional[TimeValue] = None,
        end_time: Optional[TimeValue] = None,
        venue: Optional[str] = None,
    ) -> AsyncGenerator[OhlcReport, None]:
        """Get historical OHLC bars for a stock.

        :param symbol: Stock symbol (e.g. ``'ZBRA'``).
        :param interval: Sampling interval accepted by :meth:`~.Interval.parse`.
        :param start_date: First date to include.
        :param end_date: Last date to include.
        :param start_time: Earliest time of day to include. Optional.
        :param end_time: Latest time of day to include. Optional.
        :param venue: Optional venue override. Optional.
        :return: Async generator of :class:`~.OhlcReport` objects.
        """
        interval_obj = Interval.parse(interval)
        params: Dict[str, Any] = {
            'symbol': symbol,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'interval': interval_obj,
        }
        if start_time is not None:
            params['start_time'] = format_time(start_time)
        if end_time is not None:
            params['end_time'] = format_time(end_time)
        if venue is not None:
            params['venue'] = venue

        split_days = self.date_range_params(7)
        async for row in self.stream_data('stock', 'history', 'ohlc', params_gen=split_days, **params):
            yield self._make_ohlc(symbol, row)

    # ── EOD ───────────────────────────────────────────────────────────────────

    async def get_eod(
        self,
        symbol: str,
        *,
        start_date: DateValue,
        end_date: DateValue,
    ) -> AsyncGenerator[EodReport, None]:
        """Get end-of-day reports for a stock over a date range.

        :param symbol: Stock symbol (e.g. ``'ZBRA'``).
        :param start_date: First date to include.
        :param end_date: Last date to include.
        :return: Async generator of :class:`~.EodReport` objects.
        """
        params: Dict[str, Any] = {
            'symbol': symbol,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
        }
        split_days = self.date_range_params(30)
        async for row in self.stream_data('stock', 'history', 'eod', params_gen=split_days, **params):
            fields = parse_eod_report(row)
            fields.pop('symbol', None)
            yield EodReport(Stock.create(symbol=symbol), **fields)


class ThetaIndexClient(_ThetaClient):
    """Client for ThetaData index endpoints."""

    # ── Internal helpers ─────────────────────────────────────────────────────

    async def _gen_index_prices(
        self, symbol: str, gen: AsyncGenerator[Dict[str, str], None]
    ) -> AsyncGenerator[IndexPriceReport, None]:
        async for data in gen:
            parsed = parse_index_price_report(data)
            if parsed['price'] != 0:  # filter off-hours zero prices
                yield IndexPriceReport(entity=Index.create(symbol=symbol), **parsed)

    # ── Discovery ─────────────────────────────────────────────────────────────

    async def get_symbols(self) -> List[str]:
        """Return all available index symbols.

        :return: List of symbol strings (e.g. ``['SPX', 'NDX', ...]``).
        """
        symbols = [r['symbol'] async for r in self.stream_data('index', 'list', 'symbols')]
        return _filter_symbols(symbols)

    async def get_dates(self, symbol: str) -> List[str]:
        """Return all dates for which data is available for an index.

        :param symbol: Index symbol (e.g. ``'SPX'``).
        :return: List of date strings.
        """
        return [r['date'] async for r in self.stream_data('index', 'list', 'dates', symbol=symbol)]

    # ── Single-item methods (Optional[T]; overloaded on time) ────────────────

    async def get_price(
        self,
        symbol: str,
        *,
        time: Optional[DateTimeValue] = None,
    ) -> Optional[IndexPriceReport]:
        """Get the price for an index.

        Returns the current snapshot price when ``time`` is omitted.
        Returns the historical price at the specified time when ``time`` is given.

        :param symbol: Index symbol (e.g. ``'SPX'``).
        :param time: If provided, returns the historical price at this time.
            If omitted, returns the current snapshot price.
        :return: :class:`~.IndexPriceReport`, or ``None`` if no data.
        """
        if time is not None:
            date_str, time_str = format_date_time(time)
            gen = self._gen_index_prices(symbol, self.stream_data(
                'index', 'at_time', 'price',
                params_gen=self.date_range_params(30),
                symbol=symbol,
                start_date=date_str,
                end_date=date_str,
                time_of_day=time_str,
            ))
        else:
            gen = self._gen_index_prices(
                symbol, self.stream_data('index', 'snapshot', 'price', symbol=symbol)
            )
        return await anext(gen, None)

    async def get_ohlc(
        self,
        symbol: str,
    ) -> Optional[OhlcReport]:
        """Get the current day OHLC report for an index.

        :param symbol: Index symbol (e.g. ``'SPX'``).
        :return: :class:`~.OhlcReport`, or ``None`` if no data.
        """
        async for row in self.stream_data('index', 'snapshot', 'ohlc', symbol=symbol):
            fields = parse_ohlc_report(row)
            fields.pop('symbol', None)
            return OhlcReport(Index.create(symbol=symbol), **fields)
        return None

    # ── Multi-day at-time series ──────────────────────────────────────────────

    def get_prices(
        self,
        symbol: str,
        *,
        start_date: DateValue,
        end_date: DateValue,
        time: TimeValue,
    ) -> AsyncGenerator[IndexPriceReport, None]:
        """Get index prices at the same time across a date range.

        Returns one :class:`~.IndexPriceReport` per trading day — the price
        nearest to but not after ``time`` on each day.

        :param symbol: Index symbol (e.g. ``'SPX'``).
        :param start_date: First date in the range.
        :param end_date: Last date in the range.
        :param time: Time of day to sample on each date (e.g. ``'16:00:00'``
            for daily close).
        :return: Async generator of :class:`~.IndexPriceReport` objects.
        """
        params = {
            'symbol': symbol,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'time_of_day': format_time(time),
        }
        split_days = self.date_range_params(30)
        return self._gen_index_prices(
            symbol,
            self.stream_data('index', 'at_time', 'price', params_gen=split_days, **params),
        )

    # ── Historical interval-based generators ─────────────────────────────────

    def get_historical_prices(
        self,
        symbol: str,
        interval: int | str | Interval,
        *,
        start_date: DateValue,
        end_date: DateValue,
    ) -> AsyncGenerator[IndexPriceReport, None]:
        """Get historical index prices at a given interval.

        Returns price-only data (no OHLC). For OHLC bars use
        :meth:`get_historical_ohlc`.

        Zero-price records (off-hours placeholders) are filtered out
        automatically.

        :param symbol: Index symbol (e.g. ``'SPX'``).
        :param interval: Sampling interval accepted by :meth:`~.Interval.parse`.
        :param start_date: First date to include.
        :param end_date: Last date to include.
        :return: Async generator of :class:`~.IndexPriceReport` objects.
        """
        interval_obj = Interval.parse(interval)
        params = {
            'symbol': symbol,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'interval': interval_obj,
        }
        ms = interval_obj.to_milliseconds()
        split_days = self.date_range_params(3 if ms <= 2 * 60 * 1000 else 7)
        return self._gen_index_prices(
            symbol,
            self.stream_data('index', 'history', 'price', params_gen=split_days, **params),
        )

    async def get_historical_ohlc(
        self,
        symbol: str,
        interval: int | str | Interval,
        *,
        start_date: DateValue,
        end_date: DateValue,
    ) -> AsyncGenerator[OhlcReport, None]:
        """Get historical OHLC bars for an index.

        For price-only data use :meth:`get_historical_prices`.

        :param symbol: Index symbol (e.g. ``'SPX'``).
        :param interval: Sampling interval accepted by :meth:`~.Interval.parse`.
        :param start_date: First date to include.
        :param end_date: Last date to include.
        :return: Async generator of :class:`~.OhlcReport` objects.
        """
        interval_obj = Interval.parse(interval)
        params = {
            'symbol': symbol,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'interval': interval_obj,
        }
        split_days = self.date_range_params(7)
        async for row in self.stream_data('index', 'history', 'ohlc', params_gen=split_days, **params):
            fields = parse_ohlc_report(row)
            fields.pop('symbol', None)
            yield OhlcReport(Index.create(symbol=symbol), **fields)


class ThetaClient:
    """
    A ThetaData client that combines the option, stock, and index APIs.
    """
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

        self._option = None
        self._stock = None
        self._index = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_, **__):
        await self.close()

    def _get_client(self, name: str, klass: type) -> _ThetaClient:
        client = getattr(self, name, None)
        if client is None:
            client = klass(*self._args, **self._kwargs)
            setattr(self, name, client)

        return client

    async def _close_client(self, name) -> None:
        client = getattr(self, name, None)
        if client is not None:
            setattr(self, name, None)
            await client.close()

    @property
    def option(self) -> ThetaOptionClient:
        return self._get_client('_option', ThetaOptionClient)

    @property
    def stock(self) -> ThetaStockClient:
        return self._get_client('_stock', ThetaStockClient)

    @property
    def index(self) -> ThetaIndexClient:
        return self._get_client('_index', ThetaIndexClient)

    async def close(self):
        for client in ('_option', '_stock', '_index'):
            await self._close_client(client)
