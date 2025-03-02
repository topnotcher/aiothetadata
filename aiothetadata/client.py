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
    DEFAULT_URL = 'http://127.0.0.1:25510/'

    def __init__(self, url: Optional[str]=None):
        if url is None:
            url = self.DEFAULT_URL

        self._base = url
        self._path = ('v2',)

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
        get_params['use_csv'] = 'true'

        if params_gen is None:
            params_gen = self._single_params(get_params)
        else:
            params_gen = params_gen(get_params)

        async with _PagedRequest(self.session, self._build_url(*pcs), params_gen) as pages:
            async for resp in pages:

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

    async def get_symbols(self) -> List[str]:
        return [r['root'] async for r in self.stream_data('list/roots/option')]

    @staticmethod
    def _populate_entity_params(request: Dict[str, Any], params: Dict[str, Any]) -> None:
        entity_params = {}
        for param in ('symbol', 'right', 'strike', 'expiration'):
            entity_params[param] = params.pop(param, None)

        if not entity_params.get('symbol'):
            entity_params['symbol'] = request['root']

        if not entity_params.get('right'):
            entity_params['right'] = request['right']

        if not entity_params.get('strike'):
            entity_params['strike'] = parse_strike(request['strike'])

        entity_params['expiration'] = parse_date(request['exp'])

        params['entity'] = Option.create(**entity_params)

    def _make_quote(self, request: Dict[str, Any], response: Dict[str, Any]) -> Quote:
        params = parse_quote_fields(response)
        self._populate_entity_params(request, params)
        return Quote(**params)

    def _make_trade(self, request: Dict[str, Any], response: Dict[str, Any]) -> Trade:
        params = parse_trade_fields(response)
        self._populate_entity_params(request, params)
        return Trade(**params)

    def _get_at_time(
        self, request: str, *, symbol: str, expiration: DateValue,
        start_date: str, end_date: str, time: int,
        strike: Optional[PriceValue]=None, right: Optional[OptionRight],
    ) -> Tuple[Dict[str, Any], AsyncGenerator[Dict[str, str], None]]:

        params = {
            'root': symbol,
            'exp': format_date(expiration),
            'start_date': start_date,
            'end_date': end_date,
            'ivl': time,
            'rth': 'false'
        }

        if strike and right:
            params['strike'] = format_price(strike)
            params['right'] = right

            days = 30
            get_type = 'at_time'

        else:
            days = 5
            get_type = 'bulk_at_time'

        split_days = self.date_range_params(days)

        return params, self.stream_data(get_type, 'option', request,  params_gen=split_days, **params)

    async def _gen_quotes(
        self, params: Dict[str, Any], gen: AsyncGenerator[Dict[str, str], None]
    ) -> AsyncGenerator[Quote, None]:

        async for row in gen:
            # Looks like a weekend quote?
            # 0,0,0,0.0000,0,0,0,0.0000,0,0
            if row['date'] != '0':  # TODO: what are these???
                yield self._make_quote(params, row)

    def _get_quotes_at_time(
        self, symbol: str, expiration: DateValue, start_date: str, end_date: str, time: int,
        strike: Optional[PriceValue]=None, right: Optional[OptionRight]=None,
    ) -> AsyncGenerator[Quote, None]:

        params, gen = self._get_at_time(
            request='quote', symbol=symbol, expiration=expiration, strike=strike,
            right=right, start_date=start_date, end_date=end_date, time=time,
        )

        return self._gen_quotes(params, gen)

    def get_quotes_at_time(
        self, symbol: str, expiration: DateValue, strike: PriceValue,
        right: OptionRight, start_date: DateValue, end_date: DateValue,
        time: TimeValue,
    ) -> AsyncGenerator[Quote, None]:
        """
        Get quotes at a specific time of day for a range of days.
        """
        start_date = format_date(start_date)
        end_date = format_date(end_date)
        time = format_time(time)

        return self._get_quotes_at_time(
            symbol=symbol, expiration=expiration, strike=strike, right=right,
            start_date=start_date, end_date=end_date, time=time
        )

    async def get_quote_at_time(
        self, symbol: str, expiration: DateValue, strike: PriceValue,
        right: OptionRight, time: DateTimeValue,
    ) -> Quote:
        """
        Get an option quote at a given time.
        """
        quote_date, quote_time = format_date_time(time)

        gen = self._get_quotes_at_time(
            symbol=symbol, expiration=expiration, strike=strike, right=right,
            start_date=quote_date, end_date=quote_date, time=quote_time,
        )

        return await anext(gen)

    def get_all_quotes_at_time(
        self, symbol: str, expiration: DateValue, start_date: DateValue, end_date: DateValue, time: TimeValue,
    ) -> AsyncGenerator[Quote, None]:
        """
        Get quotes at a specific time of day for all contracts for a range of
        days.
        """
        start_date = format_date(start_date)
        end_date = format_date(end_date)
        time = format_time(time)

        return self._get_quotes_at_time(
            symbol=symbol, expiration=expiration, start_date=start_date, end_date=end_date, time=time
        )

    async def _get_trades_at_time(
        self, symbol: str, expiration: DateValue, start_date: str, end_date: str, time: int,
        strike: Optional[PriceValue]=None, right: Optional[OptionRight]=None,
    ) -> AsyncGenerator[Trade, None]:

        params, gen = self._get_at_time(
            request='trade', symbol=symbol, expiration=expiration, strike=strike,
            right=right, start_date=start_date, end_date=end_date, time=time,
        )

        async for row in gen:
            yield self._make_trade(params, row)

    def get_trades_at_time(
        self, symbol: str, expiration: DateValue, strike: PriceValue,
        right: OptionRight, start_date: DateValue, end_date: DateValue,
        time: TimeValue,
    ) -> AsyncGenerator[Trade, None]:
        """
        Get trades at a specific time of day for a range of days.
        """
        start_date = format_date(start_date)
        end_date = format_date(end_date)
        time = format_time(time)

        return self._get_trades_at_time(
            symbol=symbol, expiration=expiration, strike=strike, right=right,
            start_date=start_date, end_date=end_date, time=time
        )

    async def get_trade_at_time(
        self, symbol: str, expiration: DateValue, strike: PriceValue,
        right: OptionRight, time: DateTimeValue,
    ) -> Trade:
        """
        Get an option trades at a given time.
        """
        trade_date, trade_time = format_date_time(time)

        gen = self._get_trades_at_time(
            symbol=symbol, expiration=expiration, strike=strike, right=right,
            start_date=trade_date, end_date=trade_date, time=trade_time,
        )

        return await anext(gen)

    def get_all_trades_at_time(
        self, symbol: str, expiration: DateValue, start_date: DateValue, end_date: DateValue, time: TimeValue,
    ) -> AsyncGenerator[Quote, None]:
        """
        Get trades at a specific time of day for all contracts for a range of
        days.
        """
        start_date = format_date(start_date)
        end_date = format_date(end_date)
        time = format_time(time)

        return self._get_trades_at_time(
            symbol=symbol, expiration=expiration, start_date=start_date, end_date=end_date, time=time
        )

    async def get_eod_report(
        self, symbol: str, expiration: DateValue, strike: PriceValue,
        right: OptionRight, date: DateValue,
    ) -> EodReport:

        report_date = format_date(date)

        params = {
            'root': symbol,
            'start_date': report_date,
            'end_date': report_date,
            'exp': format_date(expiration),
            'strike': format_price(strike),
            'right': right,
        }

        result = (await self.get_data('hist/option/eod',  **params))[0]
        report = parse_eod_report(result)
        self._populate_entity_params(params, report)

        return EodReport(**report)

    def get_historical_quotes(
        self, symbol: str, expiration: DateValue, strike: PriceValue,
        right: OptionRight, start_date: DateValue, end_date: DateValue,
        start_time: TimeValue, end_time: TimeValue, interval: int | Interval,
    ) -> AsyncGenerator[Quote, None]:
        """
        Get all quotes for the specified instrument in a given time range for a
        range of days.

        :param symbol: The symbol.
        :param expiration: The option expiration.
        :param strike: The option strike.
        :param right: The option right.
        :param start_date: The first date to get quotes for.
        :param end_date: The last date to get quotes for.
        :param start_time: The starting time to get quotes for.
        :param end_time: The starting time to get quotes for.
        :param interval: The interval apart, in milliseconds, to get quotes
            for. Use :attr:`~.Interval.TICK` for tick-level quotes.
        """
        params = {
            'root': symbol,
            'exp': format_date(expiration),
            'strike': format_price(strike),
            'right': right,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'start_time': format_time(start_time),
            'end_time': format_time(end_time),
            'ivl': interval,

            # TODO: Does rth matter? The user is specifying a time range.
            'rth': 'false'
        }

        # TODO: for tick level, docs suggest 1 week or 7 days for higher
        # activity tickers. I could probabably base this on the time range as
        # well as the interval.
        if interval <= 2 * 60:
            split_days = self.date_range_params(3)

        else:
            split_days = self.date_range_params(7)

        gen = self.stream_data('hist/option/quote', params_gen=split_days, **params)
        return self._gen_quotes(params, gen)


class ThetaStockClient(_ThetaClient):

    async def get_symbols(self) -> List[str]:
        return [r['root'] async for r in self.stream_data('list/roots/stock')]

    def _get_at_time(
        self, request: str, symbol: str, start_date: str, end_date: str, time: int,
    ) -> AsyncGenerator[Dict[str, str], None]:

        params = {
            'root': symbol,
            'start_date': start_date,
            'end_date': end_date,
            'ivl': time,
            'venue': 'utp_cta',  # TODO
            'rth': 'false'
        }

        split_days = self.date_range_params(30)

        return self.stream_data('at_time/stock', request, params_gen=split_days, **params)

    async def _get_quotes_at_time(
        self, symbol: str, start_date: str, end_date: str, time: int,
    ) -> AsyncGenerator[Stock, None]:

        gen = self._get_at_time(
            request='quote', symbol=symbol, start_date=start_date, end_date=end_date, time=time,
        )

        async for row in gen:
            yield Quote(Stock.create(symbol=symbol), **parse_quote_fields(row))

    async def _get_trades_at_time(
        self, symbol: str, start_date: str, end_date: str, time: int,
    ) -> AsyncGenerator[Trade, None]:

        gen = self._get_at_time(
            request='trade', symbol=symbol, start_date=start_date, end_date=end_date, time=time,
        )

        async for row in gen:
            yield Trade(Stock.create(symbol=symbol), **parse_trade_fields(row))

    async def get_quotes_at_time(
        self, symbol: str, start_date: DateValue, end_date: DateValue, time: TimeValue,
    ) -> AsyncGenerator[Quote, None]:
        """
        Get quotes at a specific time of day for a range of days.
        """
        start_date = format_date(start_date)
        end_date = format_date(end_date)
        time = format_time(time)

        gen = self._get_quotes_at_time(
            symbol=symbol, start_date=start_date, end_date=end_date, time=time
        )

        async for quote in gen:
            yield quote

    async def get_quote_at_time(self, symbol: str, time: DateTimeValue) -> Quote:
        """
        Get a stock quote at a given time.
        """
        quote_date, quote_time = format_date_time(time)

        gen = self._get_quotes_at_time(
            symbol=symbol, start_date=quote_date, end_date=quote_date, time=quote_time
        )

        return await anext(gen)

    async def get_trades_at_time(
        self, symbol: str, start_date: DateValue, end_date: DateValue, time: TimeValue,
    ) -> AsyncGenerator[Trade, None]:
        """
        Get trades at a specific time of day for a range of days.
        """
        start_date = format_date(start_date)
        end_date = format_date(end_date)
        time = format_time(time)

        gen = self._get_trades_at_time(
            symbol=symbol, start_date=start_date, end_date=end_date, time=time
        )

        async for trade in gen:
            yield trade

    async def get_trade_at_time(self, symbol: str, time: DateTimeValue) -> Trade:
        """
        Get a stock trade at a given time.
        """
        trade_date, trade_time = format_date_time(time)

        gen = self._get_trades_at_time(
            symbol=symbol, start_date=trade_date, end_date=trade_date, time=trade_time
        )

        return await anext(gen)

    async def get_eod_report(self, symbol: str, date: DateValue) -> EodReport:

        report_date = format_date(date)

        params = {
            'root': symbol,
            'start_date': report_date,
            'end_date': report_date,
        }

        result = (await self.get_data('hist/stock/eod',  **params))[0]
        report = parse_eod_report(result)

        return EodReport(Stock.create(symbol=symbol), **report)


class ThetaIndexClient(_ThetaClient):

    async def get_historical_prices(
        self, symbol: str, start_date: DateValue, end_date: DateValue,
        interval: Interval, hours: TradingHours=TradingHours.REGULAR,
    ) -> AsyncGenerator[IndexPriceReport, None]:

        params = {
            'root': symbol,
            'start_date': format_date(start_date),
            'end_date': format_date(end_date),
            'ivl': interval,
            # TODO: add my own params serialization because I'm sick of this.
            'rth': 'true' if (hours == TradingHours.REGULAR) else 'false',
        }

        # TODO: copied this from options. Need to come up with better numbers
        # and generalize.
        if interval <= 2 * 60:
            split_days = self.date_range_params(3)

        else:
            split_days = self.date_range_params(7)

        gen = self.stream_data('hist/index/price',  params_gen=split_days, **params)
        async for data in gen:
            parsed = parse_index_price_report(data)
            # TODO: at least for SPX (not quoted off hours), I get $0 quotes
            # starting at midnight.
            if parsed['price'] != 0:
                yield IndexPriceReport(entity=Index.create(symbol=symbol), **parsed)


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
