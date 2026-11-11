"""Tests for the ThetaClient base class (pagination, HTTP errors)."""
import socket

import aiohttp
import pytest
from aiohttp import web

from aiothetadata.client import ThetaOptionClient, ThetaDataHttpError, PermissionDeniedError
from aiothetadata.constants import OptionRight
from aiothetadata import datetime

from .utils import csv_response, BaseThetaClientTest


QUOTE_HEADER = [
    'symbol', 'expiration', 'strike', 'right', 'timestamp',
    'bid_size', 'bid_exchange', 'bid', 'bid_condition',
    'ask_size', 'ask_exchange', 'ask', 'ask_condition',
]


class TestThetaClientPagination(BaseThetaClientTest):
    """Pagination behaviour (Next-Page header, date-range splitting)."""

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_paged_response(self):
        """Should follow Next-Page header and concatenate results."""
        first_page = ['MSFT', 'AAPL', 'SPX']
        second_page = ['SMCI', 'AVGO', 'ZBRA']

        async def get_roots(request):
            response = csv_response(['symbol'], [[v] for v in first_page])
            response.headers['Next-Page'] = self.make_url('/page/1')
            return response

        async def get_second_page(request):
            return csv_response(['symbol'], [[v] for v in second_page])

        self.handler.register('/v3/option/list/symbols', get_roots)
        self.handler.register('/page/1', get_second_page)

        symbols = await self.client.get_symbols()
        assert symbols == first_page + second_page

    async def test_paged_request(self):
        """Large date ranges should be split into 30-day chunks."""
        quote = {
            'symbol': '"SPXW"', 'expiration': '2024-03-15', 'strike': '6000.000',
            'right': 'PUT', 'timestamp': '2025-02-17T10:00:00',
            'bid_size': '169', 'bid_exchange': '5', 'bid': '5.0000', 'bid_condition': '50',
            'ask_size': '30', 'ask_exchange': '5', 'ask': '5.2000', 'ask_condition': '50',
        }
        ranges = []

        async def get_quotes(request):
            ranges.append((request.query['start_date'], request.query['end_date']))
            return csv_response(quote.keys(), [quote.values()])

        self.handler.register('/v3/option/at_time/quote', get_quotes)

        async for _ in self.client.get_quotes(
            symbol='SPXW', expiration=20240315, strike=6000,
            right=OptionRight.PUT, start_date=20240101, end_date=20240331,
            time='10:00:00',
        ):
            pass

        expected = [
            ('20240101', '20240130'), ('20240131', '20240229'),
            ('20240301', '20240330'), ('20240331', '20240331'),
        ]
        assert ranges == expected

    async def test_connection_refused_raises(self):
        """A request that can't connect should raise, not end with no data."""
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]

        async with ThetaOptionClient(f'http://127.0.0.1:{port}/') as client:
            with pytest.raises(aiohttp.ClientConnectionError):
                await client.get_symbols()

    async def test_failed_page_raises_after_earlier_pages(self):
        """The pages before a failed one should be yielded, then the error raised."""
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]

        async def get_roots(request):
            response = csv_response(['symbol'], [['MSFT'], ['AAPL']])
            response.headers['Next-Page'] = f'http://127.0.0.1:{port}/page/1'
            return response

        self.handler.register('/v3/option/list/symbols', get_roots)

        symbols = []
        with pytest.raises(aiohttp.ClientConnectionError):
            async for row in self.client.stream_data('option', 'list', 'symbols'):
                symbols.append(row['symbol'])
        assert symbols == ['MSFT', 'AAPL']

    async def test_params_error_raises(self):
        """An error building the next request should raise, not end the data."""
        def params(base):
            yield base
            raise ValueError('bad params')

        async def get_roots(request):
            return csv_response(['symbol'], [['MSFT']])

        self.handler.register('/v3/option/list/symbols', get_roots)

        symbols = []
        with pytest.raises(ValueError, match='bad params'):
            async for row in self.client.stream_data('option', 'list', 'symbols', params_gen=params):
                symbols.append(row['symbol'])
        assert symbols == ['MSFT']


class TestThetaClientHttpErrors(BaseThetaClientTest):
    """Non-472 HTTP errors should raise ThetaDataHttpError."""

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_non_472_error_raises(self):
        async def server_error(request):
            return web.Response(status=500, text='Internal server error')

        self.handler.register('/v3/option/at_time/quote', server_error)

        with pytest.raises(ThetaDataHttpError) as exc_info:
            await self.client.get_quote(
                symbol='SPXW', expiration=20250407, strike=4985,
                right=OptionRight.PUT, time='20250404 10:00:00',
            )
        assert exc_info.value.status == 500

    async def test_403_raises_permission_denied(self):
        """HTTP 403 should raise PermissionDeniedError (subclass of ThetaDataHttpError)."""
        async def forbidden(request):
            return web.Response(status=403, text='Subscription required')

        self.handler.register('/v3/option/snapshot/quote', forbidden)

        with pytest.raises(PermissionDeniedError) as exc_info:
            await self.client.get_quote(
                symbol='SPXW', expiration=20250407, strike=4985, right=OptionRight.PUT,
            )
        assert exc_info.value.status == 403

    async def test_471_raises_permission_denied(self):
        """HTTP 471 should also raise PermissionDeniedError."""
        async def forbidden(request):
            return web.Response(status=471, text='Permission denied')

        self.handler.register('/v3/option/snapshot/quote', forbidden)

        with pytest.raises(PermissionDeniedError) as exc_info:
            await self.client.get_quote(
                symbol='SPXW', expiration=20250407, strike=4985, right=OptionRight.PUT,
            )
        assert exc_info.value.status == 471

    async def test_permission_denied_is_http_error(self):
        """PermissionDeniedError should be catchable as ThetaDataHttpError."""
        async def forbidden(request):
            return web.Response(status=403, text='Subscription required')

        self.handler.register('/v3/option/snapshot/quote', forbidden)

        with pytest.raises(ThetaDataHttpError):
            await self.client.get_quote(
                symbol='SPXW', expiration=20250407, strike=4985, right=OptionRight.PUT,
            )
