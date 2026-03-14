"""Tests for the ThetaClient base class (pagination, HTTP errors)."""
import pytest
from aiohttp import web

from aiothetadata.client import ThetaOptionClient, ThetaDataHttpError
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

        async for _ in self.client.get_quotes_at_time(
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


class TestThetaClientHttpErrors(BaseThetaClientTest):
    """Non-472 HTTP errors should raise ThetaDataHttpError."""

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_non_472_error_raises(self):
        async def server_error(request):
            return web.Response(status=500, text='Internal server error')

        self.handler.register('/v3/option/at_time/quote', server_error)

        with pytest.raises(ThetaDataHttpError) as exc_info:
            await self.client.get_quote_at_time(
                symbol='SPXW', expiration=20250407, strike=4985,
                right=OptionRight.PUT, time='20250404 10:00:00',
            )
        assert exc_info.value.status == 500
