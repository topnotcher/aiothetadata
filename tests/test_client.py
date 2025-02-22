import io
import csv
import unittest
from decimal import Decimal

import pytest
from aiohttp import web
from aiohttp.test_utils import RawTestServer, AioHTTPTestCase

from aiothetadata.client import ThetaClient, ThetaStockClient, ThetaOptionClient, ThetaIndexClient, ThetaDataHttpError
from aiothetadata.constants import *
from aiothetadata.types import *
from aiothetadata import datetime


QUOTE_HEADER = [
    'ms_of_day',
    'bid_size',
    'bid_exchange',
    'bid',
    'bid_condition',
    'ask_size',
    'ask_exchange',
    'ask',
    'ask_condition',
    'date',
]


def csv_response(header, rows):
    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow(header)
    for row in rows:
        if isinstance(row, str):
            row = row.split(',')

        writer.writerow(row)

    return web.Response(text=output.getvalue(), content_type='text/csv')


class Handler:
    def __init__(self):
        self.handlers = {}

    def register(self, path, handler):
        self.handlers[path] = handler

    async def __call__(self, request):
        return await self.handlers[request.path](request)


class BaseThetaClientTest(AioHTTPTestCase):
    async def asyncSetUp(self):
        self.handler = Handler()
        self.server = RawTestServer(self.handler)
        await self.server.start_server()
        self.client = await self.get_client(self.make_url('/'))

    async def get_client(self, url):
        raise NotImplementedError

    async def asyncTearDown(self):
        await self.server.close()
        await self.client.close()

    def make_url(self, *args, **kwargs):
        return str(self.server.make_url(*args, **kwargs))

    @staticmethod
    def ensure_csv(request):
        assert request.query['use_csv'] == 'true'


class TestThetaClient(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    # TODO: I really should refactor so I can test the paging separately.
    async def test_paged_response(self):
        first_page = ['MSFT', 'AAPL', 'SPX']
        second_page = ['SMCI', 'AVGO', 'ZBRA']

        def make_row(values):
            return [[value] for value in values]

        async def get_roots(request):
            self.ensure_csv(request)

            response = csv_response(['root'], make_row(first_page))
            response.headers['Next-Page'] = self.make_url('/page/1')

            return response

        async def get_second_page(request):
            return csv_response(['root'], make_row(second_page))

        self.handler.register('/v2/list/roots/option', get_roots)
        self.handler.register('/page/1', get_second_page)

        symbols = await self.client.get_symbols()
        assert symbols == first_page + second_page

    async def test_paged_request(self):
        quote = {
            'ms_of_day': '36000000',
            'bid_size': '169',
            'bid_exchange': '5',
            'bid': '5.0000',
            'bid_condition': '50',
            'ask_size': '30',
            'ask_exchange': '5',
            'ask': '5.2000',
            'ask_condition': '50',
            'date': '20250217',
        }
        ranges = []

        async def get_quotes(request):
            start = request.query['start_date']
            end = request.query['end_date']
            ranges.append((start, end))

            return csv_response(quote.keys(), [quote.values()])

        self.handler.register('/v2/at_time/option/quote', get_quotes)

        quotes = self.client.get_quotes_at_time(
            symbol='SPXW',
            expiration=20240315,
            strike=6000,
            right=OptionRight.PUT,
            start_date=20240101,
            end_date=20240331,
            time='10:00:00',
        )

        async for _quote in quotes:
            pass

        expected = [('20240101', '20240130'), ('20240131', '20240229'), ('20240301', '20240330'), ('20240331', '20240331')]
        assert ranges == expected


class TestThetaOptionClient(BaseThetaClientTest):
    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_get_symbols(self):
        roots = ['MSFT', 'AAPL', 'SPX']

        async def get_roots(request):
            self.ensure_csv(request)
            return csv_response(['root'], [[root] for root in roots])

        self.handler.register('/v2/list/roots/option', get_roots)

        symbols = await self.client.get_symbols()
        assert symbols == roots

    async def test_null_quote(self):
        quote_data = [
            # Test this weird thing that happens on weekends.
            '0,0,0,0.0000,0,0,0,0.0000,0,0',
            '36000000,1,1,325.3600,0,2,1,326.2800,0,20250219'
        ]

        async def get_quotes(request):
            self.ensure_csv(request)
            return csv_response(QUOTE_HEADER, quote_data)

        self.handler.register('/v2/at_time/option/quote', get_quotes)

        quote_gen = self.client.get_quotes_at_time(
            symbol='SPXW',
            expiration=20240315,
            strike=6000,
            right=OptionRight.PUT,
            start_date=20240220,
            end_date=20240229,
            time='10:00:00',
        )

        quotes = [q async for q in quote_gen]
        assert len(quotes) == 1
        assert quotes[0].ask == Decimal('326.2800')

    async def test_get_quote_at_time(self):
        quote_data = [
            '36000000,1,1,325.3600,0,2,3,326.2800,1,20250219'
        ]

        params = {}
        async def get_quotes(request):
            self.ensure_csv(request)
            params.update(request.query)
            return csv_response(QUOTE_HEADER, quote_data)

        self.handler.register('/v2/at_time/option/quote', get_quotes)

        quote = await self.client.get_quote_at_time(
            symbol='SPXW',
            expiration=20240315,
            strike=6000,
            right=OptionRight.PUT,
            time=datetime.datetime(2024, 3, 1, 10, 0, 0),
        )

        assert isinstance(quote, OptionQuote)
        expected = {
            'root': 'SPXW',
            'strike': '6000000',
            'exp': '20240315',
            'right': 'P',
            'start_date': '20240301',
            'end_date': '20240301',
            'ivl': '36000000',
            'rth': 'false',
            'use_csv': 'true'
        }
        assert params == expected

        assert quote.bid == Decimal('325.3600')
        assert quote.bid_size == 1
        assert quote.bid_exchange == Exchange.NQEX
        assert isinstance(quote.bid_exchange, Exchange)
        assert quote.bid_condition == QuoteCondition.REGULAR
        assert isinstance(quote.bid_condition, QuoteCondition)

        assert quote.ask == Decimal('326.2800')
        assert quote.ask_size == 2
        assert quote.ask_exchange == Exchange.NYSE
        assert isinstance(quote.ask_exchange, Exchange)
        assert quote.ask_condition == QuoteCondition.BID_ASK_AUTO_EXEC
        assert isinstance(quote.ask_condition, QuoteCondition)

        assert quote.time == datetime.datetime(2025, 2, 19, 10, 0, 0)


class TestThetaStockClient(BaseThetaClientTest):
    async def get_client(self, url):
        return ThetaStockClient(url)

    async def test_get_symbols(self):
        roots = ['MSFT', 'AAPL', 'ZBRA']

        async def get_roots(request):
            self.ensure_csv(request)
            return csv_response(['root'], [[root] for root in roots])

        self.handler.register('/v2/list/roots/stock', get_roots)

        symbols = await self.client.get_symbols()
        assert symbols == roots
