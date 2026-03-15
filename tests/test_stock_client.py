"""Tests for ThetaStockClient."""
from decimal import Decimal

import pytest
from aiohttp import web

from aiothetadata.client import ThetaStockClient
from aiothetadata.constants import Exchange, QuoteCondition
from aiothetadata.types import Quote, Trade, OhlcReport, EodReport, Stock, FinancialEntityType
from aiothetadata import datetime as thetadt

from .utils import csv_response, BaseThetaClientTest


QUOTE_SNAPSHOT_HEADER = [
    'timestamp', 'symbol',
    'bid_size', 'bid_exchange', 'bid', 'bid_condition',
    'ask_size', 'ask_exchange', 'ask', 'ask_condition',
]

QUOTE_AT_TIME_HEADER = [
    'timestamp', 'symbol',
    'bid_size', 'bid_exchange', 'bid', 'bid_condition',
    'ask_size', 'ask_exchange', 'ask', 'ask_condition',
]

TRADE_SNAPSHOT_HEADER = [
    'timestamp', 'symbol', 'sequence', 'size', 'condition', 'price',
]

OHLC_SNAPSHOT_HEADER = [
    'timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'count',
]

OHLC_HISTORY_HEADER = [
    'timestamp', 'open', 'high', 'low', 'close', 'volume', 'count', 'vwap',
]

EOD_HEADER = [
    'created', 'last_trade', 'open', 'high', 'low', 'close',
    'volume', 'count',
    'bid_size', 'bid_exchange', 'bid', 'bid_condition',
    'ask_size', 'ask_exchange', 'ask', 'ask_condition',
]

SAMPLE_QUOTE_ROW = [
    '2026-03-12T19:59:01.112', 'ZBRA',
    '40', '1', '202.57', '0',
    '40', '1', '209.09', '0',
]

SAMPLE_OHLC_ROW = [
    '2026-03-13T19:58:51.324', 'ZBRA',
    '207.450', '207.650', '201.640', '202.720', '755370', '26005',
]


# ── Discovery ─────────────────────────────────────────────────────────────────

class TestThetaStockClientDiscovery(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaStockClient(url)

    async def test_get_symbols(self):
        async def handler(request):
            return csv_response(['symbol'], [['MSFT'], ['AAPL'], ['ZBRA']])

        h = self.handler.register('/v3/stock/list/symbols', handler)
        symbols = await self.client.get_symbols()
        h.assert_csv()
        assert symbols == ['MSFT', 'AAPL', 'ZBRA']


# ── get_quote (current) ───────────────────────────────────────────────────────

class TestThetaStockClientGetQuoteCurrent(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaStockClient(url)

    async def test_returns_quote_with_correct_bid_ask(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_QUOTE_ROW])

        self.handler.register('/v3/stock/snapshot/quote', handler)
        result = await self.client.get_quote('ZBRA')
        assert result is not None
        assert isinstance(result, Quote)
        assert result.bid == Decimal('202.57')
        assert result.ask == Decimal('209.09')

    async def test_entity_is_stock_with_correct_symbol(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_QUOTE_ROW])

        self.handler.register('/v3/stock/snapshot/quote', handler)
        result = await self.client.get_quote('ZBRA')
        assert isinstance(result.entity, Stock)
        assert result.entity.type == FinancialEntityType.STOCK
        assert result.symbol == 'ZBRA'

    async def test_correct_params_no_venue(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_QUOTE_ROW])

        h = self.handler.register('/v3/stock/snapshot/quote', handler)
        await self.client.get_quote('ZBRA')
        params = h.get_params()
        assert params['symbol'] == 'ZBRA'
        assert params['format'] == 'csv'
        assert 'venue' not in params

    async def test_venue_passed_through(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_QUOTE_ROW])

        h = self.handler.register('/v3/stock/snapshot/quote', handler)
        await self.client.get_quote('ZBRA', venue='utp_cta')
        assert h.get_params()['venue'] == 'utp_cta'

    async def test_returns_none_for_empty(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [])

        self.handler.register('/v3/stock/snapshot/quote', handler)
        assert await self.client.get_quote('ZBRA') is None

    async def test_returns_none_for_472(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/stock/snapshot/quote', handler)
        assert await self.client.get_quote('ZBRA') is None


# ── get_quote (historical at-time) ───────────────────────────────────────────

class TestThetaStockClientGetQuoteAtTime(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaStockClient(url)

    async def test_uses_at_time_endpoint(self):
        async def handler(request):
            return csv_response(QUOTE_AT_TIME_HEADER, [SAMPLE_QUOTE_ROW])

        h = self.handler.register('/v3/stock/at_time/quote', handler)
        result = await self.client.get_quote('ZBRA', time='20260312 10:00:00')
        assert result is not None
        assert isinstance(result, Quote)
        params = h.get_params()
        assert params['symbol'] == 'ZBRA'
        assert params['start_date'] == '20260312'
        assert params['end_date'] == '20260312'
        assert params['time_of_day'] == '10:00:00.000'

    async def test_returns_none_for_472(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/stock/at_time/quote', handler)
        assert await self.client.get_quote('ZBRA', time='20260312 10:00:00') is None


# ── get_last_trade ────────────────────────────────────────────────────────────

class TestThetaStockClientGetLastTrade(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaStockClient(url)

    async def test_current_uses_snapshot_endpoint(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        h = self.handler.register('/v3/stock/snapshot/trade', handler)
        result = await self.client.get_last_trade('ZBRA')
        assert result is None
        assert len(h.requests) == 1  # confirms snapshot endpoint was hit

    async def test_historical_uses_at_time_endpoint(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/stock/at_time/trade', handler)
        result = await self.client.get_last_trade('ZBRA', time='20260312 10:00:00')
        assert result is None


# ── get_ohlc (current) ────────────────────────────────────────────────────────

class TestThetaStockClientGetOhlc(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaStockClient(url)

    async def test_returns_ohlc_report(self):
        async def handler(request):
            return csv_response(OHLC_SNAPSHOT_HEADER, [SAMPLE_OHLC_ROW])

        h = self.handler.register('/v3/stock/snapshot/ohlc', handler)
        result = await self.client.get_ohlc('ZBRA')
        assert result is not None
        assert isinstance(result, OhlcReport)
        assert result.open == Decimal('207.450')
        assert result.high == Decimal('207.650')
        assert result.close == Decimal('202.720')
        assert result.volume == 755370
        assert result.symbol == 'ZBRA'
        params = h.get_params()
        assert params['symbol'] == 'ZBRA'

    async def test_venue_passed_through(self):
        async def handler(request):
            return csv_response(OHLC_SNAPSHOT_HEADER, [SAMPLE_OHLC_ROW])

        h = self.handler.register('/v3/stock/snapshot/ohlc', handler)
        await self.client.get_ohlc('ZBRA', venue='utp_cta')
        assert h.get_params()['venue'] == 'utp_cta'

    async def test_returns_none_for_empty(self):
        async def handler(request):
            return csv_response(OHLC_SNAPSHOT_HEADER, [])

        self.handler.register('/v3/stock/snapshot/ohlc', handler)
        assert await self.client.get_ohlc('ZBRA') is None

    async def test_returns_none_for_472(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/stock/snapshot/ohlc', handler)
        assert await self.client.get_ohlc('ZBRA') is None


# ── get_quotes (multi-day series) ────────────────────────────────────────────

class TestThetaStockClientGetQuotes(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaStockClient(url)

    async def test_yields_quote_per_day(self):
        async def handler(request):
            return csv_response(QUOTE_AT_TIME_HEADER, [SAMPLE_QUOTE_ROW, SAMPLE_QUOTE_ROW])

        h = self.handler.register('/v3/stock/at_time/quote', handler)
        results = [q async for q in self.client.get_quotes(
            'ZBRA', start_date=20260310, end_date=20260312, time='10:00:00',
        )]
        assert len(results) == 2
        assert all(isinstance(q, Quote) for q in results)
        params = h.get_params()
        assert params['symbol'] == 'ZBRA'
        assert params['time_of_day'] == '10:00:00.000'

    async def test_472_yields_nothing(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/stock/at_time/quote', handler)
        results = [q async for q in self.client.get_quotes(
            'ZBRA', start_date=20260310, end_date=20260312, time='10:00:00',
        )]
        assert results == []


# ── get_historical_ohlc ───────────────────────────────────────────────────────

class TestThetaStockClientGetHistoricalOhlc(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaStockClient(url)

    async def test_yields_ohlc_bars(self):
        data = [
            '2026-03-10T09:30:00,213.78,214.25,213.280,213.960,6074,80,213.75',
            '2026-03-10T09:45:00,213.96,215.00,213.50,214.50,4200,55,214.20',
        ]

        async def handler(request):
            return csv_response(OHLC_HISTORY_HEADER, data)

        h = self.handler.register('/v3/stock/history/ohlc', handler)
        results = [r async for r in self.client.get_historical_ohlc(
            'ZBRA', '15m', start_date=20260310, end_date=20260310,
        )]
        assert len(results) == 2
        assert all(isinstance(r, OhlcReport) for r in results)
        assert results[0].open == Decimal('213.78')
        assert results[0].vwap == Decimal('213.75')
        params = h.get_params()
        assert params['symbol'] == 'ZBRA'
        assert params['interval'] == '15m'

    async def test_472_yields_nothing(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/stock/history/ohlc', handler)
        results = [r async for r in self.client.get_historical_ohlc(
            'ZBRA', '15m', start_date=20260310, end_date=20260310,
        )]
        assert results == []


# ── get_eod ───────────────────────────────────────────────────────────────────

class TestThetaStockClientGetEod(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaStockClient(url)

    async def test_yields_eod_reports(self):
        data = [
            '2026-03-10T17:15:02.686,2026-03-10T16:48:14.047,213.7800,218.0500,208.8200,212.4700,712450,25156,120,73,210.0000,0,40,1,217.5300,0',
            '2026-03-11T17:15:33.973,2026-03-11T17:01:09.611,213.190,215.555,210.040,213.670,583269,23880,40,1,210.050,0,40,1,218.450,0',
        ]

        async def handler(request):
            return csv_response(EOD_HEADER, data)

        h = self.handler.register('/v3/stock/history/eod', handler)
        results = [r async for r in self.client.get_eod(
            'ZBRA', start_date=20260310, end_date=20260311,
        )]
        assert len(results) == 2
        assert all(isinstance(r, EodReport) for r in results)
        assert results[0].close == Decimal('212.4700')
        params = h.get_params()
        assert params['symbol'] == 'ZBRA'

    async def test_472_yields_nothing(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/stock/history/eod', handler)
        results = [r async for r in self.client.get_eod(
            'ZBRA', start_date=20260310, end_date=20260311,
        )]
        assert results == []
