"""Tests for ThetaIndexClient."""
from decimal import Decimal

import pytest
from aiohttp import web

from aiothetadata.client import ThetaIndexClient
from aiothetadata.constants import Interval
from aiothetadata.types import IndexPriceReport, OhlcReport, FinancialEntityType
from aiothetadata import datetime as thetadt

from .utils import csv_response, BaseThetaClientTest


AT_TIME_HEADER = ['timestamp', 'price']
SNAPSHOT_HEADER = ['timestamp', 'symbol', 'price']
OHLC_SNAPSHOT_HEADER = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'count']
OHLC_HISTORY_HEADER = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'count', 'vwap']


# ── Discovery ─────────────────────────────────────────────────────────────────

class TestThetaIndexClientDiscovery(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaIndexClient(url)

    async def test_get_symbols(self):
        async def handler(request):
            return csv_response(['symbol'], [['SPX'], ['NDX']])

        h = self.handler.register('/v3/index/list/symbols', handler)
        symbols = await self.client.get_symbols()
        h.assert_csv()
        assert symbols == ['SPX', 'NDX']

    async def test_get_dates(self):
        async def handler(request):
            return csv_response(['date'], [['20260310'], ['20260311']])

        h = self.handler.register('/v3/index/list/dates', handler)
        dates = await self.client.get_dates('SPX')
        assert dates == ['20260310', '20260311']
        assert h.get_params()['symbol'] == 'SPX'


# ── get_price (current) ───────────────────────────────────────────────────────

class TestThetaIndexClientGetPriceCurrent(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaIndexClient(url)

    async def test_returns_index_price_report(self):
        async def handler(request):
            return csv_response(SNAPSHOT_HEADER, [['2026-03-12T16:04:31', 'SPX', '6672.62']])

        self.handler.register('/v3/index/snapshot/price', handler)
        result = await self.client.get_price('SPX')
        assert result is not None
        assert isinstance(result, IndexPriceReport)
        assert result.price == Decimal('6672.62')
        assert result.symbol == 'SPX'

    async def test_entity_type_is_index(self):
        async def handler(request):
            return csv_response(SNAPSHOT_HEADER, [['2026-03-12T16:04:31', 'SPX', '6672.62']])

        self.handler.register('/v3/index/snapshot/price', handler)
        result = await self.client.get_price('SPX')
        assert result.type == FinancialEntityType.INDEX

    async def test_correct_request_params(self):
        async def handler(request):
            return csv_response(SNAPSHOT_HEADER, [['2026-03-12T16:04:31', 'SPX', '6672.62']])

        h = self.handler.register('/v3/index/snapshot/price', handler)
        await self.client.get_price('SPX')
        params = h.get_params()
        assert params['symbol'] == 'SPX'
        assert params['format'] == 'csv'

    async def test_returns_none_for_empty_response(self):
        async def handler(request):
            return csv_response(SNAPSHOT_HEADER, [])

        self.handler.register('/v3/index/snapshot/price', handler)
        assert await self.client.get_price('SPX') is None

    async def test_returns_none_for_472_response(self):
        async def handler(request):
            return web.Response(status=472, text='No data found for your request')

        self.handler.register('/v3/index/snapshot/price', handler)
        assert await self.client.get_price('SPX') is None

    async def test_timestamps_parsed_as_eastern(self):
        async def handler(request):
            return csv_response(SNAPSHOT_HEADER, [['2026-03-12T16:04:31', 'SPX', '6672.62']])

        self.handler.register('/v3/index/snapshot/price', handler)
        result = await self.client.get_price('SPX')
        assert result.time.tzinfo == thetadt.MarketTimeZone
        assert result.time.hour == 16
        assert result.time.minute == 4
        assert result.time.second == 31


# ── get_price (historical at-time) ────────────────────────────────────────────

class TestThetaIndexClientGetPriceAtTime(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaIndexClient(url)

    async def test_uses_at_time_endpoint(self):
        async def handler(request):
            return csv_response(AT_TIME_HEADER, [['2026-03-12T16:00:00', '6672.62']])

        h = self.handler.register('/v3/index/at_time/price', handler)
        result = await self.client.get_price('SPX', time='20260312 16:00:00')
        assert result is not None
        assert isinstance(result, IndexPriceReport)
        assert result.price == Decimal('6672.62')
        params = h.get_params()
        assert params['symbol'] == 'SPX'
        assert params['start_date'] == '20260312'
        assert params['end_date'] == '20260312'
        assert params['time_of_day'] == '16:00:00.000'

    async def test_returns_none_for_472(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/index/at_time/price', handler)
        assert await self.client.get_price('SPX', time='20260312 16:00:00') is None

    async def test_filters_zero_price(self):
        """Zero-price rows should be skipped in at-time mode too."""
        async def handler(request):
            return csv_response(AT_TIME_HEADER, [['2026-03-12T00:00:00', '0']])

        self.handler.register('/v3/index/at_time/price', handler)
        assert await self.client.get_price('SPX', time='20260312 00:00:00') is None


# ── get_ohlc (current) ────────────────────────────────────────────────────────

class TestThetaIndexClientGetOhlc(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaIndexClient(url)

    async def test_returns_ohlc_report(self):
        async def handler(request):
            return csv_response(OHLC_SNAPSHOT_HEADER, [
                ['2026-03-13T16:03:24', 'SPX', '6673.49', '6733.30', '6623.92', '6632.19', '0', '0']
            ])

        h = self.handler.register('/v3/index/snapshot/ohlc', handler)
        result = await self.client.get_ohlc('SPX')
        assert result is not None
        assert isinstance(result, OhlcReport)
        assert result.open == Decimal('6673.49')
        assert result.high == Decimal('6733.30')
        assert result.close == Decimal('6632.19')
        assert result.symbol == 'SPX'
        assert h.get_params()['symbol'] == 'SPX'

    async def test_returns_none_for_empty(self):
        async def handler(request):
            return csv_response(OHLC_SNAPSHOT_HEADER, [])

        self.handler.register('/v3/index/snapshot/ohlc', handler)
        assert await self.client.get_ohlc('SPX') is None

    async def test_returns_none_for_472(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/index/snapshot/ohlc', handler)
        assert await self.client.get_ohlc('SPX') is None


# ── get_prices (multi-day at-time series) ─────────────────────────────────────

class TestThetaIndexClientGetPrices(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaIndexClient(url)

    async def test_returns_price_reports(self):
        rows = [
            ['2024-01-02T16:00:00', '4742.83'],
            ['2024-01-03T16:00:00', '4704.81'],
            ['2024-01-04T16:00:00', '4688.68'],
        ]

        async def handler(request):
            return csv_response(AT_TIME_HEADER, rows)

        self.handler.register('/v3/index/at_time/price', handler)
        results = [r async for r in self.client.get_prices(
            'SPX', start_date=20240102, end_date=20240104, time='16:00:00',
        )]
        assert len(results) == 3
        assert all(isinstance(r, IndexPriceReport) for r in results)
        assert results[0].price == Decimal('4742.83')

    async def test_request_params(self):
        async def handler(request):
            return csv_response(AT_TIME_HEADER, [['2024-06-03T16:00:00', '5283.40']])

        h = self.handler.register('/v3/index/at_time/price', handler)
        async for _ in self.client.get_prices(
            'SPX', start_date=20240603, end_date=20240603, time='16:00:00',
        ):
            pass
        params = h.get_params()
        assert params['symbol'] == 'SPX'
        assert params['start_date'] == '20240603'
        assert params['end_date'] == '20240603'
        assert params['time_of_day'] == '16:00:00.000'
        assert params['format'] == 'csv'

    async def test_filters_zero_prices(self):
        rows = [
            ['2024-01-02T00:00:00', '0'],
            ['2024-01-02T16:00:00', '4742.83'],
        ]

        async def handler(request):
            return csv_response(AT_TIME_HEADER, rows)

        self.handler.register('/v3/index/at_time/price', handler)
        results = [r async for r in self.client.get_prices(
            'SPX', start_date=20240102, end_date=20240102, time='16:00:00',
        )]
        assert len(results) == 1
        assert results[0].price == Decimal('4742.83')

    async def test_date_range_pagination(self):
        ranges_seen = []

        async def handler(request):
            ranges_seen.append((request.query['start_date'], request.query['end_date']))
            return csv_response(AT_TIME_HEADER, [['2024-01-15T16:00:00', '5000.00']])

        self.handler.register('/v3/index/at_time/price', handler)
        async for _ in self.client.get_prices(
            'SPX', start_date=20240101, end_date=20240331, time='16:00:00',
        ):
            pass
        assert len(ranges_seen) > 1

    async def test_no_data_472_yields_nothing(self):
        async def handler(request):
            return web.Response(status=472, text='No data found for your request')

        self.handler.register('/v3/index/at_time/price', handler)
        results = [r async for r in self.client.get_prices(
            'SPX', start_date=20250404, end_date=20250404, time='10:00:00',
        )]
        assert results == []


# ── get_historical_prices ─────────────────────────────────────────────────────

class TestThetaIndexClientGetHistoricalPrices(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaIndexClient(url)

    async def test_yields_price_reports(self):
        rows = [
            ['2026-03-12T09:45:00', '6715.07'],
            ['2026-03-12T10:00:00', '6720.33'],
        ]

        async def handler(request):
            return csv_response(AT_TIME_HEADER, rows)

        h = self.handler.register('/v3/index/history/price', handler)
        results = [r async for r in self.client.get_historical_prices(
            'SPX', '15m', start_date=20260312, end_date=20260312,
        )]
        assert len(results) == 2
        assert all(isinstance(r, IndexPriceReport) for r in results)
        params = h.get_params()
        assert params['symbol'] == 'SPX'
        assert params['interval'] == '15m'

    async def test_filters_zero_prices(self):
        rows = [
            ['2026-03-12T00:00:00', '0.0'],
            ['2026-03-12T09:45:00', '6715.07'],
        ]

        async def handler(request):
            return csv_response(AT_TIME_HEADER, rows)

        self.handler.register('/v3/index/history/price', handler)
        results = [r async for r in self.client.get_historical_prices(
            'SPX', '15m', start_date=20260312, end_date=20260312,
        )]
        assert len(results) == 1
        assert results[0].price == Decimal('6715.07')

    async def test_472_yields_nothing(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/index/history/price', handler)
        results = [r async for r in self.client.get_historical_prices(
            'SPX', '15m', start_date=20260312, end_date=20260312,
        )]
        assert results == []


# ── get_historical_ohlc ───────────────────────────────────────────────────────

class TestThetaIndexClientGetHistoricalOhlc(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaIndexClient(url)

    async def test_yields_ohlc_bars(self):
        rows = [
            ['2026-03-10T09:30:00', '6796.56', '6798.96', '6759.74', '6787.79', '0', '0', '0.00'],
            ['2026-03-10T09:45:00', '6787.79', '6810.00', '6785.00', '6805.50', '0', '0', '0.00'],
        ]

        async def handler(request):
            return csv_response(OHLC_HISTORY_HEADER, rows)

        h = self.handler.register('/v3/index/history/ohlc', handler)
        results = [r async for r in self.client.get_historical_ohlc(
            'SPX', '15m', start_date=20260310, end_date=20260310,
        )]
        assert len(results) == 2
        assert all(isinstance(r, OhlcReport) for r in results)
        assert results[0].open == Decimal('6796.56')
        assert results[0].high == Decimal('6798.96')
        params = h.get_params()
        assert params['symbol'] == 'SPX'
        assert params['interval'] == '15m'

    async def test_472_yields_nothing(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/index/history/ohlc', handler)
        results = [r async for r in self.client.get_historical_ohlc(
            'SPX', '15m', start_date=20260310, end_date=20260310,
        )]
        assert results == []
