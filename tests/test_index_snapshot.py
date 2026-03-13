"""
Tests for ThetaIndexClient.get_price_snapshot().
"""
import datetime
from decimal import Decimal

from aiohttp import web

from aiothetadata.client import ThetaIndexClient
from aiothetadata.types import IndexPriceReport, FinancialEntityType
from aiothetadata import datetime as thetadt

from .utils import csv_response, BaseThetaClientTest


INDEX_SNAPSHOT_HEADER = ['timestamp', 'symbol', 'price']


class TestThetaIndexClientGetPriceSnapshot(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaIndexClient(url)

    async def test_returns_index_price_report(self):
        """Should return an IndexPriceReport with correct price and symbol."""
        async def handler(request):
            return csv_response(INDEX_SNAPSHOT_HEADER, [
                ['2026-03-12T16:04:31', 'SPX', '6672.62'],
            ])

        self.handler.register('/v3/index/snapshot/price', handler)

        result = await self.client.get_price_snapshot('SPX')

        assert result is not None
        assert isinstance(result, IndexPriceReport)
        assert result.price == Decimal('6672.62')
        assert result.symbol == 'SPX'

    async def test_entity_type_is_index(self):
        """Returned report entity should be INDEX type."""
        async def handler(request):
            return csv_response(INDEX_SNAPSHOT_HEADER, [
                ['2026-03-12T16:04:31', 'SPX', '6672.62'],
            ])

        self.handler.register('/v3/index/snapshot/price', handler)

        result = await self.client.get_price_snapshot('SPX')

        assert result is not None
        assert result.type == FinancialEntityType.INDEX

    async def test_correct_request_params(self):
        """Should send symbol and format=csv to the API."""
        async def handler(request):
            return csv_response(INDEX_SNAPSHOT_HEADER, [
                ['2026-03-12T16:04:31', 'SPX', '6672.62'],
            ])

        h = self.handler.register('/v3/index/snapshot/price', handler)

        await self.client.get_price_snapshot('SPX')

        params = h.get_params()
        assert params['symbol'] == 'SPX'
        assert params['format'] == 'csv'

    async def test_returns_none_for_empty_response(self):
        """Should return None when the server returns no data rows."""
        async def handler(request):
            return csv_response(INDEX_SNAPSHOT_HEADER, [])

        self.handler.register('/v3/index/snapshot/price', handler)

        result = await self.client.get_price_snapshot('SPX')

        assert result is None

    async def test_returns_none_for_472_response(self):
        """Should return None (not raise) for 472 no-data response."""
        async def handler(request):
            return web.Response(status=472, text='No data found for your request')

        self.handler.register('/v3/index/snapshot/price', handler)

        result = await self.client.get_price_snapshot('SPX')

        assert result is None

    async def test_timestamps_parsed_as_eastern(self):
        """Timestamp should be parsed as US/Eastern market time."""
        async def handler(request):
            return csv_response(INDEX_SNAPSHOT_HEADER, [
                ['2026-03-12T16:04:31', 'SPX', '6672.62'],
            ])

        self.handler.register('/v3/index/snapshot/price', handler)

        result = await self.client.get_price_snapshot('SPX')

        assert result is not None
        assert result.time.tzinfo == thetadt.MarketTimeZone
        assert result.time.hour == 16
        assert result.time.minute == 4
        assert result.time.second == 31
