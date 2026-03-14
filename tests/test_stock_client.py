"""Tests for ThetaStockClient."""
from decimal import Decimal

import pytest
from aiohttp import web

from aiothetadata.client import ThetaStockClient
from aiothetadata.constants import Exchange, QuoteCondition
from aiothetadata.types import Quote, Stock, FinancialEntityType
from aiothetadata import datetime as thetadt

from .utils import csv_response, BaseThetaClientTest


STOCK_QUOTE_SNAPSHOT_HEADER = [
    'timestamp', 'symbol',
    'bid_size', 'bid_exchange', 'bid', 'bid_condition',
    'ask_size', 'ask_exchange', 'ask', 'ask_condition',
]

SAMPLE_ROW = [
    '2026-03-12T19:59:01.112', 'ZBRA',
    '40', '1', '202.57', '0',
    '40', '1', '209.09', '0',
]


class TestThetaStockClientAtTime(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaStockClient(url)

    async def test_get_symbols(self):
        roots = ['MSFT', 'AAPL', 'ZBRA']

        async def get_roots(request):
            return csv_response(['symbol'], [[root] for root in roots])

        handler = self.handler.register('/v3/stock/list/symbols', get_roots)
        symbols = await self.client.get_symbols()
        handler.assert_csv()
        assert symbols == roots

    async def test_get_quote_at_time_no_data_returns_none(self):
        async def no_data(request):
            return web.Response(status=472, text='No data found for your request')

        self.handler.register('/v3/stock/at_time/quote', no_data)

        result = await self.client.get_quote_at_time(
            symbol='SPY', time='20250404 10:00:00',
        )
        assert result is None

    async def test_get_trade_at_time_no_data_returns_none(self):
        async def no_data(request):
            return web.Response(status=472, text='No data found for your request')

        self.handler.register('/v3/stock/at_time/trade', no_data)

        result = await self.client.get_trade_at_time(
            symbol='SPY', time='20250404 10:00:00',
        )
        assert result is None


class TestThetaStockClientGetQuote(BaseThetaClientTest):
    """Tests for ThetaStockClient.get_quote() — current/snapshot data."""

    async def get_client(self, url):
        return ThetaStockClient(url)

    async def test_returns_quote_with_correct_bid_ask(self):
        """Should return a Quote with correct bid/ask prices."""
        async def handler(request):
            return csv_response(STOCK_QUOTE_SNAPSHOT_HEADER, [SAMPLE_ROW])

        self.handler.register('/v3/stock/snapshot/quote', handler)

        result = await self.client.get_quote('ZBRA')

        assert result is not None
        assert isinstance(result, Quote)
        assert result.bid == Decimal('202.57')
        assert result.ask == Decimal('209.09')

    async def test_returns_quote_with_correct_sizes_exchanges_conditions(self):
        """Should return a Quote with correct bid/ask sizes, exchanges, and conditions."""
        async def handler(request):
            return csv_response(STOCK_QUOTE_SNAPSHOT_HEADER, [SAMPLE_ROW])

        self.handler.register('/v3/stock/snapshot/quote', handler)

        result = await self.client.get_quote('ZBRA')

        assert result is not None
        assert result.bid_size == 40
        assert result.ask_size == 40
        assert result.bid_exchange == Exchange(1)
        assert result.ask_exchange == Exchange(1)
        assert result.bid_condition == QuoteCondition.REGULAR
        assert result.ask_condition == QuoteCondition.REGULAR

    async def test_returns_quote_with_correct_timestamp(self):
        """Timestamp should be parsed as US/Eastern market time."""
        async def handler(request):
            return csv_response(STOCK_QUOTE_SNAPSHOT_HEADER, [SAMPLE_ROW])

        self.handler.register('/v3/stock/snapshot/quote', handler)

        result = await self.client.get_quote('ZBRA')

        assert result is not None
        assert result.time.tzinfo == thetadt.MarketTimeZone
        assert result.time.hour == 19
        assert result.time.minute == 59
        assert result.time.second == 1

    async def test_entity_is_stock_with_correct_symbol(self):
        """Quote.entity should be a Stock with the correct symbol."""
        async def handler(request):
            return csv_response(STOCK_QUOTE_SNAPSHOT_HEADER, [SAMPLE_ROW])

        self.handler.register('/v3/stock/snapshot/quote', handler)

        result = await self.client.get_quote('ZBRA')

        assert result is not None
        assert isinstance(result.entity, Stock)
        assert result.entity.type == FinancialEntityType.STOCK
        assert result.symbol == 'ZBRA'

    async def test_correct_request_params_no_venue(self):
        """Should send symbol and format=csv; no venue when not specified."""
        async def handler(request):
            return csv_response(STOCK_QUOTE_SNAPSHOT_HEADER, [SAMPLE_ROW])

        h = self.handler.register('/v3/stock/snapshot/quote', handler)

        await self.client.get_quote('ZBRA')

        params = h.get_params()
        assert params['symbol'] == 'ZBRA'
        assert params['format'] == 'csv'
        assert 'venue' not in params

    async def test_venue_param_passed_through(self):
        """Should include venue in request params when provided."""
        async def handler(request):
            return csv_response(STOCK_QUOTE_SNAPSHOT_HEADER, [SAMPLE_ROW])

        h = self.handler.register('/v3/stock/snapshot/quote', handler)

        await self.client.get_quote('ZBRA', venue='utp_cta')

        params = h.get_params()
        assert params['symbol'] == 'ZBRA'
        assert params['venue'] == 'utp_cta'
        assert params['format'] == 'csv'

    async def test_returns_none_for_empty_response(self):
        """Should return None when the server returns no data rows."""
        async def handler(request):
            return csv_response(STOCK_QUOTE_SNAPSHOT_HEADER, [])

        self.handler.register('/v3/stock/snapshot/quote', handler)

        result = await self.client.get_quote('ZBRA')
        assert result is None

    async def test_returns_none_for_472_response(self):
        """Should return None (not raise) for 472 no-data response."""
        async def handler(request):
            return web.Response(status=472, text='No data found for your request')

        self.handler.register('/v3/stock/snapshot/quote', handler)

        result = await self.client.get_quote('ZBRA')
        assert result is None
