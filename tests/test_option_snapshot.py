"""
Tests for ThetaOptionClient.get_quote_snapshot() and
ThetaOptionClient.get_chain_quotes_snapshot().
"""
import datetime
from decimal import Decimal

from aiohttp import web

from aiothetadata.client import ThetaOptionClient
from aiothetadata.types import Quote, Option, FinancialEntityType
from aiothetadata.constants import Exchange, QuoteCondition, OptionRight
from aiothetadata import datetime as thetadt

from .utils import csv_response, BaseThetaClientTest


OPTION_QUOTE_SNAPSHOT_HEADER = [
    'timestamp', 'symbol', 'expiration', 'strike', 'right',
    'bid_size', 'bid_exchange', 'bid', 'bid_condition',
    'ask_size', 'ask_exchange', 'ask', 'ask_condition',
]

SAMPLE_PUT_ROW = [
    '2026-03-13T00:14:16.73', 'SPXW', '2026-03-20', '5500.000', 'PUT',
    '71', '5', '2.25', '50',
    '71', '5', '2.50', '50',
]

SAMPLE_CALL_ROW = [
    '2026-03-13T00:14:16.73', 'SPXW', '2026-03-20', '5600.000', 'CALL',
    '10', '1', '1.10', '0',
    '15', '1', '1.30', '0',
]


class TestThetaOptionClientGetQuoteSnapshot(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_returns_quote_with_correct_bid_ask(self):
        """Should return a Quote with correct bid/ask prices."""
        async def handler(request):
            return csv_response(OPTION_QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW])

        self.handler.register('/v3/option/snapshot/quote', handler)

        result = await self.client.get_quote_snapshot(
            symbol='SPXW',
            expiration=datetime.date(2026, 3, 20),
            strike=Decimal('5500'),
            right=OptionRight.PUT,
        )

        assert result is not None
        assert isinstance(result, Quote)
        assert result.bid == Decimal('2.25')
        assert result.ask == Decimal('2.50')

    async def test_returns_quote_with_correct_option_entity(self):
        """Quote.entity should be an Option with correct symbol, expiration, strike, right."""
        async def handler(request):
            return csv_response(OPTION_QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW])

        self.handler.register('/v3/option/snapshot/quote', handler)

        result = await self.client.get_quote_snapshot(
            symbol='SPXW',
            expiration=datetime.date(2026, 3, 20),
            strike=Decimal('5500'),
            right=OptionRight.PUT,
        )

        assert result is not None
        assert isinstance(result.entity, Option)
        assert result.entity.type == FinancialEntityType.OPTION
        assert result.symbol == 'SPXW'
        assert result.entity.expiration == datetime.date(2026, 3, 20)
        assert result.entity.strike == Decimal('5500.000')
        assert result.entity.right == OptionRight.PUT

    async def test_correct_request_params(self):
        """Should send symbol, expiration (YYYYMMDD), strike, right, and format=csv."""
        async def handler(request):
            return csv_response(OPTION_QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW])

        h = self.handler.register('/v3/option/snapshot/quote', handler)

        await self.client.get_quote_snapshot(
            symbol='SPXW',
            expiration=datetime.date(2026, 3, 20),
            strike=Decimal('5500'),
            right=OptionRight.PUT,
        )

        params = h.get_params()
        assert params['symbol'] == 'SPXW'
        assert params['expiration'] == '20260320'
        assert params['strike'] == '5500.000'
        assert params['right'] == 'PUT'
        assert params['format'] == 'csv'

    async def test_returns_none_for_empty_response(self):
        """Should return None when the server returns no data rows."""
        async def handler(request):
            return csv_response(OPTION_QUOTE_SNAPSHOT_HEADER, [])

        self.handler.register('/v3/option/snapshot/quote', handler)

        result = await self.client.get_quote_snapshot(
            symbol='SPXW',
            expiration=datetime.date(2026, 3, 20),
            strike=Decimal('5500'),
            right=OptionRight.PUT,
        )

        assert result is None

    async def test_returns_none_for_472_response(self):
        """Should return None (not raise) for 472 no-data response."""
        async def handler(request):
            return web.Response(status=472, text='No data found for your request')

        self.handler.register('/v3/option/snapshot/quote', handler)

        result = await self.client.get_quote_snapshot(
            symbol='SPXW',
            expiration=datetime.date(2026, 3, 20),
            strike=Decimal('5500'),
            right=OptionRight.PUT,
        )

        assert result is None


class TestThetaOptionClientGetChainQuotesSnapshot(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_yields_multiple_quotes(self):
        """Should yield a Quote for each row in the response."""
        async def handler(request):
            return csv_response(OPTION_QUOTE_SNAPSHOT_HEADER, [
                SAMPLE_PUT_ROW,
                SAMPLE_CALL_ROW,
            ])

        self.handler.register('/v3/option/snapshot/quote', handler)

        results = []
        async for quote in self.client.get_chain_quotes_snapshot(
            symbol='SPXW',
            expiration=datetime.date(2026, 3, 20),
        ):
            results.append(quote)

        assert len(results) == 2
        assert all(isinstance(q, Quote) for q in results)

    async def test_each_quote_has_distinct_strike_and_right(self):
        """Each yielded Quote should have the strike and right from its row."""
        async def handler(request):
            return csv_response(OPTION_QUOTE_SNAPSHOT_HEADER, [
                SAMPLE_PUT_ROW,
                SAMPLE_CALL_ROW,
            ])

        self.handler.register('/v3/option/snapshot/quote', handler)

        results = []
        async for quote in self.client.get_chain_quotes_snapshot(
            symbol='SPXW',
            expiration=datetime.date(2026, 3, 20),
        ):
            results.append(quote)

        assert results[0].entity.strike == Decimal('5500.000')
        assert results[0].entity.right == OptionRight.PUT
        assert results[1].entity.strike == Decimal('5600.000')
        assert results[1].entity.right == OptionRight.CALL

    async def test_correct_request_params_no_strike_or_right(self):
        """Should send symbol and expiration (YYYYMMDD) but no strike or right."""
        async def handler(request):
            return csv_response(OPTION_QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW])

        h = self.handler.register('/v3/option/snapshot/quote', handler)

        async for _ in self.client.get_chain_quotes_snapshot(
            symbol='SPXW',
            expiration=datetime.date(2026, 3, 20),
        ):
            pass

        params = h.get_params()
        assert params['symbol'] == 'SPXW'
        assert params['expiration'] == '20260320'
        assert params['format'] == 'csv'
        assert 'strike' not in params
        assert 'right' not in params

    async def test_empty_response_yields_nothing(self):
        """Should yield no quotes for an empty response."""
        async def handler(request):
            return csv_response(OPTION_QUOTE_SNAPSHOT_HEADER, [])

        self.handler.register('/v3/option/snapshot/quote', handler)

        results = []
        async for quote in self.client.get_chain_quotes_snapshot(
            symbol='SPXW',
            expiration=datetime.date(2026, 3, 20),
        ):
            results.append(quote)

        assert results == []

    async def test_472_yields_nothing(self):
        """Should yield nothing (not raise) for 472 no-data response."""
        async def handler(request):
            return web.Response(status=472, text='No data found for your request')

        self.handler.register('/v3/option/snapshot/quote', handler)

        results = []
        async for quote in self.client.get_chain_quotes_snapshot(
            symbol='SPXW',
            expiration=datetime.date(2026, 3, 20),
        ):
            results.append(quote)

        assert results == []
