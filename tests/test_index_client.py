"""
Tests for ThetaIndexClient.
"""
import datetime
from decimal import Decimal

from aiohttp.test_utils import RawTestServer

from aiothetadata.client import ThetaIndexClient
from aiothetadata.types import IndexPriceReport, FinancialEntityType
from aiothetadata import datetime as thetadt

from .utils import csv_response, BaseThetaClientTest


INDEX_PRICE_HEADER = ['timestamp', 'price']


class TestThetaIndexClientGetPricesAtTime(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaIndexClient(url)

    async def test_returns_price_reports(self):
        """Should yield IndexPriceReport objects for each row."""
        rows = [
            ['2024-01-02T16:00:00', '4742.83'],
            ['2024-01-03T16:00:00', '4704.81'],
            ['2024-01-04T16:00:00', '4688.68'],
        ]

        async def handler(request):
            return csv_response(INDEX_PRICE_HEADER, rows)

        self.handler.register('/v3/index/at_time/price', handler)

        results = []
        async for bar in self.client.get_prices_at_time(
            symbol='SPX',
            start_date=20240102,
            end_date=20240104,
            time='16:00:00',
        ):
            results.append(bar)

        assert len(results) == 3
        assert all(isinstance(r, IndexPriceReport) for r in results)
        assert all(r.type == FinancialEntityType.INDEX for r in results)
        assert all(r.symbol == 'SPX' for r in results)

        assert results[0].price == Decimal('4742.83')
        assert results[1].price == Decimal('4704.81')
        assert results[2].price == Decimal('4688.68')

    async def test_request_params(self):
        """Should send correct query parameters to the API."""
        async def handler(request):
            return csv_response(INDEX_PRICE_HEADER, [
                ['2024-06-03T16:00:00', '5283.40'],
            ])

        h = self.handler.register('/v3/index/at_time/price', handler)

        async for _ in self.client.get_prices_at_time(
            symbol='SPX',
            start_date=20240603,
            end_date=20240603,
            time='16:00:00',
        ):
            pass

        params = h.get_params()
        assert params['symbol'] == 'SPX'
        assert params['start_date'] == '20240603'
        assert params['end_date'] == '20240603'
        assert params['time_of_day'] == '16:00:00.000'
        assert params['format'] == 'csv'

    async def test_filters_zero_prices(self):
        """Should skip rows with price == 0 (off-hours zero quotes)."""
        rows = [
            ['2024-01-02T00:00:00', '0'],
            ['2024-01-02T16:00:00', '4742.83'],
            ['2024-01-02T23:59:00', '0'],
        ]

        async def handler(request):
            return csv_response(INDEX_PRICE_HEADER, rows)

        self.handler.register('/v3/index/at_time/price', handler)

        results = []
        async for bar in self.client.get_prices_at_time(
            symbol='SPX',
            start_date=20240102,
            end_date=20240102,
            time='16:00:00',
        ):
            results.append(bar)

        assert len(results) == 1
        assert results[0].price == Decimal('4742.83')

    async def test_timestamps_parsed_as_eastern(self):
        """Timestamps should be parsed as US/Eastern market time."""
        rows = [['2024-03-15T16:00:00', '5117.09']]

        async def handler(request):
            return csv_response(INDEX_PRICE_HEADER, rows)

        self.handler.register('/v3/index/at_time/price', handler)

        results = [r async for r in self.client.get_prices_at_time(
            symbol='SPX',
            start_date=20240315,
            end_date=20240315,
            time='16:00:00',
        )]

        assert len(results) == 1
        bar = results[0]
        assert bar.time.tzinfo == thetadt.MarketTimeZone
        assert bar.time.hour == 16
        assert bar.time.minute == 0

    async def test_date_range_pagination(self):
        """Should split large date ranges into 30-day chunks."""
        ranges_seen = []

        async def handler(request):
            ranges_seen.append((request.query['start_date'], request.query['end_date']))
            return csv_response(INDEX_PRICE_HEADER, [
                [f'{request.query["start_date"][:4]}-01-15T16:00:00', '5000.00'],
            ])

        self.handler.register('/v3/index/at_time/price', handler)

        results = [r async for r in self.client.get_prices_at_time(
            symbol='SPX',
            start_date=20240101,
            end_date=20240331,
            time='16:00:00',
        )]

        assert len(ranges_seen) > 1
        assert ranges_seen[0][0] == '20240101'
        assert ranges_seen[-1][1] <= '20240331'

    async def test_empty_response(self):
        """Should return no results for an empty response."""
        async def handler(request):
            return csv_response(INDEX_PRICE_HEADER, [])

        self.handler.register('/v3/index/at_time/price', handler)

        results = [r async for r in self.client.get_prices_at_time(
            symbol='SPX',
            start_date=20240101,
            end_date=20240101,
            time='16:00:00',
        )]

        assert results == []

    async def test_accepts_date_objects(self):
        """Should accept datetime.date objects as start/end dates."""
        rows = [['2024-07-04T16:00:00', '5537.02']]

        async def handler(request):
            return csv_response(INDEX_PRICE_HEADER, rows)

        h = self.handler.register('/v3/index/at_time/price', handler)

        results = [r async for r in self.client.get_prices_at_time(
            symbol='SPX',
            start_date=datetime.date(2024, 7, 4),
            end_date=datetime.date(2024, 7, 4),
            time='16:00:00',
        )]

        assert len(results) == 1
        assert results[0].price == Decimal('5537.02')
        params = h.get_params()
        assert params['start_date'] == '20240704'
        assert params['end_date'] == '20240704'
