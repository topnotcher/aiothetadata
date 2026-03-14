"""Tests for ThetaOptionClient."""
import datetime
from decimal import Decimal

import pytest
from aiohttp import web

from aiothetadata.client import ThetaOptionClient
from aiothetadata.constants import OptionRight, QuoteCondition, Exchange
from aiothetadata.types import (
    Quote, Trade, FirstOrderGreeks, Option, FinancialEntityType,
)
from aiothetadata import datetime as thetadt

from .utils import csv_response, BaseThetaClientTest


QUOTE_HEADER = [
    'symbol', 'expiration', 'strike', 'right', 'timestamp',
    'bid_size', 'bid_exchange', 'bid', 'bid_condition',
    'ask_size', 'ask_exchange', 'ask', 'ask_condition',
]

TRADE_HEADER = [
    'symbol', 'expiration', 'strike', 'right', 'timestamp',
    'sequence', 'ext_condition1', 'ext_condition2', 'ext_condition3',
    'ext_condition4', 'condition', 'size', 'exchange', 'price',
]

GREEKS_FIRST_ORDER_HEADER = [
    'symbol', 'expiration', 'strike', 'right', 'timestamp',
    'bid', 'ask', 'delta', 'theta', 'vega', 'rho', 'epsilon',
    'lambda', 'implied_vol', 'iv_error', 'underlying_timestamp', 'underlying_price',
]

QUOTE_SNAPSHOT_HEADER = [
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


class TestThetaOptionClientAtTime(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_get_symbols(self):
        roots = ['MSFT', 'AAPL', 'SPX']

        async def get_roots(request):
            return csv_response(['symbol'], [[root] for root in roots])

        handler = self.handler.register('/v3/option/list/symbols', get_roots)
        symbols = await self.client.get_symbols()
        handler.assert_csv()
        assert symbols == roots

    async def test_null_quote(self):
        quote_data = [
            '"SPXW","2024-03-15",6000.000,PUT,2025-02-19T10:00:00,1,1,325.3600,0,2,1,326.2800,0'
        ]

        async def get_quotes(request):
            return csv_response(QUOTE_HEADER, quote_data)

        handler = self.handler.register('/v3/option/at_time/quote', get_quotes)
        handler.assert_csv()

        quotes = [q async for q in self.client.get_quotes_at_time(
            symbol='SPXW', expiration=20240315, strike=6000,
            right=OptionRight.PUT, start_date=20240220, end_date=20240229,
            time='10:00:00',
        )]
        assert len(quotes) == 1
        assert quotes[0].ask == Decimal('326.2800')

    async def test_get_quote_at_time(self):
        quote_data = [
            'SPXW,2024-03-15,6000.000,CALL,2025-02-19T10:00:00,1,1,325.3600,0,2,3,326.2800,1'
        ]

        async def get_quotes(request):
            return csv_response(QUOTE_HEADER, quote_data)

        handler = self.handler.register('/v3/option/at_time/quote', get_quotes)

        quote = await self.client.get_quote_at_time(
            symbol='SPXW', expiration=20240315, strike=6000,
            right=OptionRight.PUT,
            time=thetadt.datetime(2024, 3, 1, 10, 0, 0),
        )
        handler.assert_csv()

        assert isinstance(quote, Quote)
        assert quote.type == FinancialEntityType.OPTION
        assert quote.bid == Decimal('325.3600')
        assert quote.bid_size == 1
        assert quote.bid_exchange == Exchange.NQEX
        assert quote.bid_condition == QuoteCondition.REGULAR
        assert quote.ask == Decimal('326.2800')
        assert quote.ask_size == 2
        assert quote.ask_exchange == Exchange.NYSE
        assert quote.ask_condition == QuoteCondition.BID_ASK_AUTO_EXEC
        assert quote.time == thetadt.datetime(2025, 2, 19, 10, 0, 0)

        expected = {
            'symbol': 'SPXW', 'strike': '6000', 'expiration': '20240315',
            'right': 'PUT', 'start_date': '20240301', 'end_date': '20240301',
            'time_of_day': '10:00:00.000', 'format': 'csv',
        }
        assert handler.get_params() == expected

    async def test_get_quote_at_time_no_data_returns_none(self):
        async def no_data(request):
            return web.Response(status=472, text='No data found for your request')

        self.handler.register('/v3/option/at_time/quote', no_data)

        result = await self.client.get_quote_at_time(
            symbol='SPXW', expiration=20250407, strike=4985,
            right=OptionRight.PUT, time='20250404 10:00:00',
        )
        assert result is None

    async def test_get_quotes_at_time(self):
        quote_data = [
            'SPXW,2024-03-15,6000.000,CALL,2025-02-19T10:00:00,1,1,325.3600,0,2,3,326.2800,1'
        ]

        async def get_quotes(request):
            return csv_response(QUOTE_HEADER, quote_data)

        handler = self.handler.register('/v3/option/at_time/quote', get_quotes)

        quotes = [q async for q in self.client.get_quotes_at_time(
            symbol='SPXW', expiration=20240315, start_date=20240211,
            end_date=20240221, strike=6000, right=OptionRight.CALL,
            time='10:01:13',
        )]

        assert all(isinstance(q, Quote) for q in quotes)
        assert all(q.type == FinancialEntityType.OPTION for q in quotes)
        expected = {
            'symbol': 'SPXW', 'strike': '6000', 'expiration': '20240315',
            'right': 'CALL', 'start_date': '20240211', 'end_date': '20240221',
            'time_of_day': '10:01:13.000', 'format': 'csv',
        }
        assert handler.get_params() == expected

    async def test_get_quotes_at_time_no_data_yields_nothing(self):
        async def no_data(request):
            return web.Response(status=472, text='No data found for your request')

        self.handler.register('/v3/option/at_time/quote', no_data)

        results = [q async for q in self.client.get_quotes_at_time(
            symbol='SPXW', expiration=20250407, start_date=20250404,
            end_date=20250404, strike=4985, right=OptionRight.PUT,
            time='10:00:00',
        )]
        assert results == []

    async def test_get_all_quotes_at_time(self):
        quote_data = [
            'SPX,2025-02-21,2000.000,CALL,2025-02-20T10:00:00,1,5,5892.30,50,1,5,5909.20,50',
            'SPX,2025-02-21,2000.000,PUT,2025-02-20T10:00:00,0,5,0.00,50,527,5,0.05,50',
        ]

        async def get_quotes(request):
            return csv_response(QUOTE_HEADER, quote_data)

        handler = self.handler.register('/v3/option/at_time/quote', get_quotes)

        quotes = [q async for q in self.client.get_all_quotes_at_time(
            symbol='SPXW', expiration=20240315, start_date=20240211,
            end_date=20240220, time='10:01:13',
        )]

        assert all(isinstance(q, Quote) for q in quotes)
        assert all(q.type == FinancialEntityType.OPTION for q in quotes)
        assert all(q.symbol == 'SPX' for q in quotes)

    async def test_get_trades_at_time(self):
        trade_data = [
            'SPXW,2024-03-15,6000.000,CALL,2025-02-18T09:59:56.864,758,32,255,255,115,115,2,57,321.8150',
            'SPXW,2024-03-15,6000.000,CALL,2025-02-19T09:59:58.844,55,32,95,255,115,115,20,3,325.8000',
        ]

        async def get_trades(request):
            return csv_response(TRADE_HEADER, trade_data)

        handler = self.handler.register('/v3/option/at_time/trade', get_trades)

        trades = [t async for t in self.client.get_trades_at_time(
            symbol='SPXW', expiration=20240315, start_date=20240211,
            end_date=20240221, strike=6000, right=OptionRight.CALL,
            time='10:01:13',
        )]

        assert len(trades) == 2
        assert all(isinstance(t, Trade) for t in trades)
        assert all(t.type == FinancialEntityType.OPTION for t in trades)

    async def test_get_trade_at_time_no_data_returns_none(self):
        async def no_data(request):
            return web.Response(status=472, text='No data found for your request')

        self.handler.register('/v3/option/at_time/trade', no_data)

        result = await self.client.get_trade_at_time(
            symbol='SPXW', expiration=20250407, strike=4985,
            right=OptionRight.PUT, time='20250404 10:00:00',
        )
        assert result is None

    async def test_get_greeks_snapshot_specific_contract(self):
        greeks_data = [
            'SPXW,2026-03-13,6850.000,PUT,2026-03-04T09:07:13.376,84.6000,85.5000,-0.5576,-3.9430,398.3919,-85.1814,83.3173,-44.6958,0.1741,0.0000,2026-03-03T16:03:53,6816.6300',
        ]

        async def get_greeks(request):
            return csv_response(GREEKS_FIRST_ORDER_HEADER, greeks_data)

        handler = self.handler.register('/v3/option/snapshot/greeks/first_order', get_greeks)

        results = [g async for g in self.client.get_greeks_snapshot(
            symbol='SPXW', expiration=20260313, strike=6850, right=OptionRight.PUT,
        )]
        handler.assert_csv()

        assert len(results) == 1
        g = results[0]
        assert isinstance(g, FirstOrderGreeks)
        assert g.type == FinancialEntityType.OPTION
        assert g.symbol == 'SPXW'
        assert g.entity.strike == Decimal('6850.000')
        assert g.entity.right == OptionRight.PUT
        assert g.delta == Decimal('-0.5576')
        assert g.iv == Decimal('0.1741')
        assert g.bid == Decimal('84.6000')
        assert g.ask == Decimal('85.5000')
        assert g.underlying_price == Decimal('6816.6300')

    async def test_get_greeks_snapshot_all_contracts(self):
        greeks_data = [
            'SPXW,2026-03-13,6850.000,PUT,2026-03-04T09:07:21.724,84.9000,85.8000,-0.5573,-3.9624,398.4344,-85.1445,83.2738,-44.5150,0.1749,0.0000,2026-03-03T16:03:53,6816.6300',
            'SPXW,2026-03-13,6850.000,CALL,2026-03-04T09:07:21.598,81.1000,81.9000,0.4602,-6.1912,400.6075,66.9827,-68.7691,38.4976,0.2349,0.0000,2026-03-03T16:03:53,6816.6300',
        ]

        async def get_greeks(request):
            return csv_response(GREEKS_FIRST_ORDER_HEADER, greeks_data)

        handler = self.handler.register('/v3/option/snapshot/greeks/first_order', get_greeks)
        results = [g async for g in self.client.get_greeks_snapshot(symbol='SPXW', expiration=20260313)]

        assert len(results) == 2
        assert results[0].entity.right == OptionRight.PUT
        assert results[1].entity.right == OptionRight.CALL

    async def test_get_greeks_at_strike(self):
        greeks_data = [
            'SPXW,2026-03-13,6850.000,PUT,2026-03-04T09:07:13.376,84.6000,85.5000,-0.5576,-3.9430,398.3919,-85.1814,83.3173,-44.6958,0.1741,0.0000,2026-03-03T16:03:53,6816.6300',
        ]

        async def get_greeks(request):
            return csv_response(GREEKS_FIRST_ORDER_HEADER, greeks_data)

        self.handler.register('/v3/option/snapshot/greeks/first_order', get_greeks)

        g = await self.client.get_greeks_at_strike(
            symbol='SPXW', expiration=20260313, strike=6850, right=OptionRight.PUT,
        )
        assert isinstance(g, FirstOrderGreeks)
        assert g.entity.strike == Decimal('6850.000')
        assert g.delta == Decimal('-0.5576')

    async def test_get_greeks_at_strike_no_data_returns_none(self):
        async def no_data(request):
            return web.Response(status=472, text='No data found for your request')

        self.handler.register('/v3/option/snapshot/greeks/first_order', no_data)

        result = await self.client.get_greeks_at_strike(
            symbol='SPXW', expiration=20250407, strike=4985, right=OptionRight.PUT,
        )
        assert result is None


class TestThetaOptionClientGetQuote(BaseThetaClientTest):
    """Tests for ThetaOptionClient.get_quote() — current/snapshot data."""

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_returns_quote_with_correct_bid_ask(self):
        """Should return a Quote with correct bid/ask prices."""
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW])

        self.handler.register('/v3/option/snapshot/quote', handler)

        result = await self.client.get_quote(
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
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW])

        self.handler.register('/v3/option/snapshot/quote', handler)

        result = await self.client.get_quote(
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
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW])

        h = self.handler.register('/v3/option/snapshot/quote', handler)

        await self.client.get_quote(
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
            return csv_response(QUOTE_SNAPSHOT_HEADER, [])

        self.handler.register('/v3/option/snapshot/quote', handler)

        result = await self.client.get_quote(
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

        result = await self.client.get_quote(
            symbol='SPXW',
            expiration=datetime.date(2026, 3, 20),
            strike=Decimal('5500'),
            right=OptionRight.PUT,
        )
        assert result is None


class TestThetaOptionClientGetChainQuotes(BaseThetaClientTest):
    """Tests for ThetaOptionClient.get_chain_quotes() — current/snapshot data."""

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_yields_multiple_quotes(self):
        """Should yield a Quote for each row in the response."""
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW, SAMPLE_CALL_ROW])

        self.handler.register('/v3/option/snapshot/quote', handler)

        results = [q async for q in self.client.get_chain_quotes(
            symbol='SPXW', expiration=datetime.date(2026, 3, 20),
        )]

        assert len(results) == 2
        assert all(isinstance(q, Quote) for q in results)

    async def test_each_quote_has_distinct_strike_and_right(self):
        """Each yielded Quote should have the strike and right from its row."""
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW, SAMPLE_CALL_ROW])

        self.handler.register('/v3/option/snapshot/quote', handler)

        results = [q async for q in self.client.get_chain_quotes(
            symbol='SPXW', expiration=datetime.date(2026, 3, 20),
        )]

        assert results[0].entity.strike == Decimal('5500.000')
        assert results[0].entity.right == OptionRight.PUT
        assert results[1].entity.strike == Decimal('5600.000')
        assert results[1].entity.right == OptionRight.CALL

    async def test_correct_request_params_no_strike_or_right(self):
        """Should send symbol and expiration (YYYYMMDD) but no strike or right."""
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW])

        h = self.handler.register('/v3/option/snapshot/quote', handler)

        async for _ in self.client.get_chain_quotes(
            symbol='SPXW', expiration=datetime.date(2026, 3, 20),
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
            return csv_response(QUOTE_SNAPSHOT_HEADER, [])

        self.handler.register('/v3/option/snapshot/quote', handler)

        results = [q async for q in self.client.get_chain_quotes(
            symbol='SPXW', expiration=datetime.date(2026, 3, 20),
        )]
        assert results == []

    async def test_472_yields_nothing(self):
        """Should yield nothing (not raise) for 472 no-data response."""
        async def handler(request):
            return web.Response(status=472, text='No data found for your request')

        self.handler.register('/v3/option/snapshot/quote', handler)

        results = [q async for q in self.client.get_chain_quotes(
            symbol='SPXW', expiration=datetime.date(2026, 3, 20),
        )]
        assert results == []
