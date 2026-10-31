"""Tests for ThetaOptionClient."""
import datetime
from decimal import Decimal

import pytest
from aiohttp import web

from aiothetadata.client import ThetaOptionClient
from aiothetadata.constants import OptionRight, QuoteCondition, Exchange, GreeksOrder
from aiothetadata.types import (
    Quote, Trade, OhlcReport, EodReport, FirstOrderGreeks, OpenInterest, EodGreeks, Option, FinancialEntityType,
)
from aiothetadata import datetime as thetadt

from .utils import csv_response, BaseThetaClientTest


# ── Shared headers and sample rows ───────────────────────────────────────────

QUOTE_AT_TIME_HEADER = [
    'symbol', 'expiration', 'strike', 'right', 'timestamp',
    'bid_size', 'bid_exchange', 'bid', 'bid_condition',
    'ask_size', 'ask_exchange', 'ask', 'ask_condition',
]

QUOTE_SNAPSHOT_HEADER = [
    'timestamp', 'symbol', 'expiration', 'strike', 'right',
    'bid_size', 'bid_exchange', 'bid', 'bid_condition',
    'ask_size', 'ask_exchange', 'ask', 'ask_condition',
]

TRADE_SNAPSHOT_HEADER = [
    'timestamp', 'symbol', 'expiration', 'strike', 'right',
    'sequence', 'size', 'exchange', 'condition', 'price',
]

OHLC_SNAPSHOT_HEADER = [
    'timestamp', 'symbol', 'expiration', 'strike', 'right',
    'open', 'high', 'low', 'close', 'volume', 'count',
]

OHLC_HISTORY_HEADER = [
    'symbol', 'expiration', 'strike', 'right', 'timestamp',
    'open', 'high', 'low', 'close', 'volume', 'count', 'vwap',
]

TRADE_AT_TIME_HEADER = [
    'symbol', 'expiration', 'strike', 'right', 'timestamp',
    'sequence', 'ext_condition1', 'ext_condition2', 'ext_condition3',
    'ext_condition4', 'condition', 'size', 'exchange', 'price',
]

GREEKS_HEADER = [
    'symbol', 'expiration', 'strike', 'right', 'timestamp',
    'bid', 'ask', 'delta', 'theta', 'vega', 'rho', 'epsilon',
    'lambda', 'implied_vol', 'iv_error', 'underlying_timestamp', 'underlying_price',
]

EOD_HEADER = [
    'symbol', 'expiration', 'strike', 'right',
    'created', 'last_trade', 'open', 'high', 'low', 'close',
    'volume', 'count',
    'bid_size', 'bid_exchange', 'bid', 'bid_condition',
    'ask_size', 'ask_exchange', 'ask', 'ask_condition',
]

SAMPLE_PUT_ROW = [
    '2026-03-13T00:14:16.73', 'SPXW', '2026-03-20', '5500.000', 'PUT',
    '71', '5', '2.25', '50', '71', '5', '2.50', '50',
]

SAMPLE_CALL_ROW = [
    '2026-03-13T00:14:16.73', 'SPXW', '2026-03-20', '5600.000', 'CALL',
    '10', '1', '1.10', '0', '15', '1', '1.30', '0',
]

SAMPLE_TRADE_ROW = [
    '2026-03-13T00:14:16.73', 'SPXW', '2026-03-20', '5500.000', 'PUT',
    '12345', '5', '57', '50', '82.17',
]

SAMPLE_OHLC_ROW = [
    '2026-03-13T16:24:11.445', 'SPXW', '2026-03-20', '5500.000', 'PUT',
    '62.10', '85.90', '43.35', '83.39', '2434', '616',
]

SAMPLE_GREEKS_ROW = (
    'SPXW,2026-03-13,6850.000,PUT,2026-03-04T09:07:13.376,'
    '84.6000,85.5000,-0.5576,-3.9430,398.3919,-85.1814,83.3173,-44.6958,'
    '0.1741,0.0000,2026-03-03T16:03:53,6816.6300'
)


# ── Discovery ─────────────────────────────────────────────────────────────────

class TestThetaOptionClientDiscovery(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_get_symbols(self):
        roots = ['MSFT', 'AAPL', 'SPX']

        async def handler(request):
            return csv_response(['symbol'], [[r] for r in roots])

        h = self.handler.register('/v3/option/list/symbols', handler)
        symbols = await self.client.get_symbols()
        h.assert_csv()
        assert symbols == roots

    async def test_get_expirations(self):
        async def handler(request):
            return csv_response(['expiration'], [['2026-03-20'], ['2026-03-27']])

        h = self.handler.register('/v3/option/list/expirations', handler)
        expirations = [e async for e in self.client.get_expirations('SPXW')]
        assert expirations == [datetime.date(2026, 3, 20), datetime.date(2026, 3, 27)]
        assert h.get_params()['symbol'] == 'SPXW'

    async def test_get_strikes(self):
        async def handler(request):
            return csv_response(['strike'], [['6600.000'], ['6650.000']])

        h = self.handler.register('/v3/option/list/strikes', handler)
        strikes = [s async for s in self.client.get_strikes('SPXW', 20260320)]
        assert strikes == [Decimal('6600.000'), Decimal('6650.000')]
        assert h.get_params()['symbol'] == 'SPXW'
        assert h.get_params()['expiration'] == '20260320'


# ── get_quote (current) ───────────────────────────────────────────────────────

class TestThetaOptionClientGetQuoteCurrent(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_returns_quote_with_correct_bid_ask(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW])

        self.handler.register('/v3/option/snapshot/quote', handler)

        result = await self.client.get_quote(
            'SPXW', datetime.date(2026, 3, 20), Decimal('5500'), OptionRight.PUT,
        )
        assert result is not None
        assert isinstance(result, Quote)
        assert result.bid == Decimal('2.25')
        assert result.ask == Decimal('2.50')

    async def test_returns_correct_option_entity(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW])

        self.handler.register('/v3/option/snapshot/quote', handler)
        result = await self.client.get_quote(
            'SPXW', datetime.date(2026, 3, 20), Decimal('5500'), OptionRight.PUT,
        )
        assert isinstance(result.entity, Option)
        assert result.symbol == 'SPXW'
        assert result.entity.expiration == datetime.date(2026, 3, 20)
        assert result.entity.strike == Decimal('5500.000')
        assert result.entity.right == OptionRight.PUT

    async def test_sends_correct_params(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW])

        h = self.handler.register('/v3/option/snapshot/quote', handler)
        await self.client.get_quote(
            'SPXW', datetime.date(2026, 3, 20), Decimal('5500'), OptionRight.PUT,
        )
        params = h.get_params()
        assert params['symbol'] == 'SPXW'
        assert params['expiration'] == '20260320'
        assert params['strike'] == '5500.000'
        assert params['right'] == 'PUT'
        assert params['format'] == 'csv'

    async def test_returns_none_for_empty_response(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [])

        self.handler.register('/v3/option/snapshot/quote', handler)
        result = await self.client.get_quote(
            'SPXW', datetime.date(2026, 3, 20), Decimal('5500'), OptionRight.PUT,
        )
        assert result is None

    async def test_returns_none_for_472(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/snapshot/quote', handler)
        result = await self.client.get_quote(
            'SPXW', datetime.date(2026, 3, 20), Decimal('5500'), OptionRight.PUT,
        )
        assert result is None


# ── get_quote (historical at-time) ───────────────────────────────────────────

class TestThetaOptionClientGetQuoteAtTime(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_returns_quote_at_time(self):
        quote_data = [
            'SPXW,2024-03-15,6000.000,CALL,2025-02-19T10:00:00,1,1,325.3600,0,2,3,326.2800,1'
        ]

        async def handler(request):
            return csv_response(QUOTE_AT_TIME_HEADER, quote_data)

        h = self.handler.register('/v3/option/at_time/quote', handler)
        result = await self.client.get_quote(
            'SPXW', 20240315, 6000, OptionRight.PUT,
            time=thetadt.datetime(2024, 3, 1, 10, 0, 0),
        )
        assert isinstance(result, Quote)
        assert result.bid == Decimal('325.3600')
        assert result.ask == Decimal('326.2800')
        params = h.get_params()
        assert params['symbol'] == 'SPXW'
        assert params['start_date'] == '20240301'
        assert params['end_date'] == '20240301'
        assert params['time_of_day'] == '10:00:00.000'

    async def test_returns_none_for_472(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/at_time/quote', handler)
        result = await self.client.get_quote(
            'SPXW', 20250407, 4985, OptionRight.PUT, time='20250404 10:00:00',
        )
        assert result is None


# ── get_chain_quotes (current) ────────────────────────────────────────────────

class TestThetaOptionClientGetChainQuotesCurrent(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_yields_multiple_quotes(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW, SAMPLE_CALL_ROW])

        self.handler.register('/v3/option/snapshot/quote', handler)
        results = [q async for q in self.client.get_chain_quotes('SPXW', datetime.date(2026, 3, 20))]
        assert len(results) == 2
        assert all(isinstance(q, Quote) for q in results)

    async def test_each_quote_has_distinct_strike_and_right(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW, SAMPLE_CALL_ROW])

        self.handler.register('/v3/option/snapshot/quote', handler)
        results = [q async for q in self.client.get_chain_quotes('SPXW', datetime.date(2026, 3, 20))]
        assert results[0].entity.strike == Decimal('5500.000')
        assert results[0].entity.right == OptionRight.PUT
        assert results[1].entity.strike == Decimal('5600.000')
        assert results[1].entity.right == OptionRight.CALL

    async def test_sends_correct_params_no_filters(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW])

        h = self.handler.register('/v3/option/snapshot/quote', handler)
        async for _ in self.client.get_chain_quotes('SPXW', datetime.date(2026, 3, 20)):
            pass
        params = h.get_params()
        assert params['symbol'] == 'SPXW'
        assert params['expiration'] == '20260320'
        assert 'strike' not in params
        assert 'right' not in params

    async def test_optional_strike_right_passed(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [SAMPLE_PUT_ROW])

        h = self.handler.register('/v3/option/snapshot/quote', handler)
        async for _ in self.client.get_chain_quotes(
            'SPXW', datetime.date(2026, 3, 20),
            strike=Decimal('5500'), right=OptionRight.PUT,
        ):
            pass
        params = h.get_params()
        assert params['strike'] == '5500.000'
        assert params['right'] == 'PUT'

    async def test_empty_response_yields_nothing(self):
        async def handler(request):
            return csv_response(QUOTE_SNAPSHOT_HEADER, [])

        self.handler.register('/v3/option/snapshot/quote', handler)
        results = [q async for q in self.client.get_chain_quotes('SPXW', datetime.date(2026, 3, 20))]
        assert results == []

    async def test_472_yields_nothing(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/snapshot/quote', handler)
        results = [q async for q in self.client.get_chain_quotes('SPXW', datetime.date(2026, 3, 20))]
        assert results == []


# ── get_chain_quotes (historical) ────────────────────────────────────────────

class TestThetaOptionClientGetChainQuotesHistorical(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_uses_at_time_endpoint(self):
        async def handler(request):
            return csv_response(QUOTE_AT_TIME_HEADER, [
                'SPXW,2024-03-15,6000.000,PUT,2025-02-19T10:00:00,1,1,325.3600,0,2,3,326.2800,1'
            ])

        h = self.handler.register('/v3/option/at_time/quote', handler)
        results = [q async for q in self.client.get_chain_quotes(
            'SPXW', 20240315, start_date=20250219, end_date=20250219, time='10:00:00',
        )]
        assert len(results) == 1
        assert isinstance(results[0], Quote)
        params = h.get_params()
        assert params['time_of_day'] == '10:00:00.000'


# ── get_quotes (multi-day series) ────────────────────────────────────────────

class TestThetaOptionClientGetQuotes(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_yields_quote_per_day(self):
        data = [
            'SPXW,2024-03-15,6000.000,PUT,2025-02-17T10:00:00,1,1,325.00,0,2,1,326.00,1',
            'SPXW,2024-03-15,6000.000,PUT,2025-02-18T10:00:00,1,1,327.00,0,2,1,328.00,1',
        ]

        async def handler(request):
            return csv_response(QUOTE_AT_TIME_HEADER, data)

        h = self.handler.register('/v3/option/at_time/quote', handler)
        results = [q async for q in self.client.get_quotes(
            'SPXW', 20240315, 6000, OptionRight.PUT,
            start_date=20250217, end_date=20250218, time='10:00:00',
        )]
        assert len(results) == 2
        assert all(isinstance(q, Quote) for q in results)
        params = h.get_params()
        assert params['strike'] == '6000'
        assert params['right'] == 'PUT'
        assert params['time_of_day'] == '10:00:00.000'

    async def test_472_yields_nothing(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/at_time/quote', handler)
        results = [q async for q in self.client.get_quotes(
            'SPXW', 20250407, 4985, OptionRight.PUT,
            start_date=20250404, end_date=20250404, time='10:00:00',
        )]
        assert results == []


# ── get_last_trade (current) ─────────────────────────────────────────────────

class TestThetaOptionClientGetLastTradeCurrent(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_returns_trade(self):
        async def handler(request):
            return csv_response(TRADE_SNAPSHOT_HEADER, [SAMPLE_TRADE_ROW])

        self.handler.register('/v3/option/snapshot/trade', handler)
        result = await self.client.get_last_trade(
            'SPXW', datetime.date(2026, 3, 20), Decimal('5500'), OptionRight.PUT,
        )
        assert result is not None
        assert isinstance(result, Trade)
        assert result.price == Decimal('82.17')
        assert result.entity.symbol == 'SPXW'
        assert result.entity.right == OptionRight.PUT

    async def test_sends_correct_params(self):
        async def handler(request):
            return csv_response(TRADE_SNAPSHOT_HEADER, [SAMPLE_TRADE_ROW])

        h = self.handler.register('/v3/option/snapshot/trade', handler)
        await self.client.get_last_trade(
            'SPXW', datetime.date(2026, 3, 20), Decimal('5500'), OptionRight.PUT,
        )
        params = h.get_params()
        assert params['symbol'] == 'SPXW'
        assert params['expiration'] == '20260320'
        assert params['strike'] == '5500.000'
        assert params['right'] == 'PUT'

    async def test_returns_none_for_empty(self):
        async def handler(request):
            return csv_response(TRADE_SNAPSHOT_HEADER, [])

        self.handler.register('/v3/option/snapshot/trade', handler)
        result = await self.client.get_last_trade(
            'SPXW', datetime.date(2026, 3, 20), Decimal('5500'), OptionRight.PUT,
        )
        assert result is None

    async def test_returns_none_for_472(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/snapshot/trade', handler)
        result = await self.client.get_last_trade(
            'SPXW', datetime.date(2026, 3, 20), Decimal('5500'), OptionRight.PUT,
        )
        assert result is None


# ── get_last_trade (historical) ───────────────────────────────────────────────

class TestThetaOptionClientGetLastTradeAtTime(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_uses_at_time_endpoint(self):
        trade_row = 'SPXW,2024-03-15,6000.000,PUT,2025-02-18T09:59:56.864,758,32,255,255,115,115,2,57,321.8150'

        async def handler(request):
            return csv_response(TRADE_AT_TIME_HEADER, [trade_row])

        h = self.handler.register('/v3/option/at_time/trade', handler)
        result = await self.client.get_last_trade(
            'SPXW', 20240315, 6000, OptionRight.PUT, time='20250218 10:00:00',
        )
        assert result is not None
        assert isinstance(result, Trade)
        params = h.get_params()
        assert params['start_date'] == '20250218'
        assert params['end_date'] == '20250218'

    async def test_returns_none_for_472(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/at_time/trade', handler)
        result = await self.client.get_last_trade(
            'SPXW', 20250407, 4985, OptionRight.PUT, time='20250404 10:00:00',
        )
        assert result is None


# ── get_trades (multi-day series) ────────────────────────────────────────────

class TestThetaOptionClientGetTrades(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_yields_trade_per_day(self):
        data = [
            'SPXW,2024-03-15,6000.000,PUT,2025-02-17T09:59:56.864,758,32,255,255,115,115,2,57,321.8150',
            'SPXW,2024-03-15,6000.000,PUT,2025-02-18T09:59:58.844,55,32,95,255,115,115,20,3,325.8000',
        ]

        async def handler(request):
            return csv_response(TRADE_AT_TIME_HEADER, data)

        self.handler.register('/v3/option/at_time/trade', handler)
        results = [t async for t in self.client.get_trades(
            'SPXW', 20240315, 6000, OptionRight.PUT,
            start_date=20250217, end_date=20250218, time='10:00:00',
        )]
        assert len(results) == 2
        assert all(isinstance(t, Trade) for t in results)


# ── get_greeks ────────────────────────────────────────────────────────────────

class TestThetaOptionClientGetGreeks(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_returns_greeks_for_contract(self):
        async def handler(request):
            return csv_response(GREEKS_HEADER, [SAMPLE_GREEKS_ROW])

        h = self.handler.register('/v3/option/snapshot/greeks/first_order', handler)
        result = await self.client.get_greeks('SPXW', 20260313, 6850, OptionRight.PUT)
        assert isinstance(result, FirstOrderGreeks)
        assert result.delta == Decimal('-0.5576')
        assert result.iv == Decimal('0.1741')
        params = h.get_params()
        assert params['symbol'] == 'SPXW'
        assert params['strike'] == '6850'
        assert params['right'] == 'PUT'

    async def test_returns_none_for_472(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/snapshot/greeks/first_order', handler)
        result = await self.client.get_greeks('SPXW', 20250407, 4985, OptionRight.PUT)
        assert result is None


# ── get_chain_greeks ──────────────────────────────────────────────────────────

class TestThetaOptionClientGetChainGreeks(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_yields_greeks_for_chain(self):
        data = [
            'SPXW,2026-03-13,6850.000,PUT,2026-03-04T09:07:21.724,84.9000,85.8000,-0.5573,-3.9624,398.4344,-85.1445,83.2738,-44.5150,0.1749,0.0000,2026-03-03T16:03:53,6816.6300',
            'SPXW,2026-03-13,6850.000,CALL,2026-03-04T09:07:21.598,81.1000,81.9000,0.4602,-6.1912,400.6075,66.9827,-68.7691,38.4976,0.2349,0.0000,2026-03-03T16:03:53,6816.6300',
        ]

        async def handler(request):
            return csv_response(GREEKS_HEADER, data)

        self.handler.register('/v3/option/snapshot/greeks/first_order', handler)
        results = [g async for g in self.client.get_chain_greeks('SPXW', 20260313)]
        assert len(results) == 2
        assert results[0].entity.right == OptionRight.PUT
        assert results[1].entity.right == OptionRight.CALL

    async def test_no_strike_right_in_params_by_default(self):
        async def handler(request):
            return csv_response(GREEKS_HEADER, [SAMPLE_GREEKS_ROW])

        h = self.handler.register('/v3/option/snapshot/greeks/first_order', handler)
        async for _ in self.client.get_chain_greeks('SPXW', 20260313):
            pass
        params = h.get_params()
        assert 'strike' not in params
        assert 'right' not in params

    async def test_472_yields_nothing(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/snapshot/greeks/first_order', handler)
        results = [g async for g in self.client.get_chain_greeks('SPXW', 20260313)]
        assert results == []


# ── get_ohlc ──────────────────────────────────────────────────────────────────

class TestThetaOptionClientGetOhlc(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_returns_ohlc_report(self):
        async def handler(request):
            return csv_response(OHLC_SNAPSHOT_HEADER, [SAMPLE_OHLC_ROW])

        h = self.handler.register('/v3/option/snapshot/ohlc', handler)
        result = await self.client.get_ohlc(
            'SPXW', datetime.date(2026, 3, 20), Decimal('5500'), OptionRight.PUT,
        )
        assert result is not None
        assert isinstance(result, OhlcReport)
        assert result.open == Decimal('62.10')
        assert result.high == Decimal('85.90')
        assert result.close == Decimal('83.39')
        assert result.volume == 2434
        assert result.entity.symbol == 'SPXW'
        assert result.entity.right == OptionRight.PUT
        params = h.get_params()
        assert params['symbol'] == 'SPXW'
        assert params['expiration'] == '20260320'
        assert params['strike'] == '5500.000'
        assert params['right'] == 'PUT'

    async def test_returns_none_for_empty(self):
        async def handler(request):
            return csv_response(OHLC_SNAPSHOT_HEADER, [])

        self.handler.register('/v3/option/snapshot/ohlc', handler)
        result = await self.client.get_ohlc(
            'SPXW', datetime.date(2026, 3, 20), Decimal('5500'), OptionRight.PUT,
        )
        assert result is None

    async def test_returns_none_for_472(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/snapshot/ohlc', handler)
        result = await self.client.get_ohlc(
            'SPXW', datetime.date(2026, 3, 20), Decimal('5500'), OptionRight.PUT,
        )
        assert result is None


# ── get_historical_ohlc ───────────────────────────────────────────────────────

class TestThetaOptionClientGetHistoricalOhlc(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_yields_ohlc_bars(self):
        data = [
            'SPXW,2026-03-20,5500.000,PUT,2026-03-10T09:30:00,44.74,53.34,40.99,43.43,420,194,49.20',
            'SPXW,2026-03-20,5500.000,PUT,2026-03-10T09:45:00,43.43,45.00,42.00,44.50,100,50,43.80',
        ]

        async def handler(request):
            return csv_response(OHLC_HISTORY_HEADER, data)

        h = self.handler.register('/v3/option/history/ohlc', handler)
        results = [r async for r in self.client.get_historical_ohlc(
            'SPXW', datetime.date(2026, 3, 20), Decimal('5500'), OptionRight.PUT,
            '15m', start_date=20260310, end_date=20260310,
        )]
        assert len(results) == 2
        assert all(isinstance(r, OhlcReport) for r in results)
        assert results[0].open == Decimal('44.74')
        params = h.get_params()
        assert params['interval'] == '15m'
        assert params['symbol'] == 'SPXW'


# ── get_historical_quotes ─────────────────────────────────────────────────────

class TestThetaOptionClientGetHistoricalQuotes(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_yields_quotes_at_interval(self):
        data = [
            'SPXW,2024-03-15,6000.000,CALL,2025-02-19T10:00:00,1,1,325.3600,0,2,3,326.2800,1',
            'SPXW,2024-03-15,6000.000,CALL,2025-02-19T10:15:00,1,1,327.0000,0,2,3,328.0000,1',
        ]

        async def handler(request):
            return csv_response(QUOTE_AT_TIME_HEADER, data)

        h = self.handler.register('/v3/option/history/quote', handler)
        results = [q async for q in self.client.get_historical_quotes(
            'SPXW', 20240315, 6000, OptionRight.CALL,
            '15m', start_date=20250219, end_date=20250219,
        )]
        assert len(results) == 2
        assert all(isinstance(q, Quote) for q in results)
        params = h.get_params()
        assert params['interval'] == '15m'


# ── get_historical_greeks ─────────────────────────────────────────────────────

class TestThetaOptionClientGetHistoricalGreeks(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_yields_greeks_at_interval(self):
        async def handler(request):
            return csv_response(GREEKS_HEADER, [SAMPLE_GREEKS_ROW])

        h = self.handler.register('/v3/option/history/greeks/first_order', handler)
        results = [g async for g in self.client.get_historical_greeks(
            'SPXW', 20260313, '15m',
            strike=6850, right=OptionRight.PUT,
            start_date=20260304, end_date=20260304,
        )]
        assert len(results) == 1
        assert isinstance(results[0], FirstOrderGreeks)
        params = h.get_params()
        assert params['interval'] == '15m'

    async def test_requires_date_or_range(self):
        with pytest.raises(ValueError):
            # no date or start_date+end_date provided
            async for _ in self.client.get_historical_greeks('SPXW', 20260313, '15m'):
                pass

    async def test_472_yields_nothing(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/history/greeks/first_order', handler)
        results = [g async for g in self.client.get_historical_greeks(
            'SPXW', 20260313, '15m', date=20260304,
        )]
        assert results == []


# ── get_eod ───────────────────────────────────────────────────────────────────

class TestThetaOptionClientGetEod(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_yields_eod_reports(self):
        data = [
            ['SPXW', '2026-03-20', '6600.000', 'PUT', '2026-03-10T17:19:12.851', '2026-03-10T16:34:49.64', '51.00', '79.47', '35.26', '42.27', '402', '160', '2', '5', '42.40', '50', '2', '5', '43.20', '50'],
        ]

        async def handler(request):
            return csv_response(EOD_HEADER, data)

        h = self.handler.register('/v3/option/history/eod', handler)
        results = [r async for r in self.client.get_eod(
            'SPXW', datetime.date(2026, 3, 20),
            strike=Decimal('6600'), right=OptionRight.PUT,
            start_date=20260310, end_date=20260310,
        )]
        assert len(results) == 1
        assert isinstance(results[0], EodReport)
        assert results[0].close == Decimal('42.27')
        params = h.get_params()
        assert params['symbol'] == 'SPXW'
        assert params['strike'] == '6600.000'
        assert params['right'] == 'PUT'

    async def test_chain_eod_no_strike_right(self):
        data = [
            ['SPXW', '2026-03-20', '7490.000', 'PUT', '2026-03-10T17:19:12.851', '2026-03-10T00:00:00', '0.00', '0.00', '0.00', '0.00', '0', '0', '2', '5', '681.50', '50', '2', '5', '705.50', '50'],
            ['SPXW', '2026-03-20', '7490.000', 'CALL', '2026-03-10T17:19:12.851', '2026-03-10T00:00:00', '0.00', '0.00', '0.00', '0.00', '0', '0', '0', '5', '0.00', '50', '266', '5', '0.35', '50'],
        ]

        async def handler(request):
            return csv_response(EOD_HEADER, data)

        h = self.handler.register('/v3/option/history/eod', handler)
        results = [r async for r in self.client.get_eod(
            'SPXW', datetime.date(2026, 3, 20),
            start_date=20260310, end_date=20260310,
        )]
        assert len(results) == 2
        params = h.get_params()
        assert 'strike' not in params
        assert 'right' not in params

    async def test_472_yields_nothing(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/history/eod', handler)
        results = [r async for r in self.client.get_eod(
            'SPXW', datetime.date(2026, 3, 20),
            start_date=20260310, end_date=20260310,
        )]
        assert results == []


# ── get_greeks (historical at-time via lookback) ──────────────────────────────

class TestThetaOptionClientGetGreeksAtTime(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_uses_history_endpoint_with_lookback(self):
        """get_greeks(time=T) should hit history/greeks with a lookback window."""
        data = [
            'SPXW,2026-03-20,6600.000,PUT,2026-03-12T09:59:59.039,66.20,66.70,-0.3460,-6.2193,365.8068,-52.2618,50.8053,-34.8830,0.2824,0.0000,2026-03-12T09:59:59.000,6699.00',
            'SPXW,2026-03-20,6600.000,PUT,2026-03-12T09:59:59.044,66.20,66.70,-0.3458,-6.2221,365.7572,-52.2411,50.7846,-34.8689,0.2826,0.0000,2026-03-12T10:00:00.000,6699.17',
        ]

        async def handler(request):
            return csv_response(GREEKS_HEADER, data)

        h = self.handler.register('/v3/option/history/greeks/first_order', handler)

        import datetime
        result = await self.client.get_greeks(
            'SPXW', datetime.date(2026, 3, 20), Decimal('6600'), OptionRight.PUT,
            time=datetime.datetime(2026, 3, 12, 10, 0, 0),
        )

        assert result is not None
        assert isinstance(result, FirstOrderGreeks)
        # Should return last record (nearest to requested time)
        assert result.delta == Decimal('-0.3458')
        params = h.get_params()
        assert params['symbol'] == 'SPXW'
        assert params['strike'] == '6600.000'
        assert params['right'] == 'PUT'
        assert params['interval'] == 'tick'

    async def test_returns_none_when_no_data_in_window(self):
        """get_greeks(time=T) returns None if no greeks records found in lookback."""
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/history/greeks/first_order', handler)

        import datetime
        result = await self.client.get_greeks(
            'SPXW', datetime.date(2026, 3, 20), Decimal('6600'), OptionRight.PUT,
            time=datetime.datetime(2026, 3, 12, 10, 0, 0),
        )
        assert result is None


# ── Open interest ─────────────────────────────────────────────────────────────

OI_HISTORY_HEADER = ['symbol', 'expiration', 'strike', 'right', 'timestamp', 'open_interest']
OI_SNAPSHOT_HEADER = ['timestamp', 'symbol', 'expiration', 'strike', 'right', 'open_interest']


class TestThetaOptionClientGetChainOpenInterest(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_snapshot_yields_open_interest(self):
        data = [
            ['2026-10-30T06:30:02.810', 'SPXW', '2026-11-02', '7480.000', 'PUT', '325'],
            ['2026-10-30T06:30:03.297', 'SPXW', '2026-11-02', '7480.000', 'CALL', '6'],
        ]

        async def handler(request):
            return csv_response(OI_SNAPSHOT_HEADER, data)

        h = self.handler.register('/v3/option/snapshot/open_interest', handler)
        results = [r async for r in self.client.get_chain_open_interest('SPXW', 20261102)]
        h.assert_csv()
        assert len(results) == 2
        assert isinstance(results[0], OpenInterest)
        assert results[0].open_interest == 325
        assert results[0].entity == Option.create(
            symbol='SPXW', expiration=datetime.date(2026, 11, 2), strike=Decimal('7480.000'), right=OptionRight.PUT,
        )
        assert results[0].time == datetime.datetime(2026, 10, 30, 6, 30, 2, 810000, tzinfo=thetadt.MarketTimeZone)
        assert results[1].entity.right == OptionRight.CALL
        params = h.get_params()
        assert params['symbol'] == 'SPXW'
        assert params['expiration'] == '20261102'
        for key in ('strike', 'right', 'start_date', 'end_date', 'date'):
            assert key not in params

    async def test_snapshot_filters_passed(self):
        async def handler(request):
            return csv_response(OI_SNAPSHOT_HEADER, [])

        h = self.handler.register('/v3/option/snapshot/open_interest', handler)
        results = [r async for r in self.client.get_chain_open_interest(
            'SPXW', 20261102, strike=Decimal('7480'), right=OptionRight.PUT, limit=10, max_dte=3,
        )]
        assert results == []
        params = h.get_params()
        assert params['strike'] == '7480.000'
        assert params['right'] == 'PUT'
        assert params['strike_range'] == '10'
        assert params['max_dte'] == '3'

    async def test_history_date_uses_history_endpoint(self):
        data = [
            ['SPXW', '2024-03-15', '5100.000', 'PUT', '2024-03-14T06:30:43.281', '7652'],
            ['SPXW', '2024-03-15', '5100.000', 'CALL', '2024-03-14T06:30:10.467', '4286'],
        ]

        async def handler(request):
            return csv_response(OI_HISTORY_HEADER, data)

        h = self.handler.register('/v3/option/history/open_interest', handler)
        results = [r async for r in self.client.get_chain_open_interest('SPXW', 20240315, date=20240314)]
        assert [r.open_interest for r in results] == [7652, 4286]
        assert results[0].entity.expiration == datetime.date(2024, 3, 15)
        params = h.get_params()
        assert params['start_date'] == '20240314'
        assert params['end_date'] == '20240314'

    async def test_history_all_expirations_parses_row_expiration(self):
        data = [
            ['SPXW', '2024-03-15', '5100.000', 'PUT', '2024-03-14T06:30:43.281', '7652'],
            ['SPXW', '2024-04-19', '4700.000', 'CALL', '2024-03-14T06:30:35.329', '1072'],
        ]

        async def handler(request):
            return csv_response(OI_HISTORY_HEADER, data)

        h = self.handler.register('/v3/option/history/open_interest', handler)
        results = [r async for r in self.client.get_chain_open_interest('SPXW', '*', date=20240314)]
        assert [r.entity.expiration for r in results] == [datetime.date(2024, 3, 15), datetime.date(2024, 4, 19)]
        assert h.get_params()['expiration'] == '*'

    async def test_history_chain_range_split_per_day(self):
        async def handler(request):
            day = request.query['start_date']
            ts = f'{day[:4]}-{day[4:6]}-{day[6:]}T06:30:00.000'
            return csv_response(OI_HISTORY_HEADER, [['SPXW', '2024-03-15', '5100.000', 'PUT', ts, '1']])

        h = self.handler.register('/v3/option/history/open_interest', handler)
        results = [r async for r in self.client.get_chain_open_interest(
            'SPXW', 20240315, start_date=20240311, end_date=20240313,
        )]
        assert len(results) == 3
        assert len(h.requests) == 3
        assert [h.get_params(i)['start_date'] for i in range(3)] == ['20240311', '20240312', '20240313']

    async def test_date_and_range_are_exclusive(self):
        with pytest.raises(ValueError):
            self.client.get_chain_open_interest('SPXW', 20240315, date=20240314, start_date=20240311)

    async def test_range_requires_both_ends(self):
        with pytest.raises(ValueError):
            self.client.get_chain_open_interest('SPXW', 20240315, start_date=20240311)

    async def test_472_yields_nothing(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/history/open_interest', handler)
        results = [r async for r in self.client.get_chain_open_interest('SPXW', 20240315, date=20240309)]
        assert results == []


class TestThetaOptionClientGetOpenInterest(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_returns_snapshot_for_contract(self):
        async def handler(request):
            return csv_response(OI_SNAPSHOT_HEADER, [
                ['2026-10-30T06:30:02.810', 'SPXW', '2026-11-02', '7480.000', 'PUT', '325'],
            ])

        h = self.handler.register('/v3/option/snapshot/open_interest', handler)
        result = await self.client.get_open_interest('SPXW', 20261102, Decimal('7480'), OptionRight.PUT)
        assert result is not None
        assert result.open_interest == 325
        params = h.get_params()
        assert params['strike'] == '7480.000'
        assert params['right'] == 'PUT'

    async def test_returns_history_for_date(self):
        async def handler(request):
            return csv_response(OI_HISTORY_HEADER, [
                ['SPXW', '2024-03-15', '5100.000', 'PUT', '2024-03-14T06:30:43.281', '7652'],
            ])

        h = self.handler.register('/v3/option/history/open_interest', handler)
        result = await self.client.get_open_interest(
            'SPXW', 20240315, Decimal('5100'), OptionRight.PUT, date=datetime.date(2024, 3, 14),
        )
        assert result.open_interest == 7652
        assert h.get_params()['start_date'] == '20240314'

    async def test_returns_none_for_472(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/snapshot/open_interest', handler)
        result = await self.client.get_open_interest('SPXW', 20261102, Decimal('7480'), OptionRight.PUT)
        assert result is None


# ── get_eod_greeks ────────────────────────────────────────────────────────────

EOD_GREEKS_HEADER = ['symbol', 'expiration', 'strike', 'right', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'count', 'bid_size', 'bid_exchange', 'bid', 'bid_condition', 'ask_size', 'ask_exchange', 'ask', 'ask_condition', 'delta', 'theta', 'vega', 'rho', 'epsilon', 'lambda', 'gamma', 'vanna', 'charm', 'vomma', 'veta', 'vera', 'speed', 'zomma', 'color', 'ultima', 'd1', 'd2', 'dual_delta', 'dual_gamma', 'implied_vol', 'iv_error', 'underlying_timestamp', 'underlying_price']

EOD_GREEKS_PUT_ROW = 'SPXW,2024-03-15,5100.000,PUT,2024-03-13T16:26:54.013,3.82,5.50,2.45,3.10,6538,1219,44,5,2.80,50,9,5,3.10,50,-0.1102,-2.5128,72.0324,-3.1377,3.1215,-193.0735,0.0034,-1.5868,19.9375,743.2371,15911.2322,-45.3071,0.0000,0.0115,0.1312,-7797.9926,1.2249,1.2143,0.1122,0.0035,0.1441,0.0002,2024-03-13T16:02:13.000,5165.3100'
EOD_GREEKS_CALL_ROW = 'SPXW,2024-04-19,4700.000,CALL,2024-03-13T15:14:33.421,499.63,499.63,499.63,499.63,3,2,250,5,489.10,50,168,5,508.70,50,0.9427,-1.1531,188.8199,443.0570,-493.6304,9.7606,0.0003,-0.8573,0.7627,2225.3039,2911.2608,-468.0484,0.0000,0.0023,0.0019,-6724.7967,1.5782,1.5137,-0.9299,0.0004,0.2027,0.0000,2024-03-13T16:02:13.000,5165.3100'


class TestThetaOptionClientGetEodGreeks(BaseThetaClientTest):

    async def get_client(self, url):
        return ThetaOptionClient(url)

    async def test_yields_eod_greeks_with_gamma(self):
        async def handler(request):
            return csv_response(EOD_GREEKS_HEADER, [EOD_GREEKS_PUT_ROW])

        h = self.handler.register('/v3/option/history/greeks/eod', handler)
        results = [g async for g in self.client.get_eod_greeks(
            'SPXW', 20240315, strike=Decimal('5100'), right=OptionRight.PUT,
            start_date=20240313, end_date=20240313,
        )]
        h.assert_csv()
        assert len(results) == 1
        g = results[0]
        assert isinstance(g, EodGreeks)
        assert g.gamma == Decimal('0.0034')
        assert g.delta == Decimal('-0.1102')
        assert g.iv == Decimal('0.1441')
        assert g.underlying_price == Decimal('5165.3100')
        assert g.close == Decimal('3.10')
        assert g.volume == 6538
        assert g.entity.expiration == datetime.date(2024, 3, 15)
        assert g.entity.right == OptionRight.PUT
        params = h.get_params()
        assert params['strike'] == '5100.000'
        assert params['right'] == 'PUT'
        assert params['start_date'] == '20240313'

    async def test_all_expirations_split_per_day(self):
        async def handler(request):
            return csv_response(EOD_GREEKS_HEADER, [EOD_GREEKS_PUT_ROW, EOD_GREEKS_CALL_ROW])

        h = self.handler.register('/v3/option/history/greeks/eod', handler)
        results = [g async for g in self.client.get_eod_greeks(
            'SPXW', '*', max_dte=45, start_date=20240313, end_date=20240314,
        )]
        assert len(h.requests) == 2
        assert len(results) == 4
        assert [g.entity.expiration for g in results[:2]] == [datetime.date(2024, 3, 15), datetime.date(2024, 4, 19)]
        params = h.get_params()
        assert params['expiration'] == '*'
        assert params['max_dte'] == '45'
        assert 'strike' not in params

    async def test_472_yields_nothing(self):
        async def handler(request):
            return web.Response(status=472, text='No data')

        self.handler.register('/v3/option/history/greeks/eod', handler)
        results = [g async for g in self.client.get_eod_greeks(
            'SPXW', 20240315, start_date=20240316, end_date=20240316,
        )]
        assert results == []
