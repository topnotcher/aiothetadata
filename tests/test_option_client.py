"""Tests for ThetaOptionClient."""
import datetime
from decimal import Decimal

import pytest
from aiohttp import web

from aiothetadata.client import ThetaOptionClient
from aiothetadata.constants import OptionRight, QuoteCondition, Exchange, GreeksOrder
from aiothetadata.types import (
    Quote, Trade, OhlcReport, EodReport, FirstOrderGreeks, Option, FinancialEntityType,
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
