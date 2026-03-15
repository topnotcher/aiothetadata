"""Acceptance tests for ThetaOptionClient.

Run with:
    THETATERM=http://thetaterm:25503 pytest acceptance/test_option.py -v
"""
import datetime
from decimal import Decimal

import pytest

from aiothetadata.constants import OptionRight
from aiothetadata.types import Quote, Trade, OhlcReport, EodReport, FirstOrderGreeks

from ._utils import (
    assert_quote, assert_ohlc,
    OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OPTION_STRIKE_CALL,
    HISTORICAL_DATE, HISTORICAL_TIME, HISTORICAL_DATETIME,
)


# ── Discovery ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_symbols(client):
    """get_symbols() returns a non-empty list of valid symbol strings."""
    symbols = await client.option.get_symbols()
    assert len(symbols) > 0
    assert all(isinstance(s, str) and s for s in symbols)


@pytest.mark.asyncio
async def test_get_expirations(client):
    """get_expirations() yields expiration dates for a given symbol."""
    expirations = [e async for e in client.option.get_expirations(OPTION_SYMBOL)]
    assert len(expirations) > 0
    assert all(isinstance(e, datetime.date) for e in expirations)


@pytest.mark.asyncio
async def test_get_strikes(client):
    """get_strikes() yields Decimal strike prices for a given symbol and expiration."""
    strikes = [s async for s in client.option.get_strikes(OPTION_SYMBOL, OPTION_EXPIRATION)]
    assert len(strikes) > 0
    assert all(isinstance(s, Decimal) for s in strikes)


# ── Current snapshot ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_quote_put(client):
    """get_quote() returns a valid Quote for a PUT contract."""
    result = await client.option.get_quote(
        OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
    )
    assert result is not None
    assert_quote(result, OPTION_SYMBOL)
    assert result.entity.right == OptionRight.PUT


@pytest.mark.asyncio
async def test_get_quote_call(client):
    """get_quote() returns a valid Quote for a CALL contract."""
    result = await client.option.get_quote(
        OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_CALL, OptionRight.CALL,
    )
    assert result is not None
    assert result.entity.right == OptionRight.CALL


@pytest.mark.asyncio
async def test_get_chain_quotes(client):
    """get_chain_quotes() yields quotes for all contracts in an expiration."""
    count = 0
    async for q in client.option.get_chain_quotes(OPTION_SYMBOL, OPTION_EXPIRATION):
        assert isinstance(q, Quote)
        assert_quote(q, OPTION_SYMBOL)
        count += 1
    assert count > 0


@pytest.mark.asyncio
async def test_get_last_trade(client):
    """get_last_trade() returns the most recent trade for a contract."""
    result = await client.option.get_last_trade(
        OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
    )
    assert result is not None
    assert isinstance(result, Trade)
    assert result.price > 0


@pytest.mark.asyncio
async def test_get_greeks(client):
    """get_greeks() returns current first-order greeks for a contract."""
    result = await client.option.get_greeks(
        OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
    )
    assert result is not None
    assert isinstance(result, FirstOrderGreeks)
    assert result.underlying_price > 0


@pytest.mark.asyncio
async def test_get_greeks_at_time(client):
    """get_greeks(time=T) returns historical greeks via lookback window."""
    result = await client.option.get_greeks(
        OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
        time=HISTORICAL_DATETIME,
    )
    assert result is not None
    assert isinstance(result, FirstOrderGreeks)
    assert result.delta < 0  # PUT delta is negative
    assert result.underlying_price > 0


@pytest.mark.asyncio
async def test_get_chain_greeks(client):
    """get_chain_greeks() yields greeks for all contracts in an expiration."""
    count = 0
    async for g in client.option.get_chain_greeks(OPTION_SYMBOL, OPTION_EXPIRATION):
        assert isinstance(g, FirstOrderGreeks)
        count += 1
    assert count > 0


@pytest.mark.asyncio
async def test_get_ohlc(client):
    """get_ohlc() returns the current-day OHLC report for a contract."""
    result = await client.option.get_ohlc(
        OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
    )
    assert result is not None
    assert_ohlc(result, OPTION_SYMBOL)


# ── Historical point-in-time ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_quote_at_time(client):
    """get_quote(time=T) returns the historical quote nearest to T."""
    result = await client.option.get_quote(
        OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
        time=HISTORICAL_DATETIME,
    )
    assert result is not None
    assert_quote(result, OPTION_SYMBOL)


@pytest.mark.asyncio
async def test_get_last_trade_at_time(client):
    """get_last_trade(time=T) returns the historical trade nearest to T."""
    result = await client.option.get_last_trade(
        OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
        time=HISTORICAL_DATETIME,
    )
    assert result is not None
    assert result.price > 0


# ── Historical series ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_quotes_series(client):
    """get_quotes() returns one Quote per day across a date range."""
    results = [q async for q in client.option.get_quotes(
        OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE, time=HISTORICAL_TIME,
    )]
    assert len(results) > 0
    assert all(isinstance(q, Quote) for q in results)


@pytest.mark.asyncio
async def test_get_trades_series(client):
    """get_trades() returns one Trade per day across a date range."""
    results = [t async for t in client.option.get_trades(
        OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE, time=HISTORICAL_TIME,
    )]
    assert len(results) > 0
    assert all(isinstance(t, Trade) for t in results)


@pytest.mark.asyncio
async def test_get_chain_quotes_historical(client):
    """get_chain_quotes() with date/time args returns the full chain at a historical time."""
    count = 0
    async for q in client.option.get_chain_quotes(
        OPTION_SYMBOL, OPTION_EXPIRATION,
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE, time=HISTORICAL_TIME,
    ):
        assert isinstance(q, Quote)
        count += 1
    assert count > 0


# ── Historical bars ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_historical_quotes(client):
    """get_historical_quotes() returns interval-sampled quote ticks."""
    results = [q async for q in client.option.get_historical_quotes(
        OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
        '15m',
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE,
        start_time='09:30:00', end_time='16:00:00',
    )]
    assert len(results) > 0
    assert all(isinstance(q, Quote) for q in results)


@pytest.mark.asyncio
async def test_get_historical_ohlc(client):
    """get_historical_ohlc() returns interval OHLC bars."""
    results = [r async for r in client.option.get_historical_ohlc(
        OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
        '15m',
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE,
    )]
    assert len(results) > 0
    assert all(isinstance(r, OhlcReport) for r in results)


@pytest.mark.asyncio
async def test_get_historical_greeks(client):
    """get_historical_greeks() returns interval greeks records."""
    results = [g async for g in client.option.get_historical_greeks(
        OPTION_SYMBOL, OPTION_EXPIRATION, '15m',
        strike=OPTION_STRIKE_PUT, right=OptionRight.PUT,
        date=HISTORICAL_DATE,
    )]
    assert len(results) > 0
    assert all(isinstance(g, FirstOrderGreeks) for g in results)


# ── EOD ───────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_eod_single_contract(client):
    """get_eod() returns EOD reports for a single contract."""
    results = [r async for r in client.option.get_eod(
        OPTION_SYMBOL, OPTION_EXPIRATION,
        strike=OPTION_STRIKE_PUT, right=OptionRight.PUT,
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE,
    )]
    assert len(results) > 0
    assert all(isinstance(r, EodReport) for r in results)


@pytest.mark.asyncio
async def test_get_eod_full_chain(client):
    """get_eod() without strike/right returns EOD for the full chain."""
    count = 0
    async for r in client.option.get_eod(
        OPTION_SYMBOL, OPTION_EXPIRATION,
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE,
    ):
        assert isinstance(r, EodReport)
        count += 1
    assert count > 0
