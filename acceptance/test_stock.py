"""Acceptance tests for ThetaStockClient.

Run with:
    THETATERM=http://thetaterm:25503 pytest acceptance/test_stock.py -v
"""
import pytest

from aiothetadata.types import Quote, Trade, OhlcReport, EodReport

from ._utils import (
    assert_quote, assert_ohlc,
    STOCK_SYMBOL, STOCK_VENUE,
    HISTORICAL_DATE, HISTORICAL_TIME, HISTORICAL_DATETIME,
)

# Note: tests that call subscription-gated endpoints will be
# automatically skipped by the PermissionDeniedError hook in conftest.py.


# ── Discovery ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_symbols(client):
    """get_symbols() returns a non-empty list of valid symbol strings."""
    symbols = await client.stock.get_symbols()
    assert len(symbols) > 0
    assert all(isinstance(s, str) and s for s in symbols)


# ── Current snapshot ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_quote(client):
    """get_quote() returns a valid delayed quote."""
    result = await client.stock.get_quote(STOCK_SYMBOL, venue=STOCK_VENUE)
    assert result is not None
    assert_quote(result, STOCK_SYMBOL)


@pytest.mark.asyncio
async def test_get_ohlc(client):
    """get_ohlc() returns the current-day OHLC report."""
    result = await client.stock.get_ohlc(STOCK_SYMBOL, venue=STOCK_VENUE)
    assert result is not None
    assert_ohlc(result, STOCK_SYMBOL)


@pytest.mark.asyncio
async def test_get_last_trade(client):
    """get_last_trade() returns the current snapshot trade."""
    result = await client.stock.get_last_trade(STOCK_SYMBOL)
    assert result is not None
    assert isinstance(result, Trade)
    assert result.price > 0


# ── Historical point-in-time ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_quote_at_time(client):
    """get_quote(time=T) returns the historical quote at the given time."""
    result = await client.stock.get_quote(STOCK_SYMBOL, time=HISTORICAL_DATETIME)
    assert result is not None
    assert_quote(result, STOCK_SYMBOL)


@pytest.mark.asyncio
async def test_get_last_trade_at_time(client):
    """get_last_trade(time=T) returns the historical trade at the given time."""
    result = await client.stock.get_last_trade(STOCK_SYMBOL, time=HISTORICAL_DATETIME)
    assert result is not None
    assert isinstance(result, Trade)
    assert result.price > 0


# ── Historical series ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_quotes_series(client):
    """get_quotes() returns one Quote per day across a date range."""
    results = [q async for q in client.stock.get_quotes(
        STOCK_SYMBOL,
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE, time=HISTORICAL_TIME,
    )]
    assert len(results) > 0
    assert all(isinstance(q, Quote) for q in results)


@pytest.mark.asyncio
async def test_get_trades_series(client):
    """get_trades() returns one Trade per day across a date range."""
    results = [t async for t in client.stock.get_trades(
        STOCK_SYMBOL,
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE, time=HISTORICAL_TIME,
    )]
    assert len(results) > 0
    assert all(isinstance(t, Trade) for t in results)


# ── Historical bars ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_historical_ohlc(client):
    """get_historical_ohlc() returns interval OHLC bars."""
    results = [r async for r in client.stock.get_historical_ohlc(
        STOCK_SYMBOL, '15m',
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE,
    )]
    assert len(results) > 0
    assert all(isinstance(r, OhlcReport) for r in results)
    assert all(r.vwap is not None for r in results)


# ── EOD ───────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_eod(client):
    """get_eod() returns EOD reports over a date range."""
    results = [r async for r in client.stock.get_eod(
        STOCK_SYMBOL,
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE,
    )]
    assert len(results) > 0
    assert all(isinstance(r, EodReport) for r in results)
