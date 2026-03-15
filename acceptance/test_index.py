"""Acceptance tests for ThetaIndexClient.

Run with:
    THETATERM=http://thetaterm:25503 pytest acceptance/test_index.py -v
"""
import pytest

from aiothetadata.types import IndexPriceReport, OhlcReport

from ._utils import (
    assert_ohlc,
    INDEX_SYMBOL,
    HISTORICAL_DATE, HISTORICAL_TIME, HISTORICAL_DATETIME,
)


# ── Discovery ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_symbols(client):
    """get_symbols() returns a non-empty list of valid (non-empty) symbol strings."""
    symbols = await client.index.get_symbols()
    assert len(symbols) > 0
    assert all(isinstance(s, str) and s and s.strip() for s in symbols), \
        'Empty symbols should have been filtered by the client'


@pytest.mark.asyncio
async def test_get_dates(client):
    """get_dates() returns a list of available date strings for an index."""
    dates = await client.index.get_dates(INDEX_SYMBOL)
    assert len(dates) > 0
    assert all(isinstance(d, str) for d in dates)


# ── Current snapshot ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_price(client):
    """get_price() returns the current snapshot price."""
    result = await client.index.get_price(INDEX_SYMBOL)
    assert result is not None
    assert isinstance(result, IndexPriceReport)
    assert result.price > 0
    assert result.symbol == INDEX_SYMBOL


@pytest.mark.asyncio
async def test_get_ohlc(client):
    """get_ohlc() returns the current-day OHLC report."""
    result = await client.index.get_ohlc(INDEX_SYMBOL)
    assert result is not None
    assert_ohlc(result, INDEX_SYMBOL)


# ── Historical point-in-time ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_price_at_time(client):
    """get_price(time=T) returns the historical price nearest to T."""
    result = await client.index.get_price(INDEX_SYMBOL, time=HISTORICAL_DATETIME)
    assert result is not None
    assert isinstance(result, IndexPriceReport)
    assert result.price > 0


# ── Historical series ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_prices_series(client):
    """get_prices() returns one price per day across a date range."""
    results = [r async for r in client.index.get_prices(
        INDEX_SYMBOL,
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE, time='16:00:00',
    )]
    assert len(results) > 0
    assert all(isinstance(r, IndexPriceReport) for r in results)
    assert all(r.price > 0 for r in results), 'Zero prices should have been filtered'


@pytest.mark.asyncio
async def test_get_prices_filters_zeros(client):
    """get_prices() filters out zero-price off-hours placeholders."""
    results = [r async for r in client.index.get_prices(
        INDEX_SYMBOL,
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE, time='00:00:01',
    )]
    assert all(r.price > 0 for r in results), f'Got zero price: {results}'


# ── Historical bars ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_historical_prices(client):
    """get_historical_prices() returns interval price records."""
    results = [r async for r in client.index.get_historical_prices(
        INDEX_SYMBOL, '15m',
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE,
    )]
    assert len(results) > 0
    assert all(isinstance(r, IndexPriceReport) for r in results)
    assert all(r.price > 0 for r in results)


@pytest.mark.asyncio
async def test_get_historical_ohlc(client):
    """get_historical_ohlc() returns interval OHLC bars."""
    results = [r async for r in client.index.get_historical_ohlc(
        INDEX_SYMBOL, '15m',
        start_date=HISTORICAL_DATE, end_date=HISTORICAL_DATE,
    )]
    assert len(results) > 0
    assert all(isinstance(r, OhlcReport) for r in results)
