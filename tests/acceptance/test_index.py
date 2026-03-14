"""Acceptance tests for ThetaIndexClient.

Run via ``acceptance.py``, not pytest directly.
"""
from ._utils import (
    shield, ok, assert_ohlc,
    INDEX_SYMBOL,
    HISTORICAL_DATE, HISTORICAL_TIME, HISTORICAL_DATETIME,
)


async def test(client):
    """Run all ThetaIndexClient acceptance tests."""
    print('\n=== ThetaIndexClient ===')
    await _test_discovery(client)
    await _test_current(client)
    await _test_at_time(client)
    await _test_historical_series(client)
    await _test_historical_bars(client)


async def _test_discovery(client):
    print('\n-- Discovery --')

    async with shield('get_symbols()'):
        symbols = await client.index.get_symbols()
        assert len(symbols) > 0
        ok('count', len(symbols))
        ok('sample', symbols[:3])

    async with shield(f'get_dates({INDEX_SYMBOL})'):
        dates = await client.index.get_dates(INDEX_SYMBOL)
        assert len(dates) > 0
        ok('count', len(dates))
        ok('recent', dates[-1])


async def _test_current(client):
    print('\n-- Current (snapshot) --')

    async with shield(f'get_price({INDEX_SYMBOL}) — current price'):
        result = await client.index.get_price(INDEX_SYMBOL)
        assert result is not None
        assert result.price > 0
        assert result.symbol == INDEX_SYMBOL
        ok('price', result.price)
        ok('time', result.time)

    async with shield(f'get_ohlc({INDEX_SYMBOL}) — current day OHLC'):
        result = await client.index.get_ohlc(INDEX_SYMBOL)
        assert result is not None
        assert_ohlc(result, INDEX_SYMBOL)
        ok('open', result.open)
        ok('high', result.high)
        ok('low', result.low)
        ok('close', result.close)


async def _test_at_time(client):
    print('\n-- Historical point-in-time --')

    async with shield(f'get_price({INDEX_SYMBOL}, time=T) — historical price'):
        result = await client.index.get_price(INDEX_SYMBOL, time=HISTORICAL_DATETIME)
        assert result is not None
        assert result.price > 0
        ok('price', result.price)
        ok('time', result.time)


async def _test_historical_series(client):
    print('\n-- Historical series --')

    async with shield(f'get_prices({INDEX_SYMBOL}) — at-time series'):
        results = [r async for r in client.index.get_prices(
            INDEX_SYMBOL,
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
            time='16:00:00',
        )]
        assert len(results) > 0
        ok('results', len(results))
        ok('close', results[0].price)

    async with shield(f'get_prices() — filters zero prices'):
        # Verify zero filtering works (midnight has price=0)
        results = [r async for r in client.index.get_prices(
            INDEX_SYMBOL,
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
            time='00:00:01',
        )]
        # Either empty (filtered) or non-zero price
        for r in results:
            assert r.price > 0, f'Zero price not filtered: {r}'
        ok('filtered results', len(results))


async def _test_historical_bars(client):
    print('\n-- Historical bars --')

    async with shield(f'get_historical_prices({INDEX_SYMBOL}) — 15-min price series'):
        results = [r async for r in client.index.get_historical_prices(
            INDEX_SYMBOL, '15m',
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
        )]
        assert len(results) > 0
        ok('bars', len(results))
        ok('first price', results[0].price)

    async with shield(f'get_historical_ohlc({INDEX_SYMBOL}) — 15-min OHLC bars'):
        results = [r async for r in client.index.get_historical_ohlc(
            INDEX_SYMBOL, '15m',
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
        )]
        assert len(results) > 0
        ok('bars', len(results))
        ok('first open', results[0].open)
        ok('first close', results[0].close)
