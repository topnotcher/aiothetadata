"""Acceptance tests for ThetaStockClient.

Run via ``acceptance.py``, not pytest directly.
"""
from ._utils import (
    shield, ok, assert_quote, assert_ohlc,
    STOCK_SYMBOL, STOCK_VENUE,
    HISTORICAL_DATE, HISTORICAL_TIME, HISTORICAL_DATETIME,
)


async def test(client):
    """Run all ThetaStockClient acceptance tests."""
    print('\n=== ThetaStockClient ===')
    await _test_discovery(client)
    await _test_current(client)
    await _test_at_time(client)
    await _test_historical_series(client)
    await _test_historical_bars(client)
    await _test_eod(client)


async def _test_discovery(client):
    print('\n-- Discovery --')

    async with shield('get_symbols()'):
        symbols = await client.stock.get_symbols()
        assert len(symbols) > 0
        ok('count', len(symbols))
        ok('sample', symbols[:3])


async def _test_current(client):
    print('\n-- Current (snapshot) --')

    async with shield(f'get_quote({STOCK_SYMBOL}, venue={STOCK_VENUE}) — delayed quote'):
        result = await client.stock.get_quote(STOCK_SYMBOL, venue=STOCK_VENUE)
        assert result is not None
        assert_quote(result, STOCK_SYMBOL)
        ok('bid', result.bid)
        ok('ask', result.ask)
        ok('mid', result.mid)
        ok('time', result.time)

    async with shield(f'get_ohlc({STOCK_SYMBOL}, venue={STOCK_VENUE}) — current day OHLC'):
        result = await client.stock.get_ohlc(STOCK_SYMBOL, venue=STOCK_VENUE)
        assert result is not None
        assert_ohlc(result, STOCK_SYMBOL)
        ok('open', result.open)
        ok('high', result.high)
        ok('low', result.low)
        ok('close', result.close)
        ok('volume', result.volume)

    async with shield(f'get_last_trade({STOCK_SYMBOL}) — snapshot trade (may require upgrade)'):
        result = await client.stock.get_last_trade(STOCK_SYMBOL)
        # 403 handled by shield; only assert if we got data
        if result is not None:
            ok('price', result.price)
            ok('size', result.size)


async def _test_at_time(client):
    print('\n-- Historical point-in-time --')

    async with shield(f'get_quote({STOCK_SYMBOL}, time=T) — historical quote'):
        result = await client.stock.get_quote(STOCK_SYMBOL, time=HISTORICAL_DATETIME)
        assert result is not None
        assert_quote(result, STOCK_SYMBOL)
        ok('bid', result.bid)
        ok('ask', result.ask)
        ok('time', result.time)

    async with shield(f'get_last_trade({STOCK_SYMBOL}, time=T) — historical trade'):
        result = await client.stock.get_last_trade(STOCK_SYMBOL, time=HISTORICAL_DATETIME)
        assert result is not None
        ok('price', result.price)
        ok('time', result.time)


async def _test_historical_series(client):
    print('\n-- Historical series --')

    async with shield(f'get_quotes({STOCK_SYMBOL}) — at-time series'):
        results = [q async for q in client.stock.get_quotes(
            STOCK_SYMBOL,
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
            time=HISTORICAL_TIME,
        )]
        assert len(results) > 0
        ok('results', len(results))
        ok('sample bid', results[0].bid)

    async with shield(f'get_trades({STOCK_SYMBOL}) — at-time trade series'):
        results = [t async for t in client.stock.get_trades(
            STOCK_SYMBOL,
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
            time=HISTORICAL_TIME,
        )]
        assert len(results) > 0
        ok('results', len(results))


async def _test_historical_bars(client):
    print('\n-- Historical bars --')

    async with shield(f'get_historical_ohlc({STOCK_SYMBOL}) — 15-min bars'):
        results = [r async for r in client.stock.get_historical_ohlc(
            STOCK_SYMBOL, '15m',
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
        )]
        assert len(results) > 0
        ok('bars', len(results))
        ok('first open', results[0].open)
        ok('first vwap', results[0].vwap)


async def _test_eod(client):
    print('\n-- EOD --')

    async with shield(f'get_eod({STOCK_SYMBOL}) — date range'):
        results = [r async for r in client.stock.get_eod(
            STOCK_SYMBOL,
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
        )]
        assert len(results) > 0
        r = results[0]
        ok('close', r.close)
        ok('volume', r.volume)
