"""Acceptance tests for ThetaOptionClient.

Run via ``acceptance.py``, not pytest directly.
"""
from ._utils import (
    shield, ok, assert_quote, assert_ohlc,
    OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OPTION_STRIKE_CALL,
    HISTORICAL_DATE, HISTORICAL_TIME, HISTORICAL_DATETIME,
)
from aiothetadata.constants import OptionRight, Interval


async def test(client):
    """Run all ThetaOptionClient acceptance tests."""
    print('\n=== ThetaOptionClient ===')
    await _test_discovery(client)
    await _test_current(client)
    await _test_at_time(client)
    await _test_historical_series(client)
    await _test_historical_bars(client)
    await _test_eod(client)


async def _test_discovery(client):
    print('\n-- Discovery --')

    async with shield('get_symbols()'):
        symbols = await client.option.get_symbols()
        assert len(symbols) > 0
        ok('count', len(symbols))
        ok('sample', symbols[:3])

    async with shield('get_expirations(SPXW)'):
        exps = [e async for e in client.option.get_expirations(OPTION_SYMBOL)]
        assert len(exps) > 0
        ok('count', len(exps))
        ok('nearest', exps[0])

    async with shield('get_strikes(SPXW, expiration)'):
        strikes = [s async for s in client.option.get_strikes(OPTION_SYMBOL, OPTION_EXPIRATION)]
        assert len(strikes) > 0
        ok('count', len(strikes))
        ok('sample', strikes[:3])


async def _test_current(client):
    print('\n-- Current (snapshot) --')

    async with shield('get_quote() — single PUT contract'):
        result = await client.option.get_quote(
            OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
        )
        assert result is not None
        assert_quote(result, OPTION_SYMBOL)
        assert result.entity.right == OptionRight.PUT
        ok('bid', result.bid)
        ok('ask', result.ask)
        ok('mid', result.mid)
        ok('strike', result.entity.strike)

    async with shield('get_quote() — single CALL contract'):
        result = await client.option.get_quote(
            OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_CALL, OptionRight.CALL,
        )
        assert result is not None
        assert result.entity.right == OptionRight.CALL
        ok('bid', result.bid)
        ok('ask', result.ask)

    async with shield('get_chain_quotes() — all contracts'):
        count = 0
        sample = []
        async for q in client.option.get_chain_quotes(OPTION_SYMBOL, OPTION_EXPIRATION):
            count += 1
            if len(sample) < 3:
                sample.append(q)
        assert count > 0
        ok('total contracts', count)
        for q in sample:
            ok(f'  ${q.entity.strike} {q.entity.right}', f'bid={q.bid} ask={q.ask}')

    async with shield('get_last_trade() — single contract'):
        result = await client.option.get_last_trade(
            OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
        )
        assert result is not None
        assert result.price > 0
        ok('price', result.price)
        ok('size', result.size)

    async with shield('get_greeks() — single contract'):
        result = await client.option.get_greeks(
            OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
        )
        assert result is not None
        ok('delta', result.delta)
        ok('iv', result.iv)
        ok('bid', result.bid)
        ok('underlying_price', result.underlying_price)

    async with shield('get_chain_greeks() — all contracts'):
        count = 0
        async for g in client.option.get_chain_greeks(OPTION_SYMBOL, OPTION_EXPIRATION):
            count += 1
        assert count > 0
        ok('total contracts', count)

    async with shield('get_ohlc() — current day'):
        result = await client.option.get_ohlc(
            OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
        )
        assert result is not None
        assert_ohlc(result, OPTION_SYMBOL)
        ok('open', result.open)
        ok('high', result.high)
        ok('low', result.low)
        ok('close', result.close)
        ok('volume', result.volume)


async def _test_at_time(client):
    print('\n-- Historical point-in-time --')

    async with shield('get_quote(time=T) — historical quote'):
        result = await client.option.get_quote(
            OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
            time=HISTORICAL_DATETIME,
        )
        assert result is not None
        assert_quote(result, OPTION_SYMBOL)
        ok('bid', result.bid)
        ok('ask', result.ask)
        ok('time', result.time)

    async with shield('get_last_trade(time=T) — historical trade'):
        result = await client.option.get_last_trade(
            OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
            time=HISTORICAL_DATETIME,
        )
        assert result is not None
        ok('price', result.price)
        ok('time', result.time)


async def _test_historical_series(client):
    print('\n-- Historical series (at-time across date range) --')

    async with shield('get_quotes() — series for single contract'):
        results = [q async for q in client.option.get_quotes(
            OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
            time=HISTORICAL_TIME,
        )]
        assert len(results) > 0
        ok('results', len(results))
        ok('sample bid', results[0].bid)

    async with shield('get_trades() — series for single contract'):
        results = [t async for t in client.option.get_trades(
            OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
            time=HISTORICAL_TIME,
        )]
        assert len(results) > 0
        ok('results', len(results))

    async with shield('get_chain_quotes() — historical chain for date range'):
        count = 0
        async for q in client.option.get_chain_quotes(
            OPTION_SYMBOL, OPTION_EXPIRATION,
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
            time=HISTORICAL_TIME,
        ):
            count += 1
        assert count > 0
        ok('total', count)


async def _test_historical_bars(client):
    print('\n-- Historical bars (interval-based) --')

    async with shield('get_historical_quotes() — 15-min quote ticks'):
        results = [q async for q in client.option.get_historical_quotes(
            OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
            '15m',
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
            start_time='09:30:00',
            end_time='16:00:00',
        )]
        assert len(results) > 0
        ok('bars', len(results))
        ok('first bid', results[0].bid)

    async with shield('get_historical_ohlc() — 15-min OHLC bars'):
        results = [r async for r in client.option.get_historical_ohlc(
            OPTION_SYMBOL, OPTION_EXPIRATION, OPTION_STRIKE_PUT, OptionRight.PUT,
            '15m',
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
        )]
        assert len(results) > 0
        ok('bars', len(results))
        ok('first open', results[0].open)

    async with shield('get_historical_greeks() — 15-min greeks'):
        results = [g async for g in client.option.get_historical_greeks(
            OPTION_SYMBOL, OPTION_EXPIRATION, '15m',
            strike=OPTION_STRIKE_PUT, right=OptionRight.PUT,
            date=HISTORICAL_DATE,
        )]
        assert len(results) > 0
        ok('records', len(results))
        ok('first delta', results[0].delta)


async def _test_eod(client):
    print('\n-- EOD --')

    async with shield('get_eod() — single contract'):
        results = [r async for r in client.option.get_eod(
            OPTION_SYMBOL, OPTION_EXPIRATION,
            strike=OPTION_STRIKE_PUT, right=OptionRight.PUT,
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
        )]
        assert len(results) > 0
        r = results[0]
        ok('close', r.close)
        ok('volume', r.volume)

    async with shield('get_eod() — full chain'):
        count = 0
        async for _ in client.option.get_eod(
            OPTION_SYMBOL, OPTION_EXPIRATION,
            start_date=HISTORICAL_DATE,
            end_date=HISTORICAL_DATE,
        ):
            count += 1
        assert count > 0
        ok('contracts', count)
