import asyncio
import datetime
import collections

from aiothetadata.client import ThetaClient
from aiothetadata.constants import OptionRight
from aiothetadata.datetime import date_at_time, date, MarketOpen, Minutes


async def main():
    async with ThetaClient() as client:
        quotes = client.option.get_quotes_at_time(
            symbol='SPXW',
            expiration=20250221,
            strike=6000,
            right=OptionRight.PUT,
            start_date=20250217,
            end_date=20250221,
            time='10:00:00',
        )

        print('SPXW 20250221 $6000 PUT quotes 2/17/2025 - 2/21/2025 @ 10:00')
        async for quote in quotes:
            print('\t', str(quote.time), 'bid:', quote.bid, 'ask:', quote.ask)

        data = await client.option.get_quote_at_time(
            symbol='SPX',
            expiration=20250221,
            strike=6000,
            right=OptionRight.PUT,
            time=date_at_time(datetime.date(2025, 2, 20), MarketOpen) + 30*Minutes
        )

        print()
        print('SPX 20250221 $6000 PUT quote 2/20/2025 @ 10:00')
        print('\t', data)

        trades = client.option.get_trades_at_time(
            symbol='SPXW',
            expiration=20250221,
            strike=6000,
            right=OptionRight.PUT,
            start_date=20250217,
            end_date=20250221,
            time='10:00:00',
        )

        print()
        print('SPXW 20250221 $6000 PUT trades 2/17/2025 - 2/21/2025 @ 10:00')
        async for trade in trades:
            print('\t', trade.time, 'price:', trade.price, 'size:', trade.size)

        data = await client.option.get_trade_at_time(
            symbol='SPX',
            expiration=20250221,
            strike=6000,
            right=OptionRight.PUT,
            time=date_at_time(datetime.date(2025, 2, 20), MarketOpen) + 30*Minutes
        )

        print()
        print('SPX 20250221 $6000 PUT trade 2/20/2025 @ 10:00')
        print('\t', data)

        quotes = client.stock.get_quotes_at_time(
            symbol='ZBRA',
            start_date=20250217,
            end_date=20250221,
            time='10:00:00',
        )

        print()
        print('ZBRA quotes 2/17/2025 - 2/21/2025 @ 10:00')
        async for quote in quotes:
            print('\t', quote.time, 'bid:', quote.bid, 'ask:', quote.ask)


        data = await client.stock.get_quote_at_time(
            symbol='ZBRA',
            time=date_at_time(datetime.date(2025, 2, 20), MarketOpen) + 30*Minutes,
        )

        print()
        print('ZBRA quote 2/21/2025 @ 10:00')
        print('\t', data)

        trades = client.stock.get_trades_at_time(
            symbol='ZBRA',
            start_date=20250217,
            end_date=20250221,
            time='10:00:00',
        )

        print()
        print('ZBRA trades 2/17/2025 - 2/21/2025 @ 10:00')
        async for trade in trades:
            print('\t', trade.time, 'price:', trade.price, 'size:', trade.size)


        data = await client.stock.get_trade_at_time(
            symbol='ZBRA',
            time=date_at_time(datetime.date(2025, 2, 20), MarketOpen) + 30*Minutes,
        )

        print()
        print('ZBRA trade 2/20/2025 @ 10:00')
        print('\t', data)

        quotes = client.option.get_all_quotes_at_time(
            symbol='SPXW',
            expiration=20250221,
            start_date=20250220,
            end_date=20250220,
            time='10:00:00',
        )

        print()
        print('SPXW 20250221 quotes 2/20/2025 @ 10:00')
        async for quote in iter_condensed(quotes, 5):
            print('\t', f'${quote.strike}', quote.right, str(quote.time), f'bid: ${quote.bid}', f'ask: ${quote.ask}')

        trades = client.option.get_all_trades_at_time(
            symbol='SPXW',
            expiration=20250221,
            start_date=20250220,
            end_date=20250220,
            time='10:00:00',
        )

        print()
        print('SPXW 20250221 trades 2/20/2025 @ 10:00')
        async for trade in iter_condensed(trades, 5):
            print('\t', f'${trade.strike}', trade.right, trade.time, f'price: ${trade.price}', 'size:', trade.size)

        report = await client.option.get_eod_report(
            symbol='SPXW',
            expiration=20250221,
            strike=6000,
            right=OptionRight.PUT,
            date=datetime.date(2025, 2, 20),
        )

        print()
        print('SPX 20250221 $6000 PUT quote EOD report on 2/20/2025')
        print('\t', report)

        report = await client.stock.get_eod_report('ZBRA', date=20250220)

        print()
        print('ZBRA EOD report on 2/20/2025')
        print('\t', report)


async def iter_condensed(gen, num):
    skipped = 0
    start = 0
    total = 0
    last_n = collections.deque()

    async for thing in gen:
        total += 1
        if start < num:
            yield thing
            start += 1

        else:
            last_n.append(thing)

            if len(last_n) > num:
                last_n.popleft()
                skipped += 1

    print(f'... skipped {skipped} ...')

    for thing in last_n:
        yield thing

    print('total:', total)


if __name__ == '__main__':
    asyncio.run(main())
