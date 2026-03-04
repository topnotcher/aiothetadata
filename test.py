import argparse
import asyncio
import datetime
import collections
import logging
import contextlib

from aiothetadata.client import ThetaClient
from aiothetadata.constants import OptionRight, Interval, TradingHours
from aiothetadata.datetime import date_at_time, date, MarketOpen, Minutes
from aiothetadata.client import ThetaDataHttpError


@contextlib.contextmanager
def shield(title):
    print()
    print(title)

    try:
        yield

    except ThetaDataHttpError as e:
        if e.status == 403:
            print('\t', str(e))

        else:
            raise


async def main():
    parser = argparse.ArgumentParser(description='Test the ThetaData client')
    parser.add_argument('url', nargs='?', default=None, help='The URL of the ThetaData server to connect to')

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    async with ThetaClient(args.url) as client:

        with shield('SPXW 20250221 $6000 PUT quotes 2/17/2025 - 2/21/2025 @ 10:00'):
            quotes = client.option.get_quotes_at_time(
                symbol='SPXW',
                expiration=20250221,
                strike=6000,
                right=OptionRight.PUT,
                start_date=20250217,
                end_date=20250221,
                time='10:00:00',
            )
            async for quote in quotes:
                print('\t', str(quote.time), 'bid:', quote.bid, 'ask:', quote.ask)

        with shield('SPX 20250221 $6000 PUT quote 2/20/2025 @ 10:00'):
            data = await client.option.get_quote_at_time(
                symbol='SPX',
                expiration=20250221,
                strike=6000,
                right=OptionRight.PUT,
                time=date_at_time(datetime.date(2025, 2, 20), MarketOpen) + 30*Minutes
            )

            print('\t', data)

        with shield('SPXW 20250221 $6000 PUT trades 2/17/2025 - 2/21/2025 @ 10:00'):
            trades = client.option.get_trades_at_time(
                symbol='SPXW',
                expiration=20250221,
                strike=6000,
                right=OptionRight.PUT,
                start_date=20250217,
                end_date=20250221,
                time='10:00:00',
            )

            async for trade in trades:
                print('\t', trade.time, 'price:', trade.price, 'size:', trade.size)

        with shield('SPX 20250221 $6000 PUT trade 2/20/2025 @ 10:00'):
            data = await client.option.get_trade_at_time(
                symbol='SPX',
                expiration=20250221,
                strike=6000,
                right=OptionRight.PUT,
                time=date_at_time(datetime.date(2025, 2, 20), MarketOpen) + 30*Minutes
            )

            print('\t', data)

        with shield('ZBRA quotes 2/17/2025 - 2/21/2025 @ 10:00'):
            quotes = client.stock.get_quotes_at_time(
                symbol='ZBRA',
                start_date=20250217,
                end_date=20250221,
                time='10:00:00',
            )

            async for quote in quotes:
                print('\t', quote.time, 'bid:', quote.bid, 'ask:', quote.ask)


        with shield('ZBRA quote 2/20/2025 @ 10:00'):
            data = await client.stock.get_quote_at_time(
                symbol='ZBRA',
                time=date_at_time(datetime.date(2025, 2, 20), MarketOpen) + 30*Minutes,
            )

            print('\t', data)

        with shield('ZBRA trades 2/17/2025 - 2/21/2025 @ 10:00'):
            trades = client.stock.get_trades_at_time(
                symbol='ZBRA',
                start_date=20250217,
                end_date=20250221,
                time='10:00:00',
            )

            async for trade in trades:
                print('\t', trade.time, 'price:', trade.price, 'size:', trade.size)


        with shield('ZBRA trade 2/20/2025 @ 10:00'):
            data = await client.stock.get_trade_at_time(
                symbol='ZBRA',
                time=date_at_time(datetime.date(2025, 2, 20), MarketOpen) + 30*Minutes,
            )

            print('\t', data)


        with shield('SPXW 20250221 quotes 2/20/2025 @ 10:00'):
            quotes = client.option.get_all_quotes_at_time(
                symbol='SPXW',
                expiration=20250221,
                start_date=20250220,
                end_date=20250220,
                time='10:00:00',
            )

            async for quote in iter_condensed(quotes, 5):
                print('\t', f'${quote.entity.strike}', quote.entity.right, str(quote.time), f'bid: ${quote.bid}', f'ask: ${quote.ask}')

        with shield('SPXW 20250221 trades 2/20/2025 @ 10:00'):
            trades = client.option.get_all_trades_at_time(
                symbol='SPXW',
                expiration=20250221,
                start_date=20250220,
                end_date=20250220,
                time='10:00:00',
            )

            async for trade in iter_condensed(trades, 5):
                print('\t', f'${trade.entity.strike}', trade.entity.right, trade.time, f'price: ${trade.price}', 'size:', trade.size)


        with shield('SPX 20250221 $6000 PUT quote EOD report on 2/20/2025'):
            report = await client.option.get_eod_report(
                symbol='SPXW',
                expiration=20250221,
                strike=6000,
                right=OptionRight.PUT,
                date=datetime.date(2025, 2, 20),
            )

            print('\t', report)


        with shield('ZBRA quote EOD report on 2/20/2025'):
            report = await client.stock.get_eod_report('ZBRA', date=20250220)

            print('\t', report)


        with shield('SPXW 20250221 $6000 PUT quotes 2/17/2025 - 2/21/2025 @ 10:00 - 15:00'):
            quotes = client.option.get_historical_quotes(
                symbol='SPXW',
                expiration=20250221,
                strike=6000,
                right=OptionRight.PUT,
                start_date=20250217,
                end_date=20250221,
                start_time='10:00:00',
                end_time='15:00:00',
                interval=900000,  # 15 minutes
            )

            async for quote in iter_condensed(quotes, 5):
                print('\t', str(quote.time), 'bid:', quote.bid, 'ask:', quote.ask)


        with shield('SPX prices 2/17/2025 - 2/21/2025 15 minute RTH'):
            quotes = client.index.get_historical_prices(
                symbol='SPX',
                start_date=20250217,
                end_date=20250221,
                interval=Interval.FIFTEEN_MINUTES,
                hours=TradingHours.REGULAR,
            )

            async for quote in iter_condensed(quotes, 5):
                print('\t', str(quote.time), f'price: ${quote.price}')


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
