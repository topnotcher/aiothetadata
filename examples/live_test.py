"""
Live integration test for all new snapshot methods.
Requires thetaterm running at http://127.0.0.1:25503.

Usage:
    python live_test.py [url]
"""
import asyncio
import sys
import datetime
from decimal import Decimal

from aiothetadata.client import ThetaClient
from aiothetadata.constants import OptionRight

URL = sys.argv[1] if len(sys.argv) > 1 else 'http://thetaterm:25503/'

# Test inputs — SPXW 2026-03-20 expiration, strikes near current market
SYMBOL_IDX  = 'SPX'
SYMBOL_OPT  = 'SPXW'
SYMBOL_STK  = 'ZBRA'
EXPIRATION  = datetime.date(2026, 3, 20)
STRIKE_PUT  = Decimal('6600')
STRIKE_CALL = Decimal('6700')
VENUE       = 'utp_cta'   # 15-min delayed (Value subscription)

SEP = '-' * 60


def ok(label, value):
    print(f'  ✓  {label}: {value}')


def fail(label, err):
    print(f'  ✗  {label}: {err}')


async def main():
    print(f'\nConnecting to {URL}\n')

    async with ThetaClient(URL) as client:

        # ------------------------------------------------------------------
        # 1. Index snapshot price
        # ------------------------------------------------------------------
        print(f'{SEP}')
        print(f'1. ThetaIndexClient.get_price("{SYMBOL_IDX}")')
        print(SEP)
        try:
            result = await client.index.get_price(SYMBOL_IDX)
            if result is None:
                fail('result', 'returned None')
            else:
                ok('price', result.price)
                ok('symbol', result.symbol)
                ok('time', result.time)
                ok('entity type', result.type)
        except Exception as e:
            fail('exception', e)

        # ------------------------------------------------------------------
        # 2. Stock snapshot quote (delayed via venue)
        # ------------------------------------------------------------------
        print(f'\n{SEP}')
        print(f'2. ThetaStockClient.get_quote("{SYMBOL_STK}", venue="{VENUE}")')
        print(SEP)
        try:
            result = await client.stock.get_quote(SYMBOL_STK, venue=VENUE)
            if result is None:
                fail('result', 'returned None')
            else:
                ok('bid', result.bid)
                ok('ask', result.ask)
                ok('mid', result.mid)
                ok('bid_size', result.bid_size)
                ok('ask_size', result.ask_size)
                ok('bid_exchange', result.bid_exchange)
                ok('ask_exchange', result.ask_exchange)
                ok('bid_condition', result.bid_condition)
                ok('ask_condition', result.ask_condition)
                ok('symbol', result.symbol)
                ok('time', result.time)
        except Exception as e:
            fail('exception', e)

        # ------------------------------------------------------------------
        # 3. Option snapshot quote — single PUT contract
        # ------------------------------------------------------------------
        print(f'\n{SEP}')
        print(f'3. ThetaOptionClient.get_quote("{SYMBOL_OPT}", {EXPIRATION}, ${STRIKE_PUT}, PUT)')
        print(SEP)
        try:
            result = await client.option.get_quote(
                symbol=SYMBOL_OPT,
                expiration=EXPIRATION,
                strike=STRIKE_PUT,
                right=OptionRight.PUT,
            )
            if result is None:
                fail('result', 'returned None')
            else:
                ok('bid', result.bid)
                ok('ask', result.ask)
                ok('mid', result.mid)
                ok('entity.symbol', result.entity.symbol)
                ok('entity.expiration', result.entity.expiration)
                ok('entity.strike', result.entity.strike)
                ok('entity.right', result.entity.right)
                ok('time', result.time)
        except Exception as e:
            fail('exception', e)

        # ------------------------------------------------------------------
        # 4. Option snapshot quote — single CALL contract
        # ------------------------------------------------------------------
        print(f'\n{SEP}')
        print(f'4. ThetaOptionClient.get_quote("{SYMBOL_OPT}", {EXPIRATION}, ${STRIKE_CALL}, CALL)')
        print(SEP)
        try:
            result = await client.option.get_quote(
                symbol=SYMBOL_OPT,
                expiration=EXPIRATION,
                strike=STRIKE_CALL,
                right=OptionRight.CALL,
            )
            if result is None:
                fail('result', 'returned None')
            else:
                ok('bid', result.bid)
                ok('ask', result.ask)
                ok('mid', result.mid)
                ok('entity.strike', result.entity.strike)
                ok('entity.right', result.entity.right)
                ok('time', result.time)
        except Exception as e:
            fail('exception', e)

        # ------------------------------------------------------------------
        # 5. Option chain snapshot — all contracts for one expiration
        # ------------------------------------------------------------------
        print(f'\n{SEP}')
        print(f'5. ThetaOptionClient.get_chain_quotes("{SYMBOL_OPT}", {EXPIRATION})')
        print(SEP)
        try:
            count = 0
            sample = []
            async for quote in client.option.get_chain_quotes(
                symbol=SYMBOL_OPT,
                expiration=EXPIRATION,
            ):
                count += 1
                if len(sample) < 3:
                    sample.append(quote)

            if count == 0:
                fail('result', 'yielded nothing')
            else:
                ok('total contracts', count)
                for q in sample:
                    ok(f'  ${q.entity.strike} {q.entity.right}',
                       f'bid={q.bid} ask={q.ask} @ {q.time}')
        except Exception as e:
            fail('exception', e)

    print(f'\n{SEP}\nDone.\n')


if __name__ == '__main__':
    asyncio.run(main())
