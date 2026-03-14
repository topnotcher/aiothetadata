#!/usr/bin/env python3
"""Run all acceptance tests against a live thetaterm instance.

Acceptance tests verify that the client library correctly communicates with
the ThetaData API. They require a running thetaterm instance and a valid
subscription.

Usage::

    python acceptance.py [url]

``url`` defaults to ``http://127.0.0.1:25503/``.

Each test section prints a pass/fail indicator. Tests gated by subscription
level print a skip notice on HTTP 403 rather than failing.
"""
import asyncio
import sys

from aiothetadata.client import ThetaClient
from tests.acceptance import test_option, test_stock, test_index


async def main() -> None:
    url = sys.argv[1] if len(sys.argv) > 1 else 'http://127.0.0.1:25503/'
    print(f'Connecting to {url}')

    failed = 0
    async with ThetaClient(url) as client:
        for module in (test_option, test_stock, test_index):
            try:
                await module.test(client)
            except Exception as e:
                failed += 1
                print(f'\n  !! Section aborted: {e}')

    print()
    if failed:
        print(f'DONE — {failed} section(s) aborted due to errors.')
        sys.exit(1)
    else:
        print('DONE — all sections passed.')


if __name__ == '__main__':
    asyncio.run(main())
