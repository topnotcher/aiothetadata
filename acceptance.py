#!/usr/bin/env python3
"""Run all acceptance tests against a live thetaterm instance.

Acceptance tests verify that the client library correctly communicates with
the ThetaData API. They require a running thetaterm instance and a valid
subscription.

Usage::

    python acceptance.py [url] [option|stock|index]

``url`` defaults to ``http://127.0.0.1:25503/``.
``option|stock|index`` runs only that client's tests (optional).

Examples::

    python acceptance.py
    python acceptance.py http://thetaterm:25503/
    python acceptance.py http://thetaterm:25503/ index
    python acceptance.py http://thetaterm:25503/ option stock

Each test section prints a pass/fail indicator. Tests gated by subscription
level print a skip notice on HTTP 403 rather than failing.
"""
import asyncio
import sys

from aiothetadata.client import ThetaClient
from tests.acceptance import test_option, test_stock, test_index

MODULES = {
    'option': test_option,
    'stock': test_stock,
    'index': test_index,
}


async def main() -> None:
    args = sys.argv[1:]

    # First arg is URL if it starts with http, otherwise default
    if args and args[0].startswith('http'):
        url = args.pop(0)
    else:
        url = 'http://127.0.0.1:25503/'

    # Remaining args are client names to run; default is all
    if args:
        unknown = set(args) - set(MODULES)
        if unknown:
            print(f'Unknown client(s): {", ".join(sorted(unknown))}')
            print(f'Valid options: {", ".join(MODULES)}')
            sys.exit(1)
        selected = [MODULES[name] for name in args]
    else:
        selected = list(MODULES.values())

    print(f'Connecting to {url}')
    if len(selected) < len(MODULES):
        print(f'Running: {", ".join(args)}')

    failed = 0
    async with ThetaClient(url) as client:
        for module in selected:
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
