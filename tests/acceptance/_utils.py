"""Shared utilities for acceptance tests.

Acceptance tests run against a live thetaterm instance and verify that the
client library correctly communicates with the API. They are not part of the
automated test suite (pytest) and must be run manually via ``acceptance.py``.

Update the fixture constants below to reflect current market conditions before
running. The historical fixtures should be recent trading days with known data.
"""
import contextlib
import datetime
from decimal import Decimal

from aiothetadata.client import ThetaDataHttpError


# ── Fixtures ─────────────────────────────────────────────────────────────────
# Update before running. Historical date should be a recent past trading day.

OPTION_SYMBOL = 'SPXW'
INDEX_SYMBOL = 'SPX'
STOCK_SYMBOL = 'ZBRA'
STOCK_VENUE = 'utp_cta'     # 15-min delayed (Value subscription)

# Near-term upcoming expiration (update to current nearest weekly)
OPTION_EXPIRATION = datetime.date(2026, 3, 20)

# ATM or near-ATM strike for SPX ~6650 area
OPTION_STRIKE_PUT = Decimal('6600')
OPTION_STRIKE_CALL = Decimal('6700')

# A recent past trading day with known data (not today, not a holiday)
HISTORICAL_DATE = datetime.date(2026, 3, 12)
HISTORICAL_TIME = '10:00:00'
HISTORICAL_DATETIME = datetime.datetime(2026, 3, 12, 10, 0, 0)


# ── shield() ─────────────────────────────────────────────────────────────────

@contextlib.asynccontextmanager
async def shield(title):
    """Run a named acceptance test block, printing pass/fail.

    On success, prints ``  ✓ passed``. On ThetaDataHttpError with status 403,
    prints a skip notice (subscription limitation). Any other exception is
    printed and re-raised.
    """
    print(f'\n  [{title}]')
    try:
        yield
        print('    ✓ passed')
    except ThetaDataHttpError as e:
        if e.status == 403:
            print(f'    ⚠ skipped (403 — subscription limit): {e}')
        else:
            print(f'    ✗ FAILED (HTTP {e.status}): {e}')
            raise
    except AssertionError as e:
        print(f'    ✗ FAILED (assertion): {e}')
        raise
    except Exception as e:
        print(f'    ✗ FAILED ({type(e).__name__}): {e}')
        raise


def ok(label, value):
    """Print a labelled value for human review."""
    print(f'    · {label}: {value}')


def assert_quote(quote, symbol):
    """Basic sanity checks for a Quote object."""
    from aiothetadata.types import Quote
    assert isinstance(quote, Quote), f'Expected Quote, got {type(quote)}'
    assert quote.symbol == symbol, f'Symbol mismatch: {quote.symbol} != {symbol}'
    assert quote.bid >= 0, f'Negative bid: {quote.bid}'
    assert quote.ask >= quote.bid, f'ask < bid: ask={quote.ask} bid={quote.bid}'


def assert_ohlc(report, symbol):
    """Basic sanity checks for an OhlcReport."""
    from aiothetadata.types import OhlcReport
    assert isinstance(report, OhlcReport), f'Expected OhlcReport, got {type(report)}'
    assert report.symbol == symbol
    assert report.high >= report.low, f'high < low: {report.high} < {report.low}'
    assert report.open > 0
    assert report.close > 0
