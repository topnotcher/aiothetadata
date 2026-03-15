"""Shared fixtures and helpers for acceptance tests."""
import datetime
from decimal import Decimal

from aiothetadata.types import Quote, OhlcReport


# ── Test fixtures ─────────────────────────────────────────────────────────────
# Update these to reflect current market conditions before running.

OPTION_SYMBOL = 'SPXW'
INDEX_SYMBOL = 'SPX'
STOCK_SYMBOL = 'ZBRA'
STOCK_VENUE = 'utp_cta'     # 15-min delayed (Value subscription)

# Near-term upcoming expiration (update to current nearest weekly)
OPTION_EXPIRATION = datetime.date(2026, 3, 20)

# Near-ATM strikes
OPTION_STRIKE_PUT = Decimal('6600')
OPTION_STRIKE_CALL = Decimal('6700')

# A recent past trading day with known data (not today, not a holiday)
HISTORICAL_DATE = datetime.date(2026, 3, 12)
HISTORICAL_TIME = '10:00:00'
HISTORICAL_DATETIME = datetime.datetime(2026, 3, 12, 10, 0, 0)


# ── Assertion helpers ─────────────────────────────────────────────────────────

def assert_quote(quote: Quote, symbol: str) -> None:
    """Basic sanity checks for a Quote object."""
    assert isinstance(quote, Quote), f'Expected Quote, got {type(quote)}'
    assert quote.symbol == symbol, f'Symbol mismatch: {quote.symbol} != {symbol}'
    assert quote.bid >= 0, f'Negative bid: {quote.bid}'
    assert quote.ask >= quote.bid, f'ask < bid: ask={quote.ask} bid={quote.bid}'


def assert_ohlc(report: OhlcReport, symbol: str) -> None:
    """Basic sanity checks for an OhlcReport."""
    assert isinstance(report, OhlcReport), f'Expected OhlcReport, got {type(report)}'
    assert report.symbol == symbol
    assert report.high >= report.low, f'high < low: {report.high} < {report.low}'
    assert report.open > 0
    assert report.close > 0
