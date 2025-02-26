import enum

from ._trade_conditions import _TRADE_CONDITIONS


__all__ = (
    'OptionRight',
    'QuoteCondition',
    'Exchange',
    'TradeCondition',
    'Interval',
)


class OptionRight(enum.StrEnum):
    CALL = 'C'
    PUT = 'P'


class Interval(enum.Enum):
    TICK = 0
    MINUTE = 60000
    SECOND = 1000
    FIVE_MINUTES = 300000
    FIFTEEN_MINUTES = 900000


class QuoteCondition(enum.IntEnum):
    """
    Quote condition

    https://http-docs.thetadata.us/Articles/Data-And-Requests/Values/Quote-Conditions
    """

    REGULAR = 0
    BID_ASK_AUTO_EXEC = 1
    ROTATION = 2
    SPECIALIST_ASK = 3
    SPECIALIST_BID = 4
    LOCKED = 5
    FAST_MARKET = 6
    SPECIALIST_BID_ASK = 7
    ONE_SIDE = 8
    OPENING_QUOTE = 9
    CLOSING_QUOTE = 10
    MARKET_MAKER_CLOSED = 11
    DEPTH_ON_ASK = 12
    DEPTH_ON_BID = 13
    DEPTH_ON_BID_ASK = 14
    TIER_3 = 15
    CROSSED = 16
    HALTED = 17
    OPERATIONAL_HALT = 18
    NEWS = 19
    NEWS_PENDING = 20
    NON_FIRM = 21
    DUE_TO_RELATED = 22
    RESUME = 23
    NO_MARKET_MAKERS = 24
    ORDER_IMBALANCE = 25
    ORDER_INFLUX = 26
    INDICATED = 27
    PRE_OPEN = 28
    IN_VIEW_OF_COMMON = 29
    RELATED_NEWS_PENDING = 30
    RELATED_NEWS_OUT = 31
    ADDITIONAL_INFO = 32
    RELATED_ADDL_INFO = 33
    NO_OPEN_RESUME = 34
    DELETED = 35
    REGULATORY_HALT = 36
    SEC_SUSPENSION = 37
    NON_COMLIANCE = 38
    FILINGS_NOT_CURRENT = 39
    CATS_HALTED = 40
    CATS = 41
    EX_DIV_OR_SPLIT = 42
    UNASSIGNED = 43
    INSIDE_OPEN = 44
    INSIDE_CLOSED = 45
    OFFER_WANTED = 46
    BID_WANTED = 47
    CASH = 48
    INACTIVE = 49
    NATIONAL_BBO = 50
    NOMINAL = 51
    CABINET = 52
    NOMINAL_CABINET = 53
    BLANK_PRICE = 54
    SLOW_BID_ASK = 55
    SLOW_LIST = 56
    SLOW_BID = 57
    SLOW_ASK = 58
    BID_OFFER_WANTED = 59
    SUB_PENNY = 60
    NON_BBO = 61
    SPECIAL_OPEN = 62
    BENCHMARK = 63
    IMPLIED = 64
    EXCHANGE_BEST = 65
    MKT_WIDE_HALT_1 = 66
    MKT_WIDE_HALT_2 = 67
    MKT_WIDE_HALT_3 = 68
    ON_DEMAND_AUCTION = 69
    NON_FIRM_BID = 70
    NON_FIRM_ASK = 71
    RETAIL_BID = 72
    RETAIL_ASK = 73
    RETAIL_QTE = 74

    @classmethod
    def from_code(cls, code: int) -> 'QuoteCondition':
        """
        Create a ``QuoteCondition`` from the integer ``code``. If the ``code``
        is known, return the ``QuoteCondition`` instance. Otherwise, return the
        integer.
        """
        try:
            return cls(code)

        except ValueError:
            return code

_EXCHANGE_NAMES = {
    1: "Nasdaq Exchange",
    2: "Nasdaq Alternative Display Facility",
    3: "New York Stock Exchange",
    4: "American Stock Exchange",
    5: "Chicago Board Options Exchange",
    6: "International Securities Exchange",
    7: "NYSE ARCA (Pacific)",
    8: "National Stock Exchange (Cincinnati)",
    9: "Philadelphia Stock Exchange",
    10: "Options Pricing Reporting Authority",
    11: "Boston Stock/Options Exchange",
    12: "Nasdaq Global+Select Market(NMS)",
    13: "Nasdaq Capital Market (SmallCap)",
    14: "Nasdaq Bulletin Board",
    15: "Nasdaq OTC",
    16: "Nasdaq Indexes (GIDS)",
    17: "Chicago Stock Exchange",
    18: "Toronto Stock Exchange",
    19: "Canadian Venture Exchange",
    20: "Chicago ercantile Exchange",
    21: "New York Board of Trade",
    22: "ISE Mercury",
    23: "COMEX (division of NYMEX)",
    24: "Chicago Board of Trade",
    25: "New York Mercantile Exchange",
    26: "Kansas City Board of Trade",
    27: "Minneapolis Grain Exchange",
    28: "NYSE/ARCABonds",
    29: "NasdaqBasic",
    30: "DowJonesIndices",
    31: "ISEGemini",
    32: "SingaporeInternationalMonetaryExchange",
    33: "LondonStockExchange",
    34: "Eurex",
    35: "Implied Price",
    36: "Data Transmission Network",
    37: "London Metals Exchange Matched Trades",
    38: "London Metals Exchange",
    39: "Intercontinental Exchange (IPE)",
    40: "Nasdaq Mutual Funds (MFDS)",
    41: "COMEX Clearport",
    42: "CBOE C2 Option Exchange",
    43: "Miami Exchange",
    44: "NYMEX Clearport",
    45: "Barclays",
    46: "Miami Emerald Options Exchange",
    47: "NASDAQ Boston",
    48: "HotSpot Eurex US",
    49: "Eurex US",
    50: "Eurex EU",
    51: "Euronext Commodities",
    52: "Euronext Index Derivatives",
    53: "Euronext Interest Rates",
    54: "CBOE Futures Exchange",
    55: "Philadelphia Board of Trade",
    56: "FCME",
    57: "FINRA/NASDAQ Trade Reporting Facility",
    58: "BSE Trade Reporting Facility",
    59: "NYSE Trade Reporting Facility",
    60: "BATS Trading",
    61: "CBOT Floor",
    62: "Pink Sheets",
    63: "BATS Y Exchange",
    64: "Direct Edge A",
    65: "Direct Edge X",
    66: "Russell Indexes",
    67: "CME Indexes",
    68: "Investors Exchange",
    69: "Miami Pearl Options Exchange",
    70: "London Stock Exchange",
    71: "NYSE Global Index Feed",
    72: "TSX Indexes",
    73: "Members Exchange",
    74: "EMPTY",
    75: "Long-Term Stock Exchange",
    76: "EMPTY",
    77: "EMPTY",
}

class Exchange(enum.IntEnum):
    NQEX = 1
    NQAD = 2
    NYSE = 3
    AMEX = 4
    CBOE = 5
    ISEX = 6
    PACF = 7
    CINC = 8
    PHIL = 9
    OPRA = 10
    BOST = 11
    NQNM = 12
    NQSC = 13
    NQBB = 14
    NQPK = 15
    NQIX = 16
    CHIC = 17
    TSE = 18
    CDNX = 19
    CME = 20
    NYBT = 21
    MRCY = 22
    COMX = 23
    CBOT = 24
    NYMX = 25
    KCBT = 26
    MGEX = 27
    NYBO = 28
    NQBS = 29
    DOWJ = 30
    GEMI = 31
    SIMX = 32
    FTSE = 33
    EURX = 34
    IMPL = 35
    DTN = 36
    LMT = 37
    LME = 38
    IPEX = 39
    NQMF = 40
    fcec = 41
    C2 = 42
    MIAX = 43
    CLRP = 44
    BARK = 45
    EMLD = 46
    NQBX = 47
    HOTS = 48
    EUUS = 49
    EUEU = 50
    ENCM = 51
    ENID = 52
    ENIR = 53
    CFE = 54
    PBOT = 55
    CMEFloor = 56
    NQNX = 57
    BTRF = 58
    NTRF = 59
    BATS = 60
    FCBT = 61
    PINK = 62
    BATY = 63
    EDGE = 64
    EDGX = 65
    RUSL = 66
    CMEX = 67
    IEX = 68
    PERL = 69
    LSE = 70
    GIF = 71
    TSIX = 72
    MEMX = 73
    _EMPT1 = 74
    LTSE = 75
    _EMPT2 = 76
    _EMPT3 = 77

    @classmethod
    def from_symbol(cls, symbol: str) -> 'Exchange':
        """
        Get the exchange from the symbol.
        """
        return getattr(cls, symbol)

    @property
    def description(self) -> str:
        return _EXCHANGE_NAMES[self]


class TradeCondition(enum.IntEnum):
    REGULAR = 0
    FORM_T = 1
    OUT_OF_SEQ = 2
    AVG_PRC_NASDAQ = 4
    OPEN_REPORT_LATE = 5
    OPEN_REPORT_OUT_OF_SEQ = 6
    OPEN_REPORT_IN_SEQ = 7
    PRIOR_REFERENCE_PRICE = 8
    NEXT_DAY_SALE = 9
    BUNCHED = 10
    CASH_SALE = 11
    SELLER = 12
    SOLD_LAST = 13
    RULE_127 = 14
    BUNCHED_SOLD = 15
    NON_BOARD_LOT = 16
    POSIT = 17
    AUTO_EXECUTION = 18
    HALT = 19
    DELAYED = 20
    REOPEN = 21
    ACQUISITION = 22
    BURST_BASKET = 25
    OPEN_DETAIL = 26
    INTRA_DETAIL = 27
    BASKET_ON_CLOSE = 28
    RULE_155 = 29
    DISTRIBUTION = 30
    SPLIT = 31
    REGULAR_SETTLE = 32
    CUSTOM_BASKET_CROSS = 33
    ADJ_TERMS = 34
    SPREAD = 35
    STRADDLE = 36
    BUY_WRITE = 37
    COMBO = 38
    STPD = 39
    CANC = 40
    CANC_LAST = 41
    CANC_OPEN = 42
    CANC_ONLY = 43
    CANC_STPD = 44
    MATCH_CROSS = 45
    FAST_MARKET = 46
    NOMINAL = 47
    CABINET = 48
    BLANK_PRICE = 49
    NOT_SPECIFIED = 50
    MC_OFFICIAL_CLOSE = 51
    SPECIAL_TERMS = 52
    CONTINGENT_ORDER = 53
    INTERNAL_CROSS = 54
    STOPPED_REGULAR = 55
    STOPPED_SOLD_LAST = 56
    BASIS = 58
    VWAP = 59
    SPECIAL_SESSION = 60
    NANEX_ADMIN = 61
    OPEN_REPORT = 62
    MARKET_ON_CLOSE = 63
    SETTLE_PRICE = 64
    OUT_OF_SEQ_PRE_MKT = 65
    MC_OFFICIAL_OPEN = 66
    FUTURES_SPREAD = 67
    OPEN_RANGE = 68
    CLOSE_RANGE = 69
    NOMINAL_CABINET = 70
    CHANGING_TRANS = 71
    CHANGING_TRANS_CAB = 72
    NOMINAL_UPDATE = 73
    PIT_SETTLEMENT = 74
    BLOCK_TRADE = 75
    EXG_FOR_PHYSICAL = 76
    VOLUME_ADJUSTMENT = 77
    VOLATILITY_TRADE = 78
    YELLOW_FLAG = 79
    FLOOR_PRICE = 80
    OFFICIAL_PRICE = 81
    UNOFFICIAL_PRICE = 82
    MID_BID_ASK_PRICE = 83
    END_SESSION_HIGH = 84
    END_SESSION_LOW = 85
    BACKWARDATION = 86
    CONTANGO = 87
    HOLIDAY = 88
    PRE_OPENING = 89
    POST_FULL = 90
    POST_RESTRICTED = 91
    CLOSING_AUCTION = 92
    BATCH = 93
    TRADING = 94
    INTERMARKET_SWEEP = 95
    DERIVATIVE = 96
    REOPENING = 97
    CLOSING = 98
    CAPELECTION = 99
    SPOT_SETTLEMENT = 100
    BASIS_HIGH = 101
    BASIS_LOW = 102
    YIELD = 103
    PRICE_VARIATION = 104
    CONTINGENT_TRADE = 105
    STOPPED_IM = 106
    BENCHMARK = 107
    TRADE_THRU_EXEMPT = 108
    IMPLIED = 109
    OTC = 110
    MKT_SUPERVISION = 111
    RESERVED_77 = 112
    RESERVED_91 = 113
    CONTINGENT_UTP = 114
    ODD_LOT = 115
    RESERVED_89 = 116
    CORRECTED_CS_LAST = 117
    OPRA_EXT_HOURS = 118
    RESERVED_78 = 119
    RESERVED_81 = 120
    RESERVED_84 = 121
    RESERVED_878 = 122
    RESERVED_90 = 123
    QUALIFIED_CONTINGENT_TRADE = 124
    SINGLE_LEG_AUCTION_NON_ISO = 125
    SINGLE_LEG_AUCTION_ISO = 126
    SINGLE_LEG_CROSS_NON_ISO = 127
    SINGLE_LEG_CROSS_ISO = 128
    SINGLE_LEG_FLOOR_TRADE = 129
    MULTI_LEG_AUTOELEC_TRADE = 130
    MULTI_LEG_AUCTION = 131
    MULTI_LEG_CROSS = 132
    MULTI_LEG_FLOOR_TRADE = 133
    ML_AUTO_ELEC_TRADE_AGSL = 134
    STOCK_OPTIONS_AUCTION = 135
    ML_AUCTION_AGSL = 136
    ML_FLOOR_TRADE_AGSL = 137
    STK_OPT_AUTO_ELEC_TRADE = 138
    STOCK_OPTIONS_CROSS = 139
    STOCK_OPTIONS_FLOOR_TRADE = 140
    STK_OPT_AE_TRD_AGSL = 141
    STK_OPT_AUCTION_AGSL = 142
    STK_OPT_FLOOR_TRADE_AGSL = 143
    ML_FLOOR_TRADE_OF_PP = 144
    BID_AGGRESSOR = 145
    ASK_AGGRESSOR = 146
    MULTILAT_COMP_TR_PDP = 147
    EXTENDED_HOURS_TRADE = 148

    @property
    def cancel(self) -> bool:
        return _TRADE_CONDITIONS[self]['cancel']

    @property
    def late_report(self) -> bool:
        return _TRADE_CONDITIONS[self]['latereport']

    @property
    def auto_executed(self) -> bool:
        return _TRADE_CONDITIONS[self]['autoexecuted']

    @property
    def volume(self) -> bool:
        return _TRADE_CONDITIONS[self]['volume']

    @property
    def high(self) -> bool:
        return _TRADE_CONDITIONS[self]['high']

    @property
    def low(self) -> bool:
        return _TRADE_CONDITIONS[self]['low']

    @property
    def last(self) -> bool:
        return _TRADE_CONDITIONS[self]['last']
