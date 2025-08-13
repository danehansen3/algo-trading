from enum import Enum
from typing import Dict, Any

# Adapter identifier
ALPACA = "ALPACA"

class AlpacaAccountType(Enum):
    """Alpaca account type options."""
    PAPER = "paper"
    LIVE = "live"

class AlpacaAssetClass(Enum):
    """Alpaca asset classes."""
    US_EQUITY = "us_equity"
    CRYPTO = "crypto"

class AlpacaOrderClass(Enum):
    """Alpaca order classes."""
    SIMPLE = "simple"
    BRACKET = "bracket"
    OCO = "oco"
    OTO = "oto"

class AlpacaOrderType(Enum):
    """Alpaca order types."""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"

class AlpacaTimeInForce(Enum):
    """Alpaca time in force options."""
    DAY = "day"
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"