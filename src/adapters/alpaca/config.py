from typing import Optional
from nautilus_trader.config import LiveDataClientConfig, LiveExecClientConfig

from adapters.alpaca.common import AlpacaAccountType


class AlpacaDataClientConfig(LiveDataClientConfig, frozen=True):
    """Configuration for Alpaca data client."""
    
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    account_type: AlpacaAccountType = AlpacaAccountType.PAPER
    base_url_http: Optional[str] = None
    base_url_ws: Optional[str] = None


class AlpacaExecClientConfig(LiveExecClientConfig, frozen=True):
    """Configuration for Alpaca execution client."""
    
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    account_type: AlpacaAccountType = AlpacaAccountType.PAPER
    base_url_http: Optional[str] = None
    base_url_ws: Optional[str] = None