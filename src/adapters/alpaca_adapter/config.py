"""
Configuration classes for Alpaca adapter.
"""

from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig


class AlpacaDataClientConfig(LiveDataClientConfig):
    """
    Configuration for ``AlpacaDataClient`` instances.
    
    Parameters
    ----------
    api_key : str
        The Alpaca API key for authentication.
    api_secret : str
        The Alpaca API secret for authentication.
    base_url : str, default "https://paper-api.alpaca.markets"
        The base URL for Alpaca API endpoints.
    data_feed : str, default "iex"
        The data feed to use ('iex', 'sip', or 'otc').
    use_raw_data : bool, default False
        Whether to use raw data format from Alpaca.
    sandbox : bool, default True
        Whether to use paper trading environment.
    """
    
    api_key: str
    api_secret: str
    base_url: str = "https://paper-api.alpaca.markets"
    data_feed: str = "iex"  # iex, sip, or otc
    use_raw_data: bool = False
    sandbox: bool = True


class AlpacaExecClientConfig(LiveExecClientConfig):
    """
    Configuration for ``AlpacaExecutionClient`` instances.
    
    Parameters
    ----------
    api_key : str
        The Alpaca API key for authentication.
    api_secret : str
        The Alpaca API secret for authentication.
    base_url : str, default "https://paper-api.alpaca.markets"
        The base URL for Alpaca API endpoints.
    use_raw_data : bool, default False
        Whether to use raw data format from Alpaca.
    sandbox : bool, default True
        Whether to use paper trading environment.
    """
    
    api_key: str
    api_secret: str
    base_url: str = "https://paper-api.alpaca.markets"
    use_raw_data: bool = False
    sandbox: bool = True