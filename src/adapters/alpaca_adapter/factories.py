"""
Alpaca factory classes for creating client instances.
"""

from alpaca_adapter.data import StockHistoricalDataClient
from alpaca.trading.client import TradingClient

from alpaca_adapter.config import AlpacaDataClientConfig
from alpaca_adapter.config import AlpacaExecClientConfig
from alpaca_adapter.data import AlpacaDataClient
from alpaca_adapter.execution import AlpacaExecutionClient
from alpaca_adapter.providers import AlpacaInstrumentProvider
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory


class AlpacaLiveDataClientFactory(LiveDataClientFactory):
    """
    Factory for creating Alpaca live data clients.
    """
    
    @staticmethod
    def create(
        loop,
        name: str,
        config: AlpacaDataClientConfig,
        msgbus,
        cache,
        clock,
        **kwargs,
    ) -> AlpacaDataClient:
        """
        Create an Alpaca data client.
        
        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The client name.
        config : AlpacaDataClientConfig
            The configuration for the client.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.
        **kwargs
            Additional keyword arguments.
            
        Returns
        -------
        AlpacaDataClient
            The created data client.
        """
        PyCondition.not_none(config, "config")
        
        # Create Alpaca clients
        data_client = StockHistoricalDataClient(
            api_key=config.api_key,
            secret_key=config.api_secret,
        )
        
        trading_client = TradingClient(
            api_key=config.api_key,
            secret_key=config.api_secret,
            paper=config.sandbox,
        )
        
        # Create instrument provider
        provider = AlpacaInstrumentProvider(
            client=trading_client,
            data_client=data_client,
        )
        
        # Create data client
        client = AlpacaDataClient(
            loop=loop,
            client=provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )
        
        return client


class AlpacaLiveExecClientFactory(LiveExecClientFactory):
    """
    Factory for creating Alpaca live execution clients.
    """
    
    @staticmethod
    def create(
        loop,
        name: str,
        config: AlpacaExecClientConfig,
        msgbus,
        cache,
        clock,
        **kwargs,
    ) -> AlpacaExecutionClient:
        """
        Create an Alpaca execution client.
        
        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The client name.
        config : AlpacaExecClientConfig
            The configuration for the client.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.
        **kwargs
            Additional keyword arguments.
            
        Returns
        -------
        AlpacaExecutionClient
            The created execution client.
        """
        PyCondition.not_none(config, "config")
        
        # Get account from kwargs or create default
        account = kwargs.get("account")
        if not account:
            raise ValueError("Account must be provided for execution client")
        
        # Create Alpaca clients
        data_client = StockHistoricalDataClient(
            api_key=config.api_key,
            secret_key=config.api_secret,
        )
        
        trading_client = TradingClient(
            api_key=config.api_key,
            secret_key=config.api_secret,
            paper=config.sandbox,
        )
        
        # Create instrument provider
        provider = AlpacaInstrumentProvider(
            client=trading_client,
            data_client=data_client,
        )
        
        # Create execution client
        client = AlpacaExecutionClient(
            loop=loop,
            client=provider,
            account=account,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )
        
        return client


class AlpacaInstrumentProviderFactory:
    """
    Factory for creating Alpaca instrument providers.
    """
    
    @staticmethod
    def create(
        config: AlpacaDataClientConfig | AlpacaExecClientConfig,
    ) -> AlpacaInstrumentProvider:
        """
        Create an Alpaca instrument provider.
        
        Parameters
        ----------
        config : AlpacaDataClientConfig or AlpacaExecClientConfig
            The configuration for the provider.
            
        Returns
        -------
        AlpacaInstrumentProvider
            The created instrument provider.
        """
        PyCondition.not_none(config, "config")
        
        # Create Alpaca clients
        data_client = StockHistoricalDataClient(
            api_key=config.api_key,
            secret_key=config.api_secret,
        )
        
        trading_client = TradingClient(
            api_key=config.api_key,
            secret_key=config.api_secret,
            paper=getattr(config, 'sandbox', True),
        )
        
        return AlpacaInstrumentProvider(
            client=trading_client,
            data_client=data_client,
        )