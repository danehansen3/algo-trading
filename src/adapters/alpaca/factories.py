import asyncio
import os
from nautilus_trader.live.factories import LiveDataClientFactory, LiveExecClientFactory
from nautilus_trader.common.component import Logger

from adapters.alpaca.config import AlpacaDataClientConfig, AlpacaExecClientConfig
from adapters.alpaca.data_client import AlpacaDataClient
from adapters.alpaca.execution import AlpacaExecutionClient
from adapters.alpaca.http_client import AlpacaHttpClient
from adapters.alpaca.providers import AlpacaInstrumentProvider

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus


class AlpacaLiveDataClientFactory(LiveDataClientFactory):
    """Factory for Alpaca live data clients."""
    
    @staticmethod
    def create(
        name: str,
        config: AlpacaDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        logger: Logger,
    ) -> AlpacaDataClient:
        """Create Alpaca data client."""
        
        # Get API credentials from config or environment
        api_key = config.api_key
        api_secret = config.api_secret
        
        if not api_key:
            api_key = os.getenv("ALPACA_API_KEY")
        if not api_secret:
            api_secret = os.getenv("ALPACA_API_SECRET")
        
        if not api_key or not api_secret:
            raise ValueError("Alpaca API credentials not found")
        
        # Create HTTP client
        http_client = AlpacaHttpClient(
            api_key=api_key,
            api_secret=api_secret,
            base_url=config.base_url_http,
            account_type=config.account_type,
            logger=logger,
        )
        
        # Create instrument provider
        instrument_provider = AlpacaInstrumentProvider(
            client=http_client,
            logger=logger,
        )
        
        # Create data client
        return AlpacaDataClient(
            loop=asyncio.get_event_loop(),
            client=http_client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=config,
        )


class AlpacaLiveExecClientFactory(LiveExecClientFactory):
    """Factory for Alpaca live execution clients."""
    
    @staticmethod
    def create(
        name: str,
        config: AlpacaExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        logger: Logger,
    ) -> AlpacaExecutionClient:
        """Create Alpaca execution client."""
        
        # Get API credentials from config or environment
        api_key = config.api_key
        api_secret = config.api_secret
        
        if not api_key:
            api_key = os.getenv("ALPACA_API_KEY")
        if not api_secret:
            api_secret = os.getenv("ALPACA_API_SECRET")
        
        if not api_key or not api_secret:
            raise ValueError("Alpaca API credentials not found")
        
        # Create HTTP client
        http_client = AlpacaHttpClient(
            api_key=api_key,
            api_secret=api_secret,
            base_url=config.base_url_http,
            account_type=config.account_type,
            logger=logger,
        )
        
        # Create instrument provider
        instrument_provider = AlpacaInstrumentProvider(
            client=http_client,
            logger=logger,
        )
        
        # Create execution client
        return AlpacaExecutionClient(
            loop=asyncio.get_event_loop(),
            client=http_client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=config,
        )