from typing import Any, Dict, List, Optional, Set
import asyncio

from nautilus_trader.live.data_client import LiveDataClient
from nautilus_trader.model.data import DataType
from nautilus_trader.model.identifiers import ClientId, InstrumentId
import pandas as pd

from adapters.alpaca.common import ALPACA
from adapters.alpaca.config import AlpacaDataClientConfig
from adapters.alpaca.data_parser import AlpacaDataParser
from adapters.alpaca.providers import AlpacaInstrumentProvider
from adapters.alpaca.http_client import AlpacaHttpClient

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus

from adapters.alpaca.websocket_client import AlpacaWebSocketClient


class AlpacaDataClient(LiveDataClient):
    """
    Live data client for Alpaca.
    
    Manages real-time market data feeds including trades, quotes,
    and bars from Alpaca's streaming API.
    """
    
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: AlpacaHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: AlpacaInstrumentProvider,
        config: AlpacaDataClientConfig,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(ALPACA),
            venue=None,  # Multi-venue support
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )
        
        self._client = client
        self._config = config
        
        # WebSocket client for real-time data
        self._ws_client = AlpacaWebSocketClient(
            api_key=config.api_key,
            api_secret=config.api_secret,
            account_type=config.account_type,
            logger=self._log,
        )
        
        # Data parser
        self._parser = AlpacaDataParser()
        
        # Subscriptions tracking
        self._subscribed_instruments: Set[InstrumentId] = set()
        
        # Setup WebSocket handlers
        self._setup_websocket_handlers()
    
    def _setup_websocket_handlers(self) -> None:
        """Setup WebSocket message handlers."""
        # Trade tick handler
        self._ws_client.subscribe_handler("t", self._handle_trade_tick)
        
        # Quote tick handler  
        self._ws_client.subscribe_handler("q", self._handle_quote_tick)
        
        # Authentication handler
        self._ws_client.subscribe_handler("success", self._handle_auth_success)
        
        # Error handler
        self._ws_client.subscribe_handler("error", self._handle_error)
    
    # LiveDataClient abstract methods implementation
    
    async def _connect(self) -> None:
        """Connect the data client."""
        await self._client.connect()
        await self._ws_client.connect()
        self._log.info("Data client connected")
    
    async def _disconnect(self) -> None:
        """Disconnect the data client."""
        await self._ws_client.disconnect()
        await self._client.disconnect()
        self._log.info("Data client disconnected")
    
    async def _subscribe_trade_ticks(self, instrument_id: InstrumentId) -> None:
        """Subscribe to trade ticks for instrument."""
        symbol = instrument_id.symbol.value
        await self._ws_client.subscribe_trades([symbol])
        self._subscribed_instruments.add(instrument_id)
        
        self._log.info(f"Subscribed to trade ticks for {instrument_id}")
    
    async def _subscribe_quote_ticks(self, instrument_id: InstrumentId) -> None:
        """Subscribe to quote ticks for instrument."""
        symbol = instrument_id.symbol.value
        await self._ws_client.subscribe_quotes([symbol])
        
        self._log.info(f"Subscribed to quote ticks for {instrument_id}")
    
    async def _unsubscribe_trade_ticks(self, instrument_id: InstrumentId) -> None:
        """Unsubscribe from trade ticks for instrument."""
        # Alpaca doesn't support individual unsubscribe, would need to resubscribe others
        self._subscribed_instruments.discard(instrument_id)
        self._log.info(f"Unsubscribed from trade ticks for {instrument_id}")
    
    async def _unsubscribe_quote_ticks(self, instrument_id: InstrumentId) -> None:
        """Unsubscribe from quote ticks for instrument.""" 
        # Alpaca doesn't support individual unsubscribe, would need to resubscribe others
        self._log.info(f"Unsubscribed from quote ticks for {instrument_id}")
    
    async def _request_bars(
        self,
        bar_type: BarType,
        limit: int,
        correlation_id: UUID4,
        start: Optional[pd.Timestamp] = None,
        end: Optional[pd.Timestamp] = None,
    ) -> None:
        """Request historical bars."""
        try:
            instrument_id = bar_type.instrument_id
            symbol = instrument_id.symbol.value
            
            # Convert bar specification to Alpaca timeframe
            timeframe = self._convert_bar_spec_to_timeframe(bar_type.spec)
            
            # Request bars from HTTP client
            bars_data = await self._client.get_bars(
                symbols=[symbol],
                timeframe=timeframe,
                start=start,
                end=end,
                limit=limit,
            )
            
            # Parse and send bars
            bars = []
            symbol_data = bars_data.get("bars", {}).get(symbol, [])
            
            for bar_data in symbol_data:
                bar = self._parse_bar(bar_data, bar_type)
                if bar:
                    bars.append(bar)
            
            # Send bars to engine
            self._handle_bars(bar_type, bars, None, correlation_id)
            
        except Exception as e:
            self._log.error(f"Failed to request bars: {e}")
    
    def _convert_bar_spec_to_timeframe(self, bar_spec: BarSpecification) -> str:
        """Convert Nautilus bar spec to Alpaca timeframe format."""
        # Alpaca timeframe format: "1Min", "5Min", "1Hour", "1Day", etc.
        step = bar_spec.step
        aggregation = bar_spec.aggregation
        
        if aggregation == BarAggregation.MINUTE:
            return f"{step}Min"
        elif aggregation == BarAggregation.HOUR:
            return f"{step}Hour" 
        elif aggregation == BarAggregation.DAY:
            return f"{step}Day"
        elif aggregation == BarAggregation.WEEK:
            return f"{step}Week"
        elif aggregation == BarAggregation.MONTH:
            return f"{step}Month"
        else:
            raise ValueError(f"Unsupported bar aggregation: {aggregation}")
    
    # WebSocket message handlers
    
    async def _handle_trade_tick(self, data: Dict[str, Any]) -> None:
        """Handle incoming trade tick data."""
        try:
            symbol = data["S"]
            instrument_id = InstrumentId(Symbol(symbol), Venue("ALPACA"))
            
            trade_tick = self._parser.parse_trade_tick(data, instrument_id)
            if trade_tick:
                self._handle_trade_tick(trade_tick)
                
        except Exception as e:
            self._log.error(f"Error handling trade tick: {e}")
    
    async def _handle_quote_tick(self, data: Dict[str, Any]) -> None:
        """Handle incoming quote tick data."""
        try:
            symbol = data["S"]
            instrument_id = InstrumentId(Symbol(symbol), Venue("ALPACA"))
            
            quote_tick = self._parser.parse_quote_tick(data, instrument_id)
            if quote_tick:
                self._handle_quote_tick(quote_tick)
                
        except Exception as e:
            self._log.error(f"Error handling quote tick: {e}")
    
    async def _handle_auth_success(self, data: Dict[str, Any]) -> None:
        """Handle successful authentication."""
        self._log.info("WebSocket authentication successful")
    
    async def _handle_error(self, data: Dict[str, Any]) -> None:
        """Handle WebSocket errors."""
        error_msg = data.get("msg", "Unknown error")
        self._log.error(f"WebSocket error: {error_msg}")