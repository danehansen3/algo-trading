"""
Alpaca data client implementation.
"""

import asyncio
from datetime import datetime
from typing import Any

from alpaca.data import StockHistoricalDataClient
from alpaca.data.live import StockDataStream
from alpaca.data.models import Quote, Trade, Bar
from alpaca.data.enums import DataFeed

from alpaca_adapter.config import AlpacaDataClientConfig
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.data import Bar
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class AlpacaDataClient(LiveMarketDataClient):
    """
    Provides a data client for Alpaca Markets.
    
    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    client : AlpacaInstrumentProvider
        The Alpaca instrument provider.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    config : AlpacaDataClientConfig
        The configuration for the client.
    """
    
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client,
        msgbus,
        cache,
        clock,
        config: AlpacaDataClientConfig,
    ) -> None:
        PyCondition.not_none(config, "config")
        
        super().__init__(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )
        
        self._config = config
        self._venue = Venue("ALPACA")
        
        # Initialize Alpaca clients
        self._data_client = StockHistoricalDataClient(
            api_key=config.api_key,
            secret_key=config.api_secret,
        )
        
        # Initialize data stream
        self._stream = StockDataStream(
            api_key=config.api_key,
            secret_key=config.api_secret,
            raw_data=config.use_raw_data,
            feed=DataFeed.IEX if config.data_feed == "iex" else DataFeed.SIP,
        )
        
        # Subscription tracking
        self._subscribed_quotes: set[str] = set()
        self._subscribed_trades: set[str] = set()
        self._subscribed_bars: set[str] = set()
        
        self._setup_stream_handlers()
    
    def _setup_stream_handlers(self) -> None:
        """Set up data stream event handlers."""
        
        async def quote_data_handler(data: Quote) -> None:
            """Handle incoming quote data."""
            try:
                quote_tick = self._parse_quote_tick(data)
                if quote_tick:
                    self._handle_data(quote_tick)
            except Exception as e:
                self._log.error(f"Error processing quote data: {e}")
        
        async def trade_data_handler(data: Trade) -> None:
            """Handle incoming trade data."""
            try:
                trade_tick = self._parse_trade_tick(data)
                if trade_tick:
                    self._handle_data(trade_tick)
            except Exception as e:
                self._log.error(f"Error processing trade data: {e}")
        
        async def bar_data_handler(data: Bar) -> None:
            """Handle incoming bar data."""
            try:
                bar = self._parse_bar(data)
                if bar:
                    self._handle_data(bar)
            except Exception as e:
                self._log.error(f"Error processing bar data: {e}")
        
        # Assign handlers
        self._stream._quote_handler = quote_data_handler
        self._stream._trade_handler = trade_data_handler
        self._stream._bar_handler = bar_data_handler
    
    @property
    def subscribed_instruments(self) -> list[InstrumentId]:
        """Return the subscribed instrument IDs."""
        symbols = set()
        symbols.update(self._subscribed_quotes)
        symbols.update(self._subscribed_trades)
        symbols.update(self._subscribed_bars)
        
        return [
            InstrumentId(Symbol(symbol), self._venue) 
            for symbol in symbols
        ]
    
    # Connection management
    async def _connect(self) -> None:
        """Connect to the Alpaca data stream."""
        self._log.info("Connecting to Alpaca data stream...")
        
        try:
            # Start the data stream
            await self._stream._run_forever()
            self._log.info(
                "Connected to Alpaca data stream",
                LogColor.GREEN,
            )
        except Exception as e:
            self._log.error(f"Failed to connect to Alpaca data stream: {e}")
            raise
    
    async def _disconnect(self) -> None:
        """Disconnect from the Alpaca data stream."""
        self._log.info("Disconnecting from Alpaca data stream...")
        
        try:
            await self._stream._stop()
            self._log.info("Disconnected from Alpaca data stream")
        except Exception as e:
            self._log.error(f"Error disconnecting from Alpaca data stream: {e}")
    
    def reset(self) -> None:
        """Reset the data client state."""
        self._subscribed_quotes.clear()
        self._subscribed_trades.clear()
        self._subscribed_bars.clear()
    
    def dispose(self) -> None:
        """Dispose of the data client resources."""
        # Clean up any remaining resources
        if self._data_client:
            self._data_client = None
        if self._stream:
            self._stream = None
    
    # Subscription methods
    async def _subscribe_instruments(self) -> None:
        """Subscribe to instrument status updates."""
        # Alpaca doesn't provide general instrument status updates
        # This method is a no-op for now
        pass
    
    async def _unsubscribe_instruments(self) -> None:
        """Unsubscribe from instrument status updates."""
        # Alpaca doesn't provide general instrument status updates
        # This method is a no-op for now
        pass
    
    async def _subscribe_order_book_deltas(
        self,
        instrument_id: InstrumentId,
        book_type: BookType,
        depth: int | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        """
        Subscribe to order book delta updates for an instrument.
        
        Note: Alpaca typically provides Level 1 data only (best bid/ask).
        """
        symbol = instrument_id.symbol.value
        
        # Subscribe to quotes (which provide best bid/ask)
        if symbol not in self._subscribed_quotes:
            self._stream.subscribe_quotes(self._quote_handler, symbol)
            self._subscribed_quotes.add(symbol)
            
            self._log.info(
                f"Subscribed to order book deltas for {symbol}",
                LogColor.BLUE,
            )
    
    async def _unsubscribe_order_book_deltas(self, instrument_id: InstrumentId) -> None:
        """Unsubscribe from order book delta updates for an instrument."""
        symbol = instrument_id.symbol.value
        
        if symbol in self._subscribed_quotes:
            self._stream.unsubscribe_quotes(symbol)
            self._subscribed_quotes.discard(symbol)
            
            self._log.info(f"Unsubscribed from order book deltas for {symbol}")
    
    async def _subscribe_quote_ticks(self, instrument_id: InstrumentId) -> None:
        """Subscribe to quote tick updates for an instrument."""
        symbol = instrument_id.symbol.value
        
        if symbol not in self._subscribed_quotes:
            self._stream.subscribe_quotes(self._quote_handler, symbol)
            self._subscribed_quotes.add(symbol)
            
            self._log.info(f"Subscribed to quotes for {symbol}", LogColor.BLUE)
    
    async def _unsubscribe_quote_ticks(self, instrument_id: InstrumentId) -> None:
        """Unsubscribe from quote tick updates for an instrument."""
        symbol = instrument_id.symbol.value
        
        if symbol in self._subscribed_quotes:
            self._stream.unsubscribe_quotes(symbol)
            self._subscribed_quotes.discard(symbol)
            
            self._log.info(f"Unsubscribed from quotes for {symbol}")
    
    async def _subscribe_trade_ticks(self, instrument_id: InstrumentId) -> None:
        """Subscribe to trade tick updates for an instrument."""
        symbol = instrument_id.symbol.value
        
        if symbol not in self._subscribed_trades:
            self._stream.subscribe_trades(self._trade_handler, symbol)
            self._subscribed_trades.add(symbol)
            
            self._log.info(f"Subscribed to trades for {symbol}", LogColor.BLUE)
    
    async def _unsubscribe_trade_ticks(self, instrument_id: InstrumentId) -> None:
        """Unsubscribe from trade tick updates for an instrument."""
        symbol = instrument_id.symbol.value
        
        if symbol in self._subscribed_trades:
            self._stream.unsubscribe_trades(symbol)
            self._subscribed_trades.discard(symbol)
            
            self._log.info(f"Unsubscribed from trades for {symbol}")
    
    async def _subscribe_bars(self, bar_type) -> None:
        """Subscribe to bar updates for an instrument."""
        symbol = bar_type.instrument_id.symbol.value
        
        if symbol not in self._subscribed_bars:
            self._stream.subscribe_bars(self._bar_handler, symbol)
            self._subscribed_bars.add(symbol)
            
            self._log.info(f"Subscribed to bars for {symbol}", LogColor.BLUE)
    
    async def _unsubscribe_bars(self, bar_type) -> None:
        """Unsubscribe from bar updates for an instrument."""
        symbol = bar_type.instrument_id.symbol.value
        
        if symbol in self._subscribed_bars:
            self._stream.unsubscribe_bars(symbol)
            self._subscribed_bars.discard(symbol)
            
            self._log.info(f"Unsubscribed from bars for {symbol}")
    
    # Data parsing methods
    def _parse_quote_tick(self, data: Quote) -> QuoteTick | None:
        """Parse Alpaca quote data into a QuoteTick."""
        try:
            instrument_id = InstrumentId(Symbol(data.symbol), self._venue)
            
            return QuoteTick(
                instrument_id=instrument_id,
                bid_price=Price.from_str(str(data.bid_price)),
                ask_price=Price.from_str(str(data.ask_price)),
                bid_size=Quantity.from_int(data.bid_size or 0),
                ask_size=Quantity.from_int(data.ask_size or 0),
                ts_event=self._clock.timestamp_ns(),
                ts_init=self._clock.timestamp_ns(),
            )
        except Exception as e:
            self._log.error(f"Failed to parse quote tick: {e}")
            return None
    
    def _parse_trade_tick(self, data: Trade) -> TradeTick | None:
        """Parse Alpaca trade data into a TradeTick."""
        try:
            instrument_id = InstrumentId(Symbol(data.symbol), self._venue)
            
            return TradeTick(
                instrument_id=instrument_id,
                price=Price.from_str(str(data.price)),
                size=Quantity.from_int(data.size),
                aggressor_side=AggressorSide.NO_AGGRESSOR,  # Alpaca doesn't provide this
                trade_id=TradeId(str(data.timestamp)),  # Use timestamp as trade ID
                ts_event=self._clock.timestamp_ns(),
                ts_init=self._clock.timestamp_ns(),
            )
        except Exception as e:
            self._log.error(f"Failed to parse trade tick: {e}")
            return None
    
    def _parse_bar(self, data: Bar) -> Bar | None:
        """Parse Alpaca bar data into a Bar."""
        try:
            # This would need to be implemented based on the specific
            # Bar type and timeframe being subscribed to
            # For now, return None as bars require more complex parsing
            return None
        except Exception as e:
            self._log.error(f"Failed to parse bar: {e}")
            return None