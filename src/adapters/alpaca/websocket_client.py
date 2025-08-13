import json
import asyncio
from typing import Callable, List, Optional, Set, Dict, Any
from adapters.alpaca.common import AlpacaAccountType
import websockets
from websockets.exceptions import ConnectionClosed

from nautilus_trader.common.component import Logger


class AlpacaWebSocketClient:
    """
    Low-level WebSocket client for Alpaca streaming API.
    
    Handles real-time data streams from Alpaca including trades,
    quotes, and account updates.
    """
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        account_type: AlpacaAccountType = AlpacaAccountType.PAPER,
        logger: Optional[Logger] = None,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._account_type = account_type
        self._logger = logger or Logger(name=type(self).__name__)
        
        # WebSocket URLs
        if account_type == AlpacaAccountType.PAPER:
            self._trading_ws_url = "wss://paper-api.alpaca.markets/stream"
            self._data_ws_url = "wss://stream.data.alpaca.markets/v2/iex"
        else:
            self._trading_ws_url = "wss://api.alpaca.markets/stream" 
            self._data_ws_url = "wss://stream.data.alpaca.markets/v2/iex"
        
        self._websocket: Optional[websockets.WebSocketServerProtocol] = None
        self._data_websocket: Optional[websockets.WebSocketServerProtocol] = None
        
        self._is_connected = False
        self._handlers: Dict[str, Callable] = {}
        
        self._subscribed_symbols: Set[str] = set()
        self._running_tasks: Set[asyncio.Task] = set()
    
    def subscribe_handler(self, event_type: str, handler: Callable) -> None:
        """Subscribe event handler for specific message type."""
        self._handlers[event_type] = handler
    
    async def connect(self) -> None:
        """Connect to WebSocket streams."""
        try:
            # Connect to trading stream
            self._websocket = await websockets.connect(
                self._trading_ws_url,
                extra_headers={
                    "APCA-API-KEY-ID": self._api_key,
                    "APCA-API-SECRET-KEY": self._api_secret,
                }
            )
            
            # Connect to data stream  
            self._data_websocket = await websockets.connect(self._data_ws_url)
            
            # Authenticate data stream
            auth_msg = {
                "action": "auth",
                "key": self._api_key,
                "secret": self._api_secret,
            }
            await self._data_websocket.send(json.dumps(auth_msg))
            
            self._is_connected = True
            
            # Start message handlers
            trading_task = asyncio.create_task(self._handle_trading_messages())
            data_task = asyncio.create_task(self._handle_data_messages())
            
            self._running_tasks.add(trading_task)
            self._running_tasks.add(data_task)
            
            self._logger.info("WebSocket clients connected")
            
        except Exception as e:
            self._logger.error(f"WebSocket connection failed: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Disconnect WebSocket clients."""
        self._is_connected = False
        
        # Cancel running tasks
        for task in self._running_tasks:
            task.cancel()
        
        # Close connections
        if self._websocket:
            await self._websocket.close()
        if self._data_websocket:
            await self._data_websocket.close()
            
        self._logger.info("WebSocket clients disconnected")
    
    async def _handle_trading_messages(self) -> None:
        """Handle messages from trading stream."""
        try:
            async for message in self._websocket:
                try:
                    data = json.loads(message)
                    msg_type = data.get("stream")
                    
                    if msg_type and msg_type in self._handlers:
                        await self._handlers[msg_type](data)
                        
                except Exception as e:
                    self._logger.error(f"Error handling trading message: {e}")
                    
        except ConnectionClosed:
            self._logger.warning("Trading WebSocket connection closed")
        except Exception as e:
            self._logger.error(f"Trading WebSocket error: {e}")
    
    async def _handle_data_messages(self) -> None:
        """Handle messages from data stream."""
        try:
            async for message in self._data_websocket:
                try:
                    data = json.loads(message)
                    
                    # Handle different message types
                    if isinstance(data, list):
                        for item in data:
                            msg_type = item.get("T")  # Message type
                            if msg_type and msg_type in self._handlers:
                                await self._handlers[msg_type](item)
                    else:
                        msg_type = data.get("T")
                        if msg_type and msg_type in self._handlers:
                            await self._handlers[msg_type](data)
                            
                except Exception as e:
                    self._logger.error(f"Error handling data message: {e}")
                    
        except ConnectionClosed:
            self._logger.warning("Data WebSocket connection closed")
        except Exception as e:
            self._logger.error(f"Data WebSocket error: {e}")
    
    async def subscribe_trades(self, symbols: List[str]) -> None:
        """Subscribe to trade data for symbols."""
        if not self._data_websocket:
            return
            
        subscribe_msg = {
            "action": "subscribe",
            "trades": symbols,
        }
        await self._data_websocket.send(json.dumps(subscribe_msg))
        self._subscribed_symbols.update(symbols)
        
        self._logger.info(f"Subscribed to trades: {symbols}")
    
    async def subscribe_quotes(self, symbols: List[str]) -> None:
        """Subscribe to quote data for symbols.""" 
        if not self._data_websocket:
            return
            
        subscribe_msg = {
            "action": "subscribe", 
            "quotes": symbols,
        }
        await self._data_websocket.send(json.dumps(subscribe_msg))
        
        self._logger.info(f"Subscribed to quotes: {symbols}")
    
    async def subscribe_account_updates(self) -> None:
        """Subscribe to account update stream."""
        if not self._websocket:
            return
            
        # Account updates are automatically streamed when authenticated
        self._logger.info("Subscribed to account updates")