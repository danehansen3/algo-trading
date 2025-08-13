from typing import Any, Dict, List, Optional
import asyncio
from decimal import Decimal

from nautilus_trader.adapters.alpaca.http.client import AlpacaHttpClient
from nautilus_trader.adapters.alpaca.websocket.client import AlpacaWebSocketClient
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.commands import SubmitOrder, CancelOrder, ModifyOrder
from nautilus_trader.model.events import OrderSubmitted, OrderAccepted, OrderFilled, OrderCanceled
from nautilus_trader.model.identifiers import ClientId, ClientOrderId, VenueOrderId, AccountId
from nautilus_trader.model.orders import Order, MarketOrder, LimitOrder, StopMarketOrder, StopLimitOrder
from nautilus_trader.model.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from nautilus_trader.model.objects import Money, AccountBalance, MarginBalance
from nautilus_trader.execution.reports import AccountState, PositionStatusReport, OrderStatusReport

from adapters.alpaca.common import ALPACA
from adapters.alpaca.config import AlpacaExecClientConfig
from adapters.alpaca.providers import AlpacaInstrumentProvider

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus


class AlpacaExecutionClient(LiveExecutionClient):
    """
    Live execution client for Alpaca.
    
    Manages order execution, account management, and position tracking
    with Alpaca's trading API.
    """
    
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: AlpacaHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: AlpacaInstrumentProvider,
        config: AlpacaExecClientConfig,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(ALPACA),
            venue=None,  # Multi-venue support
            oms_type=OMSType.NETTING,  # Alpaca uses netting positions
            account_type=AccountType.MARGIN,  # Default to margin account
            base_currency=None,  # Will be set from account info
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )
        
        self._client = client
        self._config = config
        
        # WebSocket client for account updates
        self._ws_client = AlpacaWebSocketClient(
            api_key=config.api_key,
            api_secret=config.api_secret,
            account_type=config.account_type,
            logger=self._log,
        )
        
        # Order tracking
        self._venue_order_ids: Dict[ClientOrderId, VenueOrderId] = {}
        self._client_order_ids: Dict[VenueOrderId, ClientOrderId] = {}
        
        # Account info
        self._account_id: Optional[AccountId] = None
        
        # Setup WebSocket handlers
        self._setup_websocket_handlers()
    
    def _setup_websocket_handlers(self) -> None:
        """Setup WebSocket message handlers."""
        # Account update handlers
        self._ws_client.subscribe_handler("account_updates", self._handle_account_update)
        self._ws_client.subscribe_handler("trade_updates", self._handle_trade_update)
    
    # LiveExecutionClient abstract methods implementation
    
    async def _connect(self) -> None:
        """Connect the execution client."""
        await self._client.connect()
        await self._ws_client.connect()
        
        # Subscribe to account updates
        await self._ws_client.subscribe_account_updates()
        
        # Load account information
        await self._load_account_info()
        
        self._log.info("Execution client connected")
    
    async def _disconnect(self) -> None:
        """Disconnect the execution client."""
        await self._ws_client.disconnect()
        await self._client.disconnect()
        self._log.info("Execution client disconnected")
    
    async def _load_account_info(self) -> None:
        """Load account information from Alpaca."""
        try:
            account_data = await self._client.get_account()
            
            # Set account ID
            self._account_id = AccountId(f"{ALPACA}-{account_data['id']}")
            
            # Create account state
            account_state = self._create_account_state(account_data)
            self.generate_account_state(
                account_state=account_state,
                ts_event=self._clock.timestamp_ns(),
            )
            
            self._log.info(f"Loaded account: {self._account_id}")
            
        except Exception as e:
            self._log.error(f"Failed to load account info: {e}")
            raise
    
    def _create_account_state(self, account_data: Dict[str, Any]) -> AccountState:
        """Create account state from Alpaca account data."""
        # Parse account balances
        equity = Decimal(account_data.get("equity", "0"))
        buying_power = Decimal(account_data.get("buying_power", "0"))
        cash = Decimal(account_data.get("cash", "0"))
        
        # Create balance objects
        balances = [
            AccountBalance(
                total=Money(equity, "USD"),
                locked=Money(Decimal("0"), "USD"),
                free=Money(buying_power, "USD"),
            )
        ]
        
        # Create margin balance if margin account
        margins = []
        if account_data.get("pattern_day_trader", False):
            margins.append(
                MarginBalance(
                    initial=Money(cash * Decimal("0.5"), "USD"),  # 50% margin requirement
                    maintenance=Money(cash * Decimal("0.25"), "USD"),  # 25% maintenance
                )
            )
        
        return AccountState(
            account_id=self._account_id,
            account_type=AccountType.MARGIN if margins else AccountType.CASH,
            base_currency="USD",
            reported=True,
            balances=balances,
            margins=margins,
            info=account_data,
            event_id=UUID4(),
            ts_event=self._clock.timestamp_ns(),
            ts_init=self._clock.timestamp_ns(),
        )
    
    async def _submit_order(self, command: SubmitOrder) -> None:
        """Submit order to Alpaca."""
        try:
            order = command.order
            
            # Convert Nautilus order to Alpaca format
            alpaca_order = self._convert_order_to_alpaca(order)
            
            # Submit order via HTTP
            response = await self._client.submit_order(alpaca_order)
            
            # Extract venue order ID
            venue_order_id = VenueOrderId(response["id"])
            
            # Store order ID mapping
            self._venue_order_ids[order.client_order_id] = venue_order_id
            self._client_order_ids[venue_order_id] = order.client_order_id
            
            # Generate order submitted event
            self.generate_order_submitted(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                ts_event=self._clock.timestamp_ns(),
            )
            
            # Generate order accepted event
            self.generate_order_accepted(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=venue_order_id,
                ts_event=self._clock.timestamp_ns(),
            )
            
            self._log.info(f"Submitted order {order.client_order_id} -> {venue_order_id}")
            
        except Exception as e:
            # Generate order rejected event
            self.generate_order_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                reason=str(e),
                ts_event=self._clock.timestamp_ns(),
            )
            self._log.error(f"Failed to submit order {command.client_order_id}: {e}")
    
    async def _cancel_order(self, command: CancelOrder) -> None:
        """Cancel order on Alpaca."""
        try:
            venue_order_id = self._venue_order_ids.get(command.client_order_id)
            if not venue_order_id:
                self._log.warning(f"Cannot find venue order ID for {command.client_order_id}")
                return
            
            # Cancel order via HTTP
            await self._client.cancel_order(venue_order_id.value)
            
            self._log.info(f"Cancelled order {command.client_order_id}")
            
        except Exception as e:
            self._log.error(f"Failed to cancel order {command.client_order_id}: {e}")
    
    async def _modify_order(self, command: ModifyOrder) -> None:
        """Modify order on Alpaca."""
        # Alpaca doesn't support direct order modification
        # Need to cancel and resubmit
        self._log.warning("Alpaca doesn't support order modification - cancel and resubmit manually")
    
    def _convert_order_to_alpaca(self, order: Order) -> Dict[str, Any]:
        """Convert Nautilus order to Alpaca order format."""
        alpaca_order = {
            "symbol": order.instrument_id.symbol.value,
            "qty": str(order.quantity),
            "side": "buy" if order.side == OrderSide.BUY else "sell",
        }
        
        # Order type conversion
        if isinstance(order, MarketOrder):
            alpaca_order["type"] = "market"
        elif isinstance(order, LimitOrder):
            alpaca_order["type"] = "limit"
            alpaca_order["limit_price"] = str(order.price)
        elif isinstance(order, StopMarketOrder):
            alpaca_order["type"] = "stop"
            alpaca_order["stop_price"] = str(order.trigger_price)
        elif isinstance(order, StopLimitOrder):
            alpaca_order["type"] = "stop_limit"
            alpaca_order["stop_price"] = str(order.trigger_price)
            alpaca_order["limit_price"] = str(order.price)
        else:
            raise ValueError(f"Unsupported order type: {type(order)}")
        
        # Time in force conversion
        tif_map = {
            TimeInForce.GTC: "gtc",
            TimeInForce.IOC: "ioc", 
            TimeInForce.FOK: "fok",
            TimeInForce.DAY: "day",
        }
        alpaca_order["time_in_force"] = tif_map.get(order.time_in_force, "day")
        
        # Add order class if needed
        alpaca_order["order_class"] = "simple"
        
        return alpaca_order
    
    # WebSocket message handlers
    
    async def _handle_account_update(self, data: Dict[str, Any]) -> None:
        """Handle account update from WebSocket."""
        try:
            # Update account state
            account_state = self._create_account_state(data)
            self.generate_account_state(
                account_state=account_state,
                ts_event=self._clock.timestamp_ns(),
            )
            
        except Exception as e:
            self._log.error(f"Error handling account update: {e}")
    
    async def _handle_trade_update(self, data: Dict[str, Any]) -> None:
        """Handle trade update from WebSocket."""
        try:
            # Parse trade update
            event = data.get("event")
            order_data = data.get("order", {})
            
            venue_order_id = VenueOrderId(order_data.get("id"))
            client_order_id = self._client_order_ids.get(venue_order_id)
            
            if not client_order_id:
                self._log.warning(f"Unknown venue order ID: {venue_order_id}")
                return
            
            # Handle different trade events
            if event == "fill":
                await self._handle_order_fill(data, client_order_id, venue_order_id)
            elif event == "canceled":
                await self._handle_order_canceled(data, client_order_id, venue_order_id)
            elif event == "rejected":
                await self._handle_order_rejected(data, client_order_id, venue_order_id)
                
        except Exception as e:
            self._log.error(f"Error handling trade update: {e}")
    
    async def _handle_order_fill(
        self,
        data: Dict[str, Any],
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) -> None:
        """Handle order fill event."""
        order_data = data.get("order", {})
        
        # Parse fill data
        fill_price = Price.from_str(order_data.get("filled_avg_price", "0"))
        fill_qty = Quantity.from_str(order_data.get("filled_qty", "0"))
        
        # Generate order filled event
        self.generate_order_filled(
            strategy_id=None,  # Will be looked up
            instrument_id=None,  # Will be looked up
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            venue_position_id=None,  # Alpaca doesn't use position IDs
            execution_id=ExecutionId(f"{venue_order_id}-{self._clock.timestamp_ns()}"),
            order_side=OrderSide.BUY if order_data.get("side") == "buy" else OrderSide.SELL,
            order_type=OrderType.MARKET,  # Simplified
            last_qty=fill_qty,
            last_px=fill_price,
            currency="USD",
            commission=Money(Decimal("0"), "USD"),  # Alpaca is commission-free
            liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
            ts_event=self._clock.timestamp_ns(),
        )
    
    async def _handle_order_canceled(
        self,
        data: Dict[str, Any],
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) -> None:
        """Handle order canceled event."""
        self.generate_order_canceled(
            strategy_id=None,  # Will be looked up
            instrument_id=None,  # Will be looked up
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            ts_event=self._clock.timestamp_ns(),
        )
    
    async def _handle_order_rejected(
        self,
        data: Dict[str, Any],
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) -> None:
        """Handle order rejected event."""
        reason = data.get("order", {}).get("reject_reason", "Unknown")
        
        self.generate_order_rejected(
            strategy_id=None,  # Will be looked up
            instrument_id=None,  # Will be looked up
            client_order_id=client_order_id,
            reason=reason,
            ts_event=self._clock.timestamp_ns(),
        )