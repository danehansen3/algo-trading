"""
Alpaca execution client implementation.
"""

import asyncio
from decimal import Decimal
from typing import Any
from uuid import uuid4

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from alpaca.trading.models import Order as AlpacaOrder
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest, StopOrderRequest, StopLimitOrderRequest
from alpaca.trading.stream import TradingStream

from alpaca_adapter.config import AlpacaExecClientConfig
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide as NautilusOrderSide
from nautilus_trader.model.enums import OrderStatus as NautilusOrderStatus
from nautilus_trader.model.enums import OrderType as NautilusOrderType
from nautilus_trader.model.enums import TimeInForce as NautilusTimeInForce
from nautilus_trader.model.events import OrderAccepted
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.events import OrderSubmitted
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class AlpacaExecutionClient(LiveExecutionClient):
    """
    Provides an execution client for Alpaca Markets.
    
    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    client : AlpacaInstrumentProvider
        The Alpaca instrument provider.
    account : Account
        The account for the client.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    config : AlpacaExecClientConfig
        The configuration for the client.
    """
    
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client,
        account,
        msgbus,
        cache,
        clock,
        config: AlpacaExecClientConfig,
    ) -> None:
        PyCondition.not_none(config, "config")
        
        super().__init__(
            loop=loop,
            client=client,
            account=account,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            config=config,
        )
        
        self._config = config
        self._venue = Venue("ALPACA")
        
        # Initialize Alpaca clients
        self._trading_client = TradingClient(
            api_key=config.api_key,
            secret_key=config.api_secret,
            paper=config.sandbox,
        )
        
        self._trading_stream = TradingStream(
            api_key=config.api_key,
            secret_key=config.api_secret,
            paper=config.sandbox,
            raw_data=config.use_raw_data,
        )
        
        # Order tracking
        self._client_order_id_to_venue: dict[ClientOrderId, VenueOrderId] = {}
        self._venue_order_id_to_client: dict[VenueOrderId, ClientOrderId] = {}
        
        self._setup_stream_handlers()
    
    def _setup_stream_handlers(self) -> None:
        """Set up trading stream event handlers."""
        
        async def trade_update_handler(data) -> None:
            """Handle trade update events."""
            try:
                await self._handle_trade_update(data)
            except Exception as e:
                self._log.error(f"Error processing trade update: {e}")
        
        # Assign handler
        self._trading_stream.subscribe_trade_updates(trade_update_handler)
    
    # Connection management
    async def _connect(self) -> None:
        """Connect to Alpaca trading APIs."""
        self._log.info("Connecting to Alpaca trading APIs...")
        
        try:
            # Authenticate and get account information
            account_info = await self._loop.run_in_executor(
                None, 
                self._trading_client.get_account
            )
            
            self._log.info(
                f"Connected to Alpaca trading APIs. Account: {account_info.id}",
                LogColor.GREEN,
            )
            
            # Start trading stream for order updates
            await self._trading_stream._run_forever()
            
        except Exception as e:
            self._log.error(f"Failed to connect to Alpaca trading APIs: {e}")
            raise
    
    async def _disconnect(self) -> None:
        """Disconnect from Alpaca trading APIs."""
        self._log.info("Disconnecting from Alpaca trading APIs...")
        
        try:
            await self._trading_stream._stop()
            self._log.info("Disconnected from Alpaca trading APIs")
        except Exception as e:
            self._log.error(f"Error disconnecting from Alpaca trading APIs: {e}")
    
    # Order management
    async def _submit_order(self, command: SubmitOrder) -> None:
        """Submit an order to Alpaca."""
        try:
            # Convert Nautilus order to Alpaca order request
            order_request = self._create_alpaca_order_request(command.order)
            
            # Submit order
            alpaca_order = await self._loop.run_in_executor(
                None,
                lambda: self._trading_client.submit_order(order_request)
            )
            
            # Track order ID mapping
            venue_order_id = VenueOrderId(str(alpaca_order.id))
            self._client_order_id_to_venue[command.order.client_order_id] = venue_order_id
            self._venue_order_id_to_client[venue_order_id] = command.order.client_order_id
            
            # Generate OrderSubmitted event
            self._generate_order_submitted(
                strategy_id=command.strategy_id,
                instrument_id=command.order.instrument_id,
                client_order_id=command.order.client_order_id,
                ts_event=self._clock.timestamp_ns(),
            )
            
            # Generate OrderAccepted event if order is immediately accepted
            if alpaca_order.status in [OrderStatus.NEW, OrderStatus.ACCEPTED]:
                self._generate_order_accepted(
                    strategy_id=command.strategy_id,
                    instrument_id=command.order.instrument_id,
                    client_order_id=command.order.client_order_id,
                    venue_order_id=venue_order_id,
                    ts_event=self._clock.timestamp_ns(),
                )
            
        except Exception as e:
            self._log.error(f"Failed to submit order: {e}")
            
            # Generate OrderRejected event
            self._generate_order_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.order.instrument_id,
                client_order_id=command.order.client_order_id,
                reason=str(e),
                ts_event=self._clock.timestamp_ns(),
            )
    
    async def _modify_order(self, command: ModifyOrder) -> None:
        """Modify an existing order."""
        try:
            venue_order_id = self._client_order_id_to_venue.get(command.client_order_id)
            if not venue_order_id:
                self._log.error(f"Cannot find venue order ID for {command.client_order_id}")
                return
            
            # Create modification request
            modify_request = self._create_modify_request(command)
            
            # Replace order (Alpaca uses replace instead of modify)
            await self._loop.run_in_executor(
                None,
                lambda: self._trading_client.replace_order_by_id(
                    order_id=venue_order_id.value,
                    order_data=modify_request
                )
            )
            
            self._log.info(f"Modified order {venue_order_id}")
            
        except Exception as e:
            self._log.error(f"Failed to modify order: {e}")
    
    async def _cancel_order(self, command: CancelOrder) -> None:
        """Cancel a specific order."""
        try:
            venue_order_id = self._client_order_id_to_venue.get(command.client_order_id)
            if not venue_order_id:
                self._log.error(f"Cannot find venue order ID for {command.client_order_id}")
                return
            
            # Cancel order
            await self._loop.run_in_executor(
                None,
                lambda: self._trading_client.cancel_order_by_id(venue_order_id.value)
            )
            
            # Generate OrderCanceled event
            self._generate_order_canceled(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=venue_order_id,
                ts_event=self._clock.timestamp_ns(),
            )
            
        except Exception as e:
            self._log.error(f"Failed to cancel order: {e}")
    
    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        """Cancel all orders for an instrument."""
        try:
            # Get all open orders
            open_orders = await self._loop.run_in_executor(
                None,
                lambda: self._trading_client.get_orders(
                    status="open",
                    symbols=[command.instrument_id.symbol.value] if command.instrument_id else None
                )
            )
            
            # Cancel each order
            for order in open_orders:
                try:
                    await self._loop.run_in_executor(
                        None,
                        lambda: self._trading_client.cancel_order_by_id(order.id)
                    )
                    
                    # Generate OrderCanceled event
                    client_order_id = self._venue_order_id_to_client.get(VenueOrderId(str(order.id)))
                    if client_order_id:
                        self._generate_order_canceled(
                            strategy_id=command.strategy_id,
                            instrument_id=InstrumentId(Symbol(order.symbol), self._venue),
                            client_order_id=client_order_id,
                            venue_order_id=VenueOrderId(str(order.id)),
                            ts_event=self._clock.timestamp_ns(),
                        )
                        
                except Exception as order_error:
                    self._log.error(f"Failed to cancel order {order.id}: {order_error}")
            
        except Exception as e:
            self._log.error(f"Failed to cancel all orders: {e}")
    
    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        """Cancel a batch of orders."""
        for cancel_order in command.cancels:
            await self._cancel_order(cancel_order)
    
    # Report generation
    async def generate_order_status_report(
        self, 
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId | None = None,
        venue_order_id: VenueOrderId | None = None,
    ) -> OrderStatusReport | None:
        """Generate an order status report."""
        try:
            if venue_order_id:
                order_id = venue_order_id.value
            elif client_order_id:
                venue_id = self._client_order_id_to_venue.get(client_order_id)
                if not venue_id:
                    return None
                order_id = venue_id.value
            else:
                return None
            
            # Get order from Alpaca
            alpaca_order = await self._loop.run_in_executor(
                None,
                lambda: self._trading_client.get_order_by_id(order_id)
            )
            
            return self._create_order_status_report(alpaca_order, instrument_id)
            
        except Exception as e:
            self._log.error(f"Failed to generate order status report: {e}")
            return None
    
    async def generate_order_status_reports(
        self,
        instrument_id: InstrumentId | None = None,
        start = None,
        end = None,
        open_only: bool = False,
    ) -> list[OrderStatusReport]:
        """Generate multiple order status reports."""
        try:
            status_filter = "open" if open_only else None
            symbol_filter = [instrument_id.symbol.value] if instrument_id else None
            
            # Get orders from Alpaca
            orders = await self._loop.run_in_executor(
                None,
                lambda: self._trading_client.get_orders(
                    status=status_filter,
                    symbols=symbol_filter,
                    after=start,
                    until=end,
                )
            )
            
            reports = []
            for order in orders:
                instr_id = InstrumentId(Symbol(order.symbol), self._venue)
                report = self._create_order_status_report(order, instr_id)
                if report:
                    reports.append(report)
            
            return reports
            
        except Exception as e:
            self._log.error(f"Failed to generate order status reports: {e}")
            return []
    
    async def generate_fill_reports(
        self,
        instrument_id: InstrumentId | None = None,
        venue_order_id: VenueOrderId | None = None,
        start = None,
        end = None,
    ) -> list[FillReport]:
        """Generate fill reports."""
        # Alpaca doesn't provide separate fill API, fills are part of order status
        # This would need to be implemented by tracking order updates
        return []
    
    async def generate_position_status_reports(
        self,
        instrument_id: InstrumentId | None = None,
        start = None,
        end = None,
    ) -> list[PositionStatusReport]:
        """Generate position status reports."""
        try:
            # Get positions from Alpaca
            positions = await self._loop.run_in_executor(
                None,
                self._trading_client.get_all_positions
            )
            
            reports = []
            for position in positions:
                if instrument_id and position.symbol != instrument_id.symbol.value:
                    continue
                    
                report = self._create_position_status_report(position)
                if report:
                    reports.append(report)
            
            return reports
            
        except Exception as e:
            self._log.error(f"Failed to generate position status reports: {e}")
            return []
    
    # Helper methods
    def _create_alpaca_order_request(self, order) -> Any:
        """Create an Alpaca order request from a Nautilus order."""
        symbol = order.instrument_id.symbol.value
        side = OrderSide.BUY if order.side == NautilusOrderSide.BUY else OrderSide.SELL
        qty = int(order.quantity.as_decimal())
        
        # Convert time in force
        tif_map = {
            NautilusTimeInForce.DAY: TimeInForce.DAY,
            NautilusTimeInForce.GTC: TimeInForce.GTC,
            NautilusTimeInForce.IOC: TimeInForce.IOC,
            NautilusTimeInForce.FOK: TimeInForce.FOK,
        }
        time_in_force = tif_map.get(order.time_in_force, TimeInForce.DAY)
        
        # Create appropriate order request based on order type
        if order.order_type == NautilusOrderType.MARKET:
            return MarketOrderRequest(
                symbol=symbol,
                side=side,
                qty=qty,
                time_in_force=time_in_force,
            )
        
        elif order.order_type == NautilusOrderType.LIMIT:
            return LimitOrderRequest(
                symbol=symbol,
                side=side,
                qty=qty,
                limit_price=float(order.price.as_decimal()),
                time_in_force=time_in_force,
            )
        
        elif order.order_type == NautilusOrderType.STOP_MARKET:
            return StopOrderRequest(
                symbol=symbol,
                side=side,
                qty=qty,
                stop_price=float(order.trigger_price.as_decimal()),
                time_in_force=time_in_force,
            )
        
        elif order.order_type == NautilusOrderType.STOP_LIMIT:
            return StopLimitOrderRequest(
                symbol=symbol,
                side=side,
                qty=qty,
                limit_price=float(order.price.as_decimal()),
                stop_price=float(order.trigger_price.as_decimal()),
                time_in_force=time_in_force,
            )
        
        else:
            raise ValueError(f"Unsupported order type: {order.order_type}")
    
    def _create_modify_request(self, command: ModifyOrder) -> Any:
        """Create a modify request from a ModifyOrder command."""
        # Alpaca uses replace orders instead of modify
        # This would need to recreate the order with new parameters
        # Implementation depends on what fields are being modified
        pass
    
    def _create_order_status_report(self, alpaca_order: AlpacaOrder, instrument_id: InstrumentId) -> OrderStatusReport | None:
        """Create an order status report from an Alpaca order."""
        try:
            # Map Alpaca order status to Nautilus status
            status_map = {
                OrderStatus.NEW: NautilusOrderStatus.ACCEPTED,
                OrderStatus.ACCEPTED: NautilusOrderStatus.ACCEPTED,
                OrderStatus.PARTIALLY_FILLED: NautilusOrderStatus.PARTIALLY_FILLED,
                OrderStatus.FILLED: NautilusOrderStatus.FILLED,
                OrderStatus.CANCELED: NautilusOrderStatus.CANCELED,
                OrderStatus.REJECTED: NautilusOrderStatus.REJECTED,
                OrderStatus.EXPIRED: NautilusOrderStatus.EXPIRED,
            }
            
            venue_order_id = VenueOrderId(str(alpaca_order.id))
            client_order_id = self._venue_order_id_to_client.get(venue_order_id)
            
            if not client_order_id:
                # Generate a new client order ID for orders not tracked
                client_order_id = ClientOrderId(str(uuid4()))
                self._venue_order_id_to_client[venue_order_id] = client_order_id
                self._client_order_id_to_venue[client_order_id] = venue_order_id
            
            return OrderStatusReport(
                account_id=AccountId(f"ALPACA-{self._config.api_key[:8]}"),
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                order_side=NautilusOrderSide.BUY if alpaca_order.side == OrderSide.BUY else NautilusOrderSide.SELL,
                order_type=self._map_order_type(alpaca_order.order_type),
                time_in_force=self._map_time_in_force(alpaca_order.time_in_force),
                order_status=status_map.get(alpaca_order.status, NautilusOrderStatus.REJECTED),
                quantity=Quantity.from_int(int(alpaca_order.qty)),
                filled_qty=Quantity.from_int(int(alpaca_order.filled_qty or 0)),
                price=Price.from_str(str(alpaca_order.limit_price)) if alpaca_order.limit_price else None,
                avg_px=Price.from_str(str(alpaca_order.filled_avg_price)) if alpaca_order.filled_avg_price else None,
                report_id=UUID4(),
                ts_accepted=self._clock.timestamp_ns(),
                ts_last=self._clock.timestamp_ns(),
                ts_init=self._clock.timestamp_ns(),
            )
            
        except Exception as e:
            self._log.error(f"Failed to create order status report: {e}")
            return None
    
    def _create_position_status_report(self, alpaca_position) -> PositionStatusReport | None:
        """Create a position status report from an Alpaca position."""
        try:
            return PositionStatusReport(
                account_id=AccountId(f"ALPACA-{self._config.api_key[:8]}"),
                instrument_id=InstrumentId(Symbol(alpaca_position.symbol), self._venue),
                position_side=self._map_position_side(alpaca_position.side),
                quantity=Quantity.from_str(str(abs(float(alpaca_position.qty)))),
                signed_qty=float(alpaca_position.qty),
                report_id=UUID4(),
                ts_last=self._clock.timestamp_ns(),
                ts_init=self._clock.timestamp_ns(),
            )
            
        except Exception as e:
            self._log.error(f"Failed to create position status report: {e}")
            return None
    
    def _map_order_type(self, alpaca_order_type) -> NautilusOrderType:
        """Map Alpaca order type to Nautilus order type."""
        type_map = {
            OrderType.MARKET: NautilusOrderType.MARKET,
            OrderType.LIMIT: NautilusOrderType.LIMIT,
            OrderType.STOP: NautilusOrderType.STOP_MARKET,
            OrderType.STOP_LIMIT: NautilusOrderType.STOP_LIMIT,
        }
        return type_map.get(alpaca_order_type, NautilusOrderType.MARKET)
    
    def _map_time_in_force(self, alpaca_tif) -> NautilusTimeInForce:
        """Map Alpaca time in force to Nautilus time in force."""
        tif_map = {
            TimeInForce.DAY: NautilusTimeInForce.DAY,
            TimeInForce.GTC: NautilusTimeInForce.GTC,
            TimeInForce.IOC: NautilusTimeInForce.IOC,
            TimeInForce.FOK: NautilusTimeInForce.FOK,
        }
        return tif_map.get(alpaca_tif, NautilusTimeInForce.DAY)
    
    def _map_position_side(self, alpaca_side) -> str:
        """Map Alpaca position side to Nautilus position side."""
        # This would need to be implemented based on Alpaca's position side values
        return "LONG" if float(alpaca_side) > 0 else "SHORT"
    
    async def _handle_trade_update(self, data) -> None:
        """Handle trade update events from Alpaca stream."""
        try:
            # Parse trade update and generate appropriate events
            # This would handle order fills, cancellations, rejections, etc.
            pass
        except Exception as e:
            self._log.error(f"Error handling trade update: {e}")