"""
Example configuration and usage of the Alpaca adapter with NautilusTrader.
"""

import asyncio
import os
from decimal import Decimal

from src.adapters.alpaca_adapter.config import AlpacaDataClientConfig
from src.adapters.alpaca_adapter.config import AlpacaExecClientConfig
from src.adapters.alpaca_adapter.factories import AlpacaInstrumentProviderFactory
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.events import PositionOpened
from nautilus_trader.model.events import PositionClosed
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.objects import Quantity


class SimpleAlpacaStrategy(Strategy):
    """
    A simple strategy example for Alpaca trading.
    
    This strategy will:
    1. Subscribe to real-time quotes for AAPL
    2. Place a small market order when certain conditions are met
    3. Log important events
    """
    
    def __init__(self, config=None) -> None:
        super().__init__(config)
        
        # Strategy parameters
        self.instrument_id = InstrumentId(Symbol("AAPL"), Venue("ALPACA"))
        self.trade_size = Quantity.from_int(1)  # 1 share
        
        # State tracking
        self.position_count = 0
        self.max_positions = 3
    
    def on_start(self) -> None:
        """Actions to be performed on strategy start."""
        self.log.info(f"Starting {self.__class__.__name__}")
        
        # Subscribe to real-time quotes for AAPL
        self.subscribe_quote_ticks(self.instrument_id)
        
        # Request initial instrument data
        self.request_instrument(self.instrument_id)
    
    def on_stop(self) -> None:
        """Actions to be performed on strategy stop."""
        self.log.info(f"Stopping {self.__class__.__name__}")
        
        # Cancel any open orders
        self.cancel_all_orders(self.instrument_id)
    
    def on_quote_tick(self, tick: QuoteTick) -> None:
        """
        Handle quote tick updates.
        
        Parameters
        ----------
        tick : QuoteTick
            The quote tick received.
        """
        self.log.info(
            f"Received quote for {tick.instrument_id}: "
            f"bid={tick.bid_price} ask={tick.ask_price}"
        )
        
        # Simple trading logic: buy if we don't have max positions
        if self.position_count < self.max_positions:
            spread = tick.ask_price - tick.bid_price
            
            # Only trade if spread is reasonable (less than $0.10)
            if spread.as_decimal() < Decimal("0.10"):
                self._place_market_order(OrderSide.BUY)
    
    def on_position_opened(self, event: PositionOpened) -> None:
        """Handle position opened events."""
        self.position_count += 1
        self.log.info(
            f"Position opened: {event.position_id} "
            f"({self.position_count}/{self.max_positions})"
        )
    
    def on_position_closed(self, event: PositionClosed) -> None:
        """Handle position closed events."""
        self.position_count = max(0, self.position_count - 1)
        self.log.info(
            f"Position closed: {event.position_id} "
            f"PnL: {event.position.realized_pnl} "
            f"({self.position_count}/{self.max_positions})"
        )
    
    def _place_market_order(self, side: OrderSide) -> None:
        """Place a market order."""
        order = MarketOrder(
            trader_id=self.trader_id,
            strategy_id=self.id,
            instrument_id=self.instrument_id,
            order_side=side,
            quantity=self.trade_size,
        )
        
        self.submit_order(order)
        self.log.info(f"Submitted {side.name} market order for {self.trade_size} shares")


async def main():
    """Main function to run the trading example."""
    
    # Get Alpaca credentials from environment variables
    api_key = os.getenv("ALPACA_API_KEY")
    api_secret = os.getenv("ALPACA_API_SECRET")
    
    if not api_key or not api_secret:
        raise ValueError(
            "Please set ALPACA_API_KEY and ALPACA_API_SECRET environment variables"
        )
    
    # Configure the trading node
    config = TradingNodeConfig(
        trader_id=TraderId("ALPACA-TRADER-001"),
        logging=LoggingConfig(log_level="INFO"),
        
        # Data client configuration
        data_clients={
            "ALPACA": AlpacaDataClientConfig(
                api_key=api_key,
                api_secret=api_secret,
                base_url="https://paper-api.alpaca.markets",  # Paper trading
                data_feed="iex",  # Free IEX data feed
                use_raw_data=False,
                sandbox=True,  # Use paper trading
            )
        },
        
        # Execution client configuration  
        exec_clients={
            "ALPACA": AlpacaExecClientConfig(
                api_key=api_key,
                api_secret=api_secret,
                base_url="https://paper-api.alpaca.markets",  # Paper trading
                use_raw_data=False,
                sandbox=True,  # Use paper trading
            )
        },
        
        # Instrument provider configuration
        data_clients_config={
            "ALPACA": InstrumentProviderConfig(
                load_all=False,  # Don't load all instruments initially
                load_ids=[
                    "AAPL.ALPACA",
                    "TSLA.ALPACA", 
                    "MSFT.ALPACA",
                ],  # Load specific instruments
                filters={
                    "asset_class": "us_equity",
                    "status": "active",
                },
            )
        },
        
        # Add strategies
        strategies=[
            {
                "strategy_id": StrategyId("SimpleAlpacaStrategy-001"),
                "strategy_class": SimpleAlpacaStrategy,
                "config": {},
            }
        ],
    )
    
    # Create and start the trading node
    node = TradingNode(config=config)
    
    try:
        await node.build()
        await node.start()
        
        print("Trading node started successfully!")
        print("Press Ctrl+C to stop...")
        
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        await node.stop()
        await node.dispose()


# Alternative configuration for backtesting preparation
def create_backtest_config():
    """Create configuration for backtesting with historical Alpaca data."""
    
    # This would be used with NautilusTrader's backtesting engine
    # using historical data from Alpaca
    
    from nautilus_trader.config import BacktestEngineConfig
    from nautilus_trader.config import BacktestDataConfig
    from nautilus_trader.config import BacktestVenueConfig
    from nautilus_trader.config import BacktestRunConfig
    
    # Configure venues
    venues_config = [
        BacktestVenueConfig(
            name="ALPACA",
            oms_type="HEDGING",  # Alpaca supports hedging
            account_type="MARGIN",  # Margin account
            base_currency="USD",
            starting_balances=["100000 USD"],  # $100k starting balance
        )
    ]
    
    # Configure data
    data_config = [
        BacktestDataConfig(
            catalog_path="./data/alpaca_catalog",  # Path to your data catalog
            data_cls="nautilus_trader.model.data.QuoteTick",
            instrument_id="AAPL.ALPACA",
            start_time="2024-01-01",
            end_time="2024-12-31",
        )
    ]
    
    # Configure engine
    engine_config = BacktestEngineConfig(
        strategies=[
            {
                "strategy_id": StrategyId("SimpleAlpacaStrategy-BACKTEST"),
                "strategy_class": SimpleAlpacaStrategy,
                "config": {},
            }
        ]
    )
    
    return BacktestRunConfig(
        engine=engine_config,
        venues=venues_config,
        data=data_config,
    )


if __name__ == "__main__":
    # Example of setting up environment variables (don't commit real keys!)
    # os.environ["ALPACA_API_KEY"] = "your_api_key_here"
    # os.environ["ALPACA_API_SECRET"] = "your_api_secret_here"
    
    # Run the live trading example
    asyncio.run(main())
    
    # Or run backtesting (commented out)
    # backtest_config = create_backtest_config()
    # engine = BacktestEngine(config=backtest_config.engine)
    # results = engine.run(config=backtest_config)