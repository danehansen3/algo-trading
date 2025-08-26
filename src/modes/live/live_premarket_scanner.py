import asyncio
import os
from pathlib import Path

from src.adapters.alpaca_adapter import ALPACA
from src.adapters.alpaca_adapter import AlpacaAccountType
from src.adapters.alpaca_adapter import AlpacaDataClientConfig
from src.adapters.alpaca_adapter import AlpacaExecClientConfig
from src.adapters.alpaca_adapter import AlpacaLiveDataClientFactory
from src.adapters.alpaca_adapter import AlpacaLiveExecClientFactory
from nautilus_trader.config import TradingNodeConfig, LoggingConfig
from nautilus_trader.live.node import TradingNode

# Import your strategy
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))
from strategy.premarket_scanner import PremarketScannerStrategy, PremarketScannerConfig


class LiveTradingNode:
    """Live trading node for production trading."""
    
    def __init__(self):
        self.node = None
    
    async def start(self):
        """Start the live trading node."""
        # Strategy configuration (more conservative for live trading)
        strategy_config = PremarketScannerConfig(
            strategy_id="PremarketScanner-LIVE-001",
            premarket_threshold=0.25,  # Higher threshold for live trading
            scan_universe=[  # Curated list of liquid stocks
                "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
                "NVDA", "META", "JPM", "JNJ", "PG"
            ],
            max_positions=3,  # Conservative position limit
            position_size_usd=2500.0,  # Conservative position size
            fast_sma_period=5,
            slow_sma_period=20,
            max_loss_per_trade_pct=0.03,  # Tighter stop loss
            daily_loss_limit_usd=1000.0,  # Conservative daily limit
        )
        
        # Trading node configuration
        node_config = TradingNodeConfig(
            trader_id="LIVE-TRADER-001",
            logging=LoggingConfig(
                log_level="INFO",
                log_file_format="{time} | {level} | {name} | {message}",
                log_to_file=True,
                log_file_path="logs/live_trading.log",
            ),
            
            # Data client (live trading)
            data_clients={
                ALPACA: AlpacaDataClientConfig(
                    api_key=None,  # Will use ALPACA_LIVE_API_KEY env var
                    api_secret=None,  # Will use ALPACA_LIVE_API_SECRET env var
                    account_type=AlpacaAccountType.LIVE,
                ),
            },
            
            # Execution client (live trading)
            exec_clients={
                ALPACA: AlpacaExecClientConfig(
                    api_key=None,  # Will use ALPACA_LIVE_API_KEY env var
                    api_secret=None,  # Will use ALPACA_LIVE_API_SECRET env var
                    account_type=AlpacaAccountType.LIVE,
                ),
            },
            
            # Timeout configurations
            timeout_connection=30.0,  # Longer timeouts for live trading
            timeout_reconciliation=15.0,
            timeout_portfolio=15.0,
            timeout_disconnection=15.0,
        )
        
        # Create trading node
        self.node = TradingNode(config=node_config)
        
        # Add client factories
        self.node.add_data_client_factory(ALPACA, AlpacaLiveDataClientFactory)
        self.node.add_exec_client_factory(ALPACA, AlpacaLiveExecClientFactory)
        
        # Add strategy
        self.node.trader.add_strategy(PremarketScannerStrategy(strategy_config))
        
        # Build and start
        self.node.build()
        await self.node.start_async()
        
        print("🔥 LIVE trading node started successfully!")
        print("📊 Strategy: Premarket Scanner with SMA Crossover")  
        print("💰 Account: LIVE TRADING - REAL MONEY!")
        print("⚠️  Please monitor closely...")
        print("⏰ Press Ctrl+C to stop...")
    
    async def stop(self):
        """Stop the live trading node."""
        if self.node:
            await self.node.stop_async()
            print("📴 Live trading node stopped.")


async def main():
    """Main entry point for live trading."""
    # Validate environment variables
    if not os.getenv("ALPACA_LIVE_API_KEY") or not os.getenv("ALPACA_LIVE_API_SECRET"):
        print("❌ Missing Alpaca live trading API credentials!")
        print("Please set ALPACA_LIVE_API_KEY and ALPACA_LIVE_API_SECRET environment variables")
        return
    
    # Confirmation prompt for live trading
    print("⚠️  WARNING: This is LIVE TRADING with REAL MONEY!")
    print("Are you sure you want to continue? (type 'YES' to confirm)")
    confirmation = input().strip()
    
    if confirmation != "YES":
        print("❌ Live trading cancelled.")
        return
    
    # Start live trading node
    live_node = LiveTradingNode()
    
    try:
        await live_node.start()
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Shutdown requested...")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await live_node.stop()


if __name__ == "__main__":
    asyncio.run(main())