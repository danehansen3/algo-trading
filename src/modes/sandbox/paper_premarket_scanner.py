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


class PaperTradingNode:
    """Paper trading node for testing strategies."""
    
    def __init__(self):
        self.node = None
    
    async def start(self):
        """Start the paper trading node."""
        # Strategy configuration
        strategy_config = PremarketScannerConfig(
            strategy_id="PremarketScanner-001",
            premarket_threshold=0.20,  # 20% threshold
            scan_universe=[  # Popular stocks for testing   - TODO: this should include all tickers, not just popular.
                "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
                "NVDA", "META", "NFLX", "AMD", "CRM"
            ],
            max_positions=10,    
            position_size_usd=5000.0,  # TODO: update to be a dynamic number 
            fast_sma_period=5,
            slow_sma_period=20,
            max_loss_per_trade_pct=0.05,
            daily_loss_limit_usd=2500.0,   # TODO: update to be a dynamic number 
        )
        
        # Trading node configuration
        node_config = TradingNodeConfig(
            trader_id="PAPER-TRADER-OVERNIGHT-GAINERS-LOSERS",
            logging=LoggingConfig(
                log_level="INFO",
                log_file_format="{time} | {level} | {name} | {message}",
                log_to_file=True,
            ),
            
            # Data client (paper trading)
            data_clients={
                ALPACA: AlpacaDataClientConfig(
                    api_key=None,  # Will use ALPACA_PAPER_API_KEY env var
                    api_secret=None,  # Will use ALPACA_PAPER_API_SECRET env var
                    account_type=AlpacaAccountType.PAPER,
                ),
            },
            
            # Execution client (paper trading)
            exec_clients={
                ALPACA: AlpacaExecClientConfig(
                    api_key=None,  # Will use ALPACA_PAPER_API_KEY env var
                    api_secret=None,  # Will use ALPACA_PAPER_API_SECRET env var
                    account_type=AlpacaAccountType.PAPER,
                ),
            },
            
            # Timeout configurations
            timeout_connection=20.0,
            timeout_reconciliation=10.0,
            timeout_portfolio=10.0,
            timeout_disconnection=10.0,
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
        
        print("🚀 Paper trading node started successfully!")
        print("📊 Strategy: Premarket Scanner with SMA Crossover")
        print("💰 Account: Paper Trading")
        print("⏰ Press Ctrl+C to stop...")
    
    async def stop(self):
        """Stop the paper trading node."""
        if self.node:
            await self.node.stop_async()
            print("📴 Paper trading node stopped.")


async def main():
    """Main entry point for paper trading."""
    # Validate environment variables
    if not os.getenv("ALPACA_PAPER_API_KEY") or not os.getenv("ALPACA_PAPER_API_SECRET"):
        print("❌ Missing Alpaca paper trading API credentials!")
        print("Please set ALPACA_PAPER_API_KEY and ALPACA_PAPER_API_SECRET environment variables")
        return
    
    # Start paper trading node
    paper_node = PaperTradingNode()
    
    try:
        await paper_node.start()
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Shutdown requested...")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await paper_node.stop()


if __name__ == "__main__":
    asyncio.run(main())