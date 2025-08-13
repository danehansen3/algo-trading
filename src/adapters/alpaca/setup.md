"""
# Alpaca Adapter for Nautilus Trader - Setup Guide

## Installation

1. Install Nautilus Trader with the Alpaca adapter:
```bash
pip install nautilus_trader[alpaca]
```

2. Or if building from source, ensure the alpaca extra dependencies are included:
```bash
pip install -e .[alpaca]
```

## Required Dependencies

The Alpaca adapter requires the following additional packages:
- `aiohttp` >= 3.8.0 - For async HTTP requests
- `websockets` >= 10.0 - For WebSocket connections

## API Credentials Setup

### 1. Create Alpaca Account
- Sign up at https://alpaca.markets/ 
- Complete account verification
- Generate API keys from the dashboard

### 2. Environment Variables
Set your API credentials as environment variables:

```bash
export ALPACA_API_KEY="your_api_key_here"
export ALPACA_API_SECRET="your_secret_key_here"
```

### 3. Paper Trading vs Live Trading
- For paper trading: Use AlpacaAccountType.PAPER (recommended for testing)
- For live trading: Use AlpacaAccountType.LIVE (requires funded account)

## Configuration Options

### Data Client Configuration

```python
from nautilus_trader.adapters.alpaca import AlpacaDataClientConfig, AlpacaAccountType

config = AlpacaDataClientConfig(
    api_key=None,  # Uses ALPACA_API_KEY env var
    api_secret=None,  # Uses ALPACA_API_SECRET env var
    account_type=AlpacaAccountType.PAPER,
    base_url_http=None,  # Uses default Alpaca URLs
    base_url_ws=None,
)
```

### Execution Client Configuration

```python
from nautilus_trader.adapters.alpaca import AlpacaExecClientConfig

config = AlpacaExecClientConfig(
    api_key=None,  # Uses ALPACA_API_KEY env var
    api_secret=None,  # Uses ALPACA_API_SECRET env var 
    account_type=AlpacaAccountType.PAPER,
    base_url_http=None,  # Uses default Alpaca URLs
    base_url_ws=None,
)
```

## Supported Features

### Asset Classes
- ✅ US Equities (stocks)
- ✅ Crypto (Bitcoin, Ethereum, etc.)
- ❌ Options (not yet implemented)
- ❌ Forex (not available on Alpaca)

### Order Types
- ✅ Market orders
- ✅ Limit orders  
- ✅ Stop orders (stop-loss)
- ✅ Stop-limit orders
- ✅ Trailing stop orders
- ❌ Bracket orders (planned)
- ❌ OCO orders (planned)

### Time in Force
- ✅ DAY - Good for day
- ✅ GTC - Good till canceled
- ✅ IOC - Immediate or cancel
- ✅ FOK - Fill or kill

### Data Types
- ✅ Real-time quotes
- ✅ Real-time trades
- ✅ Historical bars (1min, 5min, 1hour, 1day)
- ✅ Account updates
- ✅ Position updates
- ✅ Order status updates

## Usage Examples

### Basic Trading Node Setup

```python
import asyncio
from nautilus_trader.adapters.alpaca import ALPACA
from nautilus_trader.adapters.alpaca import AlpacaAccountType
from nautilus_trader.adapters.alpaca import AlpacaDataClientConfig
from nautilus_trader.adapters.alpaca import AlpacaExecClientConfig
from nautilus_trader.adapters.alpaca import AlpacaLiveDataClientFactory
from nautilus_trader.adapters.alpaca import AlpacaLiveExecClientFactory
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode

async def main():
    # Configure node
    config = TradingNodeConfig(
        trader_id="TRADER-001",
        data_clients={
            ALPACA: AlpacaDataClientConfig(
                account_type=AlpacaAccountType.PAPER
            ),
        },
        exec_clients={
            ALPACA: AlpacaExecClientConfig(
                account_type=AlpacaAccountType.PAPER
            ),
        },
    )
    
    # Create and build node
    node = TradingNode(config=config)
    node.add_data_client_factory(ALPACA, AlpacaLiveDataClientFactory)
    node.add_exec_client_factory(ALPACA, AlpacaLiveExecClientFactory)
    node.build()
    
    # Start trading
    await node.start_async()
    # ... your trading logic here ...
    await node.stop_async()

if __name__ == "__main__":
    asyncio.run(main())
```

### Strategy with Market Data

```python
from nautilus_trader.model.data import Bar
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

class MyAlpacaStrategy(Strategy):
    def __init__(self, config):
        super().__init__(config)
        self.instrument_id = InstrumentId.from_str("AAPL.ALPACA")
        
    def on_start(self):
        # Subscribe to 1-minute bars
        bar_type = BarType.from_str("AAPL.ALPACA-1-MINUTE-LAST-EXTERNAL")
        self.subscribe_bars(bar_type)
        
    def on_bar(self, bar: Bar):
        # Your trading logic here
        self.log.info(f"Received bar: {bar}")
```

## Limitations and Known Issues

1. **Position Management**: Alpaca uses a netting position model (not hedging)
2. **Order Modifications**: Alpaca doesn't support direct order modifications - orders must be canceled and resubmitted
3. **Crypto Trading**: Limited to major cryptocurrencies available on Alpaca
4. **Market Hours**: US equity trading limited to market hours and extended hours sessions
5. **Rate Limits**: Subject to Alpaca's API rate limits

## Testing

### Paper Trading
Always test your strategies with paper trading first:

```python
config = AlpacaDataClientConfig(
    account_type=AlpacaAccountType.PAPER  # Safe for testing
)
```

### Backtesting
For backtesting, use Nautilus Trader's built-in backtesting engine with historical data from Alpaca or other providers.

## Support

For issues specific to the Alpaca adapter:
1. Check the Nautilus Trader documentation
2. Review Alpaca's API documentation
3. Submit issues on the Nautilus Trader GitHub repository

## Security Best Practices

1. **Never hardcode API keys** - always use environment variables
2. **Use paper trading** for development and testing
3. **Set appropriate position sizing** to manage risk
4. **Monitor your strategies** closely when running live
5. **Keep API keys secure** and rotate them regularly

## Next Steps

After setting up the adapter:
1. Test connectivity with paper trading
2. Develop and backtest your strategies
3. Paper trade your strategies to validate behavior
4. Carefully transition to live trading with small position sizes
5. Scale up gradually as you gain confidence
"""