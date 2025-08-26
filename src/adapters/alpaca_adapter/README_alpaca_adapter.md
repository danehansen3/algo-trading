# Alpaca Adapter for NautilusTrader

This adapter provides integration between NautilusTrader and Alpaca Markets, enabling live trading and data streaming for US equities.

## Features

- **Live Market Data**: Real-time quotes, trades, and bars via Alpaca's WebSocket streams
- **Order Execution**: Full order lifecycle management (submit, modify, cancel)
- **Multiple Order Types**: Market, limit, stop, and stop-limit orders
- **Paper Trading**: Built-in support for Alpaca's paper trading environment
- **Data Feeds**: Support for IEX (free) and SIP (paid) data feeds
- **Instrument Discovery**: Automatic loading and caching of tradeable instruments
- **Position Management**: Real-time position tracking and reporting
- **Rate Limiting**: Built-in rate limiting to comply with Alpaca's API limits

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements_alpaca.txt
```

2. Ensure you have NautilusTrader installed:
```bash
pip install nautilus_trader
```

3. Copy the adapter files to your NautilusTrader installation:
```bash
cp -r nautilus_trader/adapters/alpaca/ /path/to/nautilus_trader/adapters/
```

## Configuration

### API Credentials

You'll need Alpaca API credentials. Get them from:
- Paper Trading: [Alpaca Paper Trading](https://app.alpaca.markets/paper/dashboard/overview)
- Live Trading: [Alpaca Live Trading](https://app.alpaca.markets/live/dashboard/overview)

Set your credentials as environment variables:
```bash
export ALPACA_API_KEY="your_api_key_here"
export ALPACA_API_SECRET="your_api_secret_here"
```

### Basic Configuration

```python
from nautilus_trader.adapters.alpaca.config import AlpacaDataClientConfig, AlpacaExecClientConfig

# Data client configuration
data_config = AlpacaDataClientConfig(
    api_key="your_api_key",
    api_secret="your_api_secret",
    base_url="https://paper-api.alpaca.markets",  # Paper trading
    data_feed="iex",  # Free IEX data feed
    sandbox=True,  # Use paper trading
)

# Execution client configuration
exec_config = AlpacaExecClientConfig(
    api_key="your_api_key",
    api_secret="your_api_secret",
    base_url="https://paper-api.alpaca.markets",  # Paper trading
    sandbox=True,  # Use paper trading
)
```

### Advanced Configuration Options

| Parameter | Description | Default | Options |
|-----------|-------------|---------|---------|
| `api_key` | Alpaca API key | Required | String |
| `api_secret` | Alpaca API secret | Required | String |
| `base_url` | API base URL | `https://paper-api.alpaca.markets` | Paper/Live URLs |
| `data_feed` | Data feed source | `"iex"` | `"iex"`, `"sip"`, `"otc"` |
| `use_raw_data` | Use raw data format | `False` | `True`/`False` |
| `sandbox` | Enable paper trading | `True` | `True`/`False` |

## Usage

### Basic Trading Example

```python
import asyncio
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.adapters.alpaca.config import AlpacaDataClientConfig, AlpacaExecClientConfig

async def main():
    config = TradingNodeConfig(
        trader_id="TRADER-001",
        data_clients={
            "ALPACA": AlpacaDataClientConfig(
                api_key="your_api_key",
                api_secret="your_api_secret",
                sandbox=True,
            )
        },
        exec_clients={
            "ALPACA": AlpacaExecClientConfig(
                api_key="your_api_key", 
                api_secret="your_api_secret",
                sandbox=True,
            )
        },
    )
    
    node = TradingNode(config=config)
    await node.build()
    await node.start()
    
    # Your trading logic here
    
    await node.stop()

asyncio.run(main())
```

### Strategy Implementation

```python
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.orders import MarketOrder

class MyAlpacaStrategy(Strategy):
    def on_start(self):
        # Subscribe to real-time data
        self.subscribe_quote_ticks("AAPL.ALPACA")
    
    def on_quote_tick(self, tick: QuoteTick):
        # Your trading logic
        if self.should_buy(tick):
            order = MarketOrder(
                # ... order parameters
            )
            self.submit