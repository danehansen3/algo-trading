import asyncio
from typing import Dict, List, Set, Optional
from datetime import datetime, time, timedelta
from decimal import Decimal

from nautilus_trader.model.data import Bar, TradeTick, QuoteTick
from nautilus_trader.model.identifiers import InstrumentId, StrategyId
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.enums import OrderSide, BarType, PriceType, OMSType
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.indicators.average.sma import SimpleMovingAverage
from nautilus_trader.config import StrategyConfig
from nautilus_trader.common import Logger


class PremarketScannerConfig(StrategyConfig, frozen=True):
    """Configuration for Premarket Scanner Strategy."""
    
    # Scanner parameters
    premarket_threshold: float = 0.20  # 20% gain/loss threshold
    scan_universe: List[str] = []  # List of symbols to scan (empty = scan all)
    max_positions: int = 10  # Maximum concurrent positions
    position_size_usd: float = 10000.0  # Position size in USD - TODO: update risk module so this isn't an absolute number
    
    # SMA parameters
    fast_sma_period: int = 5
    slow_sma_period: int = 20
    
    # Timing parameters
    premarket_start_time: str = "04:00:00"  # EST
    premarket_end_time: str = "09:30:00"    # EST
    market_close_time: str = "16:00:00"     # EST
    
    # Risk parameters
    max_loss_per_trade_pct: float = 0.05  # 5% max loss per trade
    daily_loss_limit_usd: float = 5000.0  # Daily loss limit - TODO: update risk module so this isn't an absolute number


class PremarketScannerStrategy(Strategy):
    """
    Premarket Scanner Strategy with SMA Crossover.
    
    Strategy Logic:
    1. During premarket, scan for stocks with >20% overnight moves
    2. Categorize into gainers and losers lists
    3. Enter long positions on gainers when fast SMA crosses above slow SMA
    4. Enter short positions on losers when fast SMA crosses below slow SMA
    5. Exit when SMAs cross back in opposite direction
    """
    
    def __init__(self, config: PremarketScannerConfig) -> None:
        super().__init__(config)
        
        # Store configuration
        self.config = config
        
        # Scanner state
        self.gainers: Set[str] = set()
        self.losers: Set[str] = set()
        self.scanned_symbols: Set[str] = set()
        self.scan_complete = False
        
        # Position tracking
        self.active_positions: Dict[InstrumentId, str] = {}  # instrument_id -> "long" or "short"
        self.position_entry_prices: Dict[InstrumentId, Price] = {}
        
        # Technical indicators - organized by symbol
        self.fast_smas: Dict[str, SimpleMovingAverage] = {}
        self.slow_smas: Dict[str, SimpleMovingAverage] = {}
        self.previous_fast_sma: Dict[str, float] = {}
        self.previous_slow_sma: Dict[str, float] = {}
        
        # Instruments cache
        self.instruments: Dict[str, Equity] = {}
        
        # Daily P&L tracking
        self.daily_pnl = Decimal("0.0")
        self.trade_count = 0
        
        # Bar types for each symbol we're monitoring
        self.bar_types: Dict[str, BarType] = {}
        
        # Market timing
        self.premarket_scanning_active = False
        self.market_open = False
        
    def on_start(self) -> None:
        """Called when the strategy is started."""
        self.log.info("Starting Premarket Scanner Strategy")
        
        # Schedule premarket scanning
        self._schedule_premarket_scan()
        
        # Subscribe to market data for initial universe
        if self.config.scan_universe:
            self._setup_initial_subscriptions()
        else:
            # If no specific universe, we'll need to get all tradable symbols
            self._request_all_instruments()
            
        self.log.info("Premarket Scanner Strategy started")
    
    def on_stop(self) -> None:
        """Called when the strategy is stopped."""
        # Close all open positions
        for instrument_id in list(self.active_positions.keys()):
            self._close_position(instrument_id, "Strategy stopping")
            
        self.log.info("Premarket Scanner Strategy stopped")
    
    def _schedule_premarket_scan(self) -> None:
        """Schedule the premarket scanning process."""
        # This would typically use the clock's timer functionality
        # For now, we'll trigger based on time checks in on_data methods
        self.premarket_scanning_active = True
        self.log.info("Premarket scanning scheduled")
    
    def _setup_initial_subscriptions(self) -> None:
        """Setup initial market data subscriptions."""
        for symbol in self.config.scan_universe:
            self._subscribe_to_symbol(symbol)
    
    def _request_all_instruments(self) -> None:
        """Request all available instruments for scanning."""
        # This would trigger the instrument provider to load all instruments
        # In practice, you'd filter for specific criteria (market cap, volume, etc.)
        self.log.info("Requesting all tradable instruments for scanning")
    
    def _subscribe_to_symbol(self, symbol: str) -> None:
        """Subscribe to market data for a specific symbol."""
        try:
            # Create instrument ID
            instrument_id = InstrumentId.from_str(f"{symbol}.ALPACA")
            
            # Get instrument from cache
            instrument = self.cache.instrument(instrument_id)
            if instrument is None:
                self.log.warning(f"Instrument not found: {instrument_id}")
                return
            
            self.instruments[symbol] = instrument
            
            # Create bar type for 1-minute bars
            bar_type = BarType.from_str(f"{symbol}.ALPACA-1-MINUTE-LAST-EXTERNAL")
            self.bar_types[symbol] = bar_type
            
            # Subscribe to bars
            self.subscribe_bars(bar_type)
            
            # Initialize technical indicators
            self.fast_smas[symbol] = SimpleMovingAverage(self.config.fast_sma_period)
            self.slow_smas[symbol] = SimpleMovingAverage(self.config.slow_sma_period)
            
            self.log.info(f"Subscribed to market data for {symbol}")
            
        except Exception as e:
            self.log.error(f"Failed to subscribe to {symbol}: {e}")
    
    def on_bar(self, bar: Bar) -> None:
        """Process incoming bar data."""
        symbol = bar.bar_type.instrument_id.symbol.value
        
        # Check if we're in premarket and should scan
        if self.premarket_scanning_active and not self.scan_complete:
            self._process_premarket_bar(bar, symbol)
        
        # Update technical indicators
        self._update_indicators(bar, symbol)
        
        # Check for trading signals
        if self.market_open and symbol in (self.gainers | self.losers):
            self._check_trading_signals(bar, symbol)
        
        # Update position management
        if bar.bar_type.instrument_id in self.active_positions:
            self._manage_position(bar)
    
    def _process_premarket_bar(self, bar: Bar, symbol: str) -> None:
        """Process bar during premarket to identify movers."""
        # Calculate overnight move percentage
        # This is simplified - in practice you'd compare to previous day's close
        current_time = datetime.now().time()
        
        if self._is_premarket_time(current_time):
            # For demonstration, we'll use a simple price movement calculation
            # In reality, you'd store previous day's closing prices and compare
            overnight_change_pct = self._calculate_overnight_change(bar, symbol)
            
            if overnight_change_pct is not None:
                if overnight_change_pct >= self.config.premarket_threshold:
                    self.gainers.add(symbol)
                    self.log.info(f"Added {symbol} to gainers list: {overnight_change_pct:.2%} move")
                elif overnight_change_pct <= -self.config.premarket_threshold:
                    self.losers.add(symbol)
                    self.log.info(f"Added {symbol} to losers list: {overnight_change_pct:.2%} move")
                
                self.scanned_symbols.add(symbol)
        
        # Check if scanning is complete
        if self._is_market_open_time(current_time):
            self._finalize_premarket_scan()
    
    def _calculate_overnight_change(self, bar: Bar, symbol: str) -> Optional[float]:
        """Calculate overnight percentage change."""
        # This is a simplified implementation
        # In practice, you'd store previous day's closing prices and compare
        
        # For now, we'll use a mock calculation based on current price volatility
        # You would replace this with actual previous close comparison
        high_low_range = (bar.high - bar.low) / bar.close
        
        # Mock overnight change calculation (replace with real logic)
        if high_low_range.as_double() > 0.15:  # High volatility proxy
            return high_low_range.as_double()
        
        return None
    
    def _update_indicators(self, bar: Bar, symbol: str) -> None:
        """Update technical indicators for the symbol."""
        if symbol not in self.fast_smas:
            return
        
        # Store previous values for crossover detection
        if self.fast_smas[symbol].initialized:
            self.previous_fast_sma[symbol] = self.fast_smas[symbol].value
        if self.slow_smas[symbol].initialized:
            self.previous_slow_sma[symbol] = self.slow_smas[symbol].value
        
        # Update indicators with new bar close price
        close_price = bar.close.as_double()
        self.fast_smas[symbol].update_raw(close_price)
        self.slow_smas[symbol].update_raw(close_price)
    
    def _check_trading_signals(self, bar: Bar, symbol: str) -> None:
        """Check for SMA crossover signals."""
        if not self._indicators_ready(symbol):
            return
        
        instrument_id = bar.bar_type.instrument_id
        
        # Skip if we already have a position in this symbol
        if instrument_id in self.active_positions:
            self._check_exit_signals(bar, symbol)
            return
        
        # Skip if we've reached max positions
        if len(self.active_positions) >= self.config.max_positions:
            return
        
        # Skip if daily loss limit reached
        if self.daily_pnl <= -Decimal(str(self.config.daily_loss_limit_usd)):
            self.log.warning("Daily loss limit reached, no new positions")
            return
        
        fast_sma = self.fast_smas[symbol].value
        slow_sma = self.slow_smas[symbol].value
        prev_fast = self.previous_fast_sma.get(symbol, 0)
        prev_slow = self.previous_slow_sma.get(symbol, 0)
        
        # Check for bullish crossover on gainers
        if (symbol in self.gainers and 
            fast_sma > slow_sma and 
            prev_fast <= prev_slow):
            self._enter_long_position(instrument_id, bar.close)
            
        # Check for bearish crossover on losers  
        elif (symbol in self.losers and 
              fast_sma < slow_sma and 
              prev_fast >= prev_slow):
            self._enter_short_position(instrument_id, bar.close)
    
    def _check_exit_signals(self, bar: Bar, symbol: str) -> None:
        """Check for exit signals on existing positions."""
        if not self._indicators_ready(symbol):
            return
        
        instrument_id = bar.bar_type.instrument_id
        position_type = self.active_positions.get(instrument_id)
        
        if not position_type:
            return
        
        fast_sma = self.fast_smas[symbol].value
        slow_sma = self.slow_smas[symbol].value
        prev_fast = self.previous_fast_sma.get(symbol, 0)
        prev_slow = self.previous_slow_sma.get(symbol, 0)
        
        # Exit long position when fast SMA crosses below slow SMA
        if (position_type == "long" and 
            fast_sma < slow_sma and 
            prev_fast >= prev_slow):
            self._close_position(instrument_id, "SMA crossover exit (long)")
            
        # Exit short position when fast SMA crosses above slow SMA
        elif (position_type == "short" and 
              fast_sma > slow_sma and 
              prev_fast <= prev_slow):
            self._close_position(instrument_id, "SMA crossover exit (short)")
    
    def _enter_long_position(self, instrument_id: InstrumentId, price: Price) -> None:
        """Enter a long position."""
        try:
            symbol = instrument_id.symbol.value
            instrument = self.instruments.get(symbol)
            
            if not instrument:
                self.log.error(f"No instrument found for {symbol}")
                return
            
            # Calculate position size
            position_value = Decimal(str(self.config.position_size_usd))
            shares = int(position_value / Decimal(str(price)))
            quantity = instrument.make_qty(shares)
            
            # Create market order
            order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=OrderSide.BUY,
                quantity=quantity,
            )
            
            # Submit order
            self.submit_order(order)
            
            # Track position
            self.active_positions[instrument_id] = "long"
            self.position_entry_prices[instrument_id] = price
            
            self.log.info(f"Entered LONG position: {symbol} @ {price} ({shares} shares)")
            
        except Exception as e:
            self.log.error(f"Failed to enter long position for {instrument_id}: {e}")
    
    def _enter_short_position(self, instrument_id: InstrumentId, price: Price) -> None:
        """Enter a short position."""
        try:
            symbol = instrument_id.symbol.value
            instrument = self.instruments.get(symbol)
            
            if not instrument:
                self.log.error(f"No instrument found for {symbol}")
                return
            
            # Calculate position size
            position_value = Decimal(str(self.config.position_size_usd))
            shares = int(position_value / Decimal(str(price)))
            quantity = instrument.make_qty(shares)
            
            # Create market order
            order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=OrderSide.SELL,
                quantity=quantity,
            )
            
            # Submit order
            self.submit_order(order)
            
            # Track position
            self.active_positions[instrument_id] = "short"
            self.position_entry_prices[instrument_id] = price
            
            self.log.info(f"Entered SHORT position: {symbol} @ {price} ({shares} shares)")
            
        except Exception as e:
            self.log.error(f"Failed to enter short position for {instrument_id}: {e}")
    
    def _close_position(self, instrument_id: InstrumentId, reason: str) -> None:
        """Close an existing position."""
        try:
            position = self.portfolio.position(instrument_id)
            if not position or position.is_flat:
                return
            
            # Determine order side for closing
            if position.is_long:
                order_side = OrderSide.SELL
            else:
                order_side = OrderSide.BUY
            
            # Create market order to close
            order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=order_side,
                quantity=abs(position.quantity),
            )
            
            # Submit order
            self.submit_order(order)
            
            # Remove from tracking
            self.active_positions.pop(instrument_id, None)
            entry_price = self.position_entry_prices.pop(instrument_id, None)
            
            symbol = instrument_id.symbol.value
            self.log.info(f"Closed position: {symbol} - Reason: {reason}")
            
        except Exception as e:
            self.log.error(f"Failed to close position for {instrument_id}: {e}")
    
    def _manage_position(self, bar: Bar) -> None:
        """Manage existing positions (stop losses, risk management)."""
        instrument_id = bar.bar_type.instrument_id
        entry_price = self.position_entry_prices.get(instrument_id)
        position_type = self.active_positions.get(instrument_id)
        
        if not entry_price or not position_type:
            return
        
        current_price = bar.close
        
        # Calculate P&L percentage
        if position_type == "long":
            pnl_pct = (current_price - entry_price) / entry_price
        else:  # short
            pnl_pct = (entry_price - current_price) / entry_price
        
        # Check for stop loss
        if pnl_pct.as_double() <= -self.config.max_loss_per_trade_pct:
            self._close_position(instrument_id, f"Stop loss hit: {pnl_pct:.2%}")
    
    def _indicators_ready(self, symbol: str) -> bool:
        """Check if indicators are ready for signal generation."""
        return (symbol in self.fast_smas and 
                symbol in self.slow_smas and
                self.fast_smas[symbol].initialized and 
                self.slow_smas[symbol].initialized and
                symbol in self.previous_fast_sma and
                symbol in self.previous_slow_sma)
    
    def _is_premarket_time(self, current_time: time) -> bool:
        """Check if current time is within premarket hours."""
        start_time = time.fromisoformat(self.config.premarket_start_time)
        end_time = time.fromisoformat(self.config.premarket_end_time)
        return start_time <= current_time <= end_time
    
    def _is_market_open_time(self, current_time: time) -> bool:
        """Check if market is open."""
        market_open = time.fromisoformat(self.config.premarket_end_time)
        market_close = time.fromisoformat(self.config.market_close_time)
        return market_open <= current_time <= market_close
    
    def _finalize_premarket_scan(self) -> None:
        """Finalize the premarket scanning process."""
        if self.scan_complete:
            return
        
        self.scan_complete = True
        self.market_open = True
        self.premarket_scanning_active = False
        
        self.log.info(f"Premarket scan complete:")
        self.log.info(f"  Gainers ({len(self.gainers)}): {list(self.gainers)}")
        self.log.info(f"  Losers ({len(self.losers)}): {list(self.losers)}")
        
        # Subscribe to additional symbols if needed
        all_targets = self.gainers | self.losers
        for symbol in all_targets:
            if symbol not in self.instruments:
                self._subscribe_to_symbol(symbol)