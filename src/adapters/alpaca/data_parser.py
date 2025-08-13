from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional

from nautilus_trader.model.data import QuoteTick, TradeTick, Bar
from nautilus_trader.model.enums import AggressorSide, BarType, PriceType
from nautilus_trader.model.identifiers import InstrumentId, TradeId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.core.datetime import millis_to_nanos


class AlpacaDataParser:
    """Parse Alpaca market data into Nautilus objects."""
    
    @staticmethod
    def parse_trade_tick(
        data: Dict[str, Any],
        instrument_id: InstrumentId,
    ) -> Optional[TradeTick]:
        """Parse Alpaca trade data to TradeTick."""
        try:
            # Alpaca trade message format:
            # {
            #   "T": "t",  # message type (trade)
            #   "S": "AAPL",  # symbol
            #   "p": 150.25,  # price
            #   "s": 100,  # size
            #   "t": "2021-01-01T15:30:00Z",  # timestamp
            #   "c": ["@"],  # conditions
            # }
            
            price = Price.from_str(str(data["p"]))
            size = Quantity.from_int(data["s"])
            
            # Convert timestamp
            timestamp_str = data["t"]
            # Parse ISO format timestamp to nanoseconds
            timestamp_ns = millis_to_nanos(
                int(datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')).timestamp() * 1000)
            )
            
            # Determine aggressor side (Alpaca doesn't provide this directly)
            aggressor_side = AggressorSide.NO_AGGRESSOR
            
            return TradeTick(
                instrument_id=instrument_id,
                price=price,
                size=size,
                aggressor_side=aggressor_side,
                trade_id=TradeId(f"{data['S']}-{timestamp_ns}"),
                ts_event=timestamp_ns,
                ts_init=timestamp_ns,
            )
            
        except Exception as e:
            # Log error and return None
            return None
    
    @staticmethod  
    def parse_quote_tick(
        data: Dict[str, Any],
        instrument_id: InstrumentId,
    ) -> Optional[QuoteTick]:
        """Parse Alpaca quote data to QuoteTick."""
        try:
            # Alpaca quote message format:
            # {
            #   "T": "q",  # message type (quote) 
            #   "S": "AAPL",  # symbol
            #   "bp": 150.24,  # bid price
            #   "bs": 100,  # bid size
            #   "ap": 150.26,  # ask price  
            #   "as": 200,  # ask size
            #   "t": "2021-01-01T15:30:00Z",  # timestamp
            # }
            
            bid_price = Price.from_str(str(data["bp"]))
            ask_price = Price.from_str(str(data["ap"]))
            bid_size = Quantity.from_int(data["bs"])
            ask_size = Quantity.from_int(data["as"])
            
            # Convert timestamp
            timestamp_str = data["t"]
            timestamp_ns = millis_to_nanos(
                int(datetime.fromisoformat(timestamp_str.replace('Z', '+00:00')).timestamp() * 1000)
            )
            
            return QuoteTick(
                instrument_id=instrument_id,
                bid_price=bid_price,
                ask_price=ask_price,
                bid_size=bid_size, 
                ask_size=ask_size,
                ts_event=timestamp_ns,
                ts_init=timestamp_ns,
            )
            
        except Exception as e:
            return None