from decimal import Decimal
from typing import Any, Dict, List, Optional, Union
import asyncio

from nautilus_trader.model.instruments import Equity, CurrencyPair
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.common.component import Logger

from adapters.alpaca.http_client import AlpacaHttpClient


class AlpacaInstrumentProvider(InstrumentProvider):
    """
    Provides instrument definitions for Alpaca.
    
    Loads and manages tradable instruments from Alpaca's asset endpoint.
    """
    
    def __init__(
        self,
        client: AlpacaHttpClient,
        logger: Logger,
        load_all: bool = True,
        log_warnings: bool = True,
    ) -> None:
        self._client = client
        self._logger = logger
        self._load_all = load_all
        self._log_warnings = log_warnings
        
        self._instruments: Dict[InstrumentId, Union[Equity, CurrencyPair]] = {}
        self._venue = Venue("ALPACA")
    
    async def load_all_async(self, filters: Optional[Dict] = None) -> None:
        """Load all available instruments asynchronously."""
        try:
            # Get all active assets
            assets = await self._client.get_assets(status="active")
            
            for asset_data in assets:
                try:
                    instrument = self._parse_instrument(asset_data)
                    if instrument:
                        self._instruments[instrument.id] = instrument
                        
                except Exception as e:
                    if self._log_warnings:
                        self._logger.warning(f"Failed to parse instrument {asset_data.get('symbol', 'UNKNOWN')}: {e}")
            
            self._logger.info(f"Loaded {len(self._instruments)} instruments")
            
        except Exception as e:
            self._logger.error(f"Failed to load instruments: {e}")
            raise
    
    def _parse_instrument(self, asset_data: Dict[str, Any]) -> Optional[Union[Equity, CurrencyPair]]:
        """Parse Alpaca asset data into Nautilus instrument."""
        try:
            symbol = asset_data["symbol"]
            name = asset_data.get("name", symbol)
            asset_class = asset_data.get("class")
            
            # Create instrument ID
            instrument_id = InstrumentId(
                symbol=Symbol(symbol),
                venue=self._venue,
            )
            
            # Create different instrument types based on asset class
            if asset_class == "us_equity":
                return Equity(
                    instrument_id=instrument_id,
                    native_symbol=Symbol(symbol),
                    currency="USD",  # Alpaca US equities are USD
                    price_precision=2,  # Standard for US equities
                    size_precision=0,  # Shares are whole numbers
                    price_increment=Price.from_str("0.01"),
                    size_increment=Quantity.from_int(1),
                    maker_fee=Decimal("0.0"),  # Alpaca is commission-free
                    taker_fee=Decimal("0.0"),
                    margin_init=Decimal("0.5"),  # 50% margin requirement
                    margin_maint=Decimal("0.25"),
                    lot_size=Quantity.from_int(1),
                    max_quantity=None,
                    min_quantity=Quantity.from_int(1),
                    max_price=None,
                    min_price=Price.from_str("0.01"),
                    ts_event=0,  # Will be set when loaded
                    ts_init=0,
                    info=asset_data,  # Store raw data for reference
                )
            
            elif asset_class == "crypto":
                # Handle crypto assets
                base_currency = symbol.split("/")[0] if "/" in symbol else symbol[:3]
                quote_currency = symbol.split("/")[1] if "/" in symbol else "USD"
                
                return CurrencyPair(
                    instrument_id=instrument_id,
                    native_symbol=Symbol(symbol),
                    base_currency=base_currency,
                    quote_currency=quote_currency,
                    price_precision=8,  # Standard crypto precision
                    size_precision=8,
                    price_increment=Price.from_str("0.00000001"),
                    size_increment=Quantity.from_str("0.00000001"), 
                    maker_fee=Decimal("0.0025"),  # Alpaca crypto fees
                    taker_fee=Decimal("0.0025"),
                    margin_init=Decimal("1.0"),  # No margin for crypto
                    margin_maint=Decimal("1.0"),
                    lot_size=Quantity.from_str("0.00000001"),
                    max_quantity=None,
                    min_quantity=Quantity.from_str("0.00000001"),
                    max_price=None,
                    min_price=Price.from_str("0.00000001"),
                    ts_event=0,
                    ts_init=0,
                    info=asset_data,
                )
            
            return None
            
        except Exception as e:
            if self._log_warnings:
                self._logger.warning(f"Error parsing instrument {asset_data.get('symbol', 'UNKNOWN')}: {e}")
            return None
    
    def get(self, instrument_id: InstrumentId) -> Optional[Union[Equity, CurrencyPair]]:
        """Get instrument by ID."""
        return self._instruments.get(instrument_id)
    
    def get_all(self) -> Dict[InstrumentId, Union[Equity, CurrencyPair]]:
        """Get all loaded instruments."""
        return self._instruments.copy()