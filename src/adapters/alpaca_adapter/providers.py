"""
Alpaca instrument provider implementation.
"""

import asyncio
from decimal import Decimal
from typing import Any

from alpaca_adapter.data import StockHistoricalDataClient
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass
from alpaca.trading.enums import AssetStatus

from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AssetType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class AlpacaInstrumentProvider(InstrumentProvider):
    """
    Provides instrument definitions for Alpaca Markets.
    
    Parameters
    ----------
    client : TradingClient
        The Alpaca trading client.
    data_client : StockHistoricalDataClient
        The Alpaca data client.
    """
    
    def __init__(
        self,
        client: TradingClient,
        data_client: StockHistoricalDataClient,
    ) -> None:
        super().__init__()
        self._client = client
        self._data_client = data_client
        self._venue = Venue("ALPACA")
        
        # Add USD currency
        self.add_currency(currency=USD)
    
    async def load_all_async(self, filters: dict[str, Any] | None = None) -> None:
        """
        Load all available instruments from Alpaca.
        
        Parameters
        ----------
        filters : dict[str, Any], optional
            Additional filters for instrument loading.
        """
        # Run blocking call in thread pool
        loop = asyncio.get_event_loop()
        assets = await loop.run_in_executor(
            None,
            lambda: self._client.get_all_assets(
                status=AssetStatus.ACTIVE,
                asset_class=AssetClass.US_EQUITY,
            )
        )
        
        # Convert Alpaca assets to Nautilus instruments
        for asset in assets:
            if asset.tradable and asset.status == AssetStatus.ACTIVE:
                instrument = self._parse_instrument(asset)
                if instrument:
                    self.add(instrument=instrument)
    
    async def load_ids_async(
        self, 
        instrument_ids: list[InstrumentId], 
        filters: dict[str, Any] | None = None,
    ) -> None:
        """
        Load specific instruments by their IDs.
        
        Parameters
        ----------
        instrument_ids : list[InstrumentId]
            The instrument IDs to load.
        filters : dict[str, Any], optional
            Additional filters for instrument loading.
        """
        symbols = [instrument_id.symbol.value for instrument_id in instrument_ids]
        
        # Run blocking call in thread pool
        loop = asyncio.get_event_loop()
        
        for symbol in symbols:
            try:
                asset = await loop.run_in_executor(
                    None,
                    lambda s=symbol: self._client.get_asset(s)
                )
                
                if asset.tradable and asset.status == AssetStatus.ACTIVE:
                    instrument = self._parse_instrument(asset)
                    if instrument:
                        self.add(instrument=instrument)
                        
            except Exception as e:
                self._log.error(f"Failed to load instrument {symbol}: {e}")
    
    async def load_async(
        self, 
        instrument_id: InstrumentId, 
        filters: dict[str, Any] | None = None,
    ) -> None:
        """
        Load a single instrument by its ID.
        
        Parameters
        ----------
        instrument_id : InstrumentId
            The instrument ID to load.
        filters : dict[str, Any], optional
            Additional filters for instrument loading.
        """
        symbol = instrument_id.symbol.value
        
        # Run blocking call in thread pool
        loop = asyncio.get_event_loop()
        
        try:
            asset = await loop.run_in_executor(
                None,
                lambda: self._client.get_asset(symbol)
            )
            
            if asset.tradable and asset.status == AssetStatus.ACTIVE:
                instrument = self._parse_instrument(asset)
                if instrument:
                    self.add(instrument=instrument)
                    
        except Exception as e:
            self._log.error(f"Failed to load instrument {symbol}: {e}")
    
    def _parse_instrument(self, asset) -> Equity | None:
        """
        Parse an Alpaca asset into a Nautilus instrument.
        
        Parameters
        ----------
        asset : Asset
            The Alpaca asset to parse.
            
        Returns
        -------
        Equity or None
            The parsed instrument, or None if parsing failed.
        """
        try:
            symbol = Symbol(asset.symbol)
            instrument_id = InstrumentId(symbol=symbol, venue=self._venue)
            
            # Create equity instrument
            return Equity(
                instrument_id=instrument_id,
                raw_symbol=Symbol(asset.symbol),
                asset_type=AssetType.SPOT,
                currency=USD,
                price_precision=2,  # US equities typically 2 decimal places
                size_precision=0,   # Shares are whole numbers
                price_increment=Price.from_str("0.01"),
                size_increment=Quantity.from_int(1),
                lot_size=Quantity.from_int(1),
                margin_init=Decimal("1.0"),  # 100% margin requirement by default
                margin_maint=Decimal("1.0"),
                maker_fee=Decimal("0.0"),  # Alpaca has no commission fees
                taker_fee=Decimal("0.0"),
                ts_event=self._clock.timestamp_ns(),
                ts_init=self._clock.timestamp_ns(),
                info={"alpaca_asset": asset.__dict__},
            )
            
        except Exception as e:
            self._log.error(f"Failed to parse instrument {asset.symbol}: {e}")
            return None