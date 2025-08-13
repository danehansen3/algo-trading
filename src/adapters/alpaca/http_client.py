import asyncio
import hmac
import hashlib
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin, urlencode
from adapters.alpaca.common import AlpacaAccountType
import aiohttp
from datetime import datetime

from nautilus_trader.common.component import Logger
from nautilus_trader.core.datetime import nanos_to_millis


class AlpacaHttpClient:
    """
    Low-level HTTP client for Alpaca API.
    
    Provides async HTTP connectivity to Alpaca's REST API with proper
    authentication and error handling.
    """
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: Optional[str] = None,
        account_type: AlpacaAccountType = AlpacaAccountType.PAPER,
        logger: Optional[Logger] = None,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._account_type = account_type
        self._logger = logger or Logger(name=type(self).__name__)
        
        # Set base URLs based on account type
        if base_url:
            self._base_url = base_url
        else:
            if account_type == AlpacaAccountType.PAPER:
                self._base_url = "https://paper-api.alpaca.markets"
            else:
                self._base_url = "https://api.alpaca.markets"
        
        self._session: Optional[aiohttp.ClientSession] = None
        
    @property
    def base_url(self) -> str:
        return self._base_url
    
    async def connect(self) -> None:
        """Connect the HTTP client."""
        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._logger.info("HTTP client connected")
    
    async def disconnect(self) -> None:
        """Disconnect the HTTP client."""
        if self._session:
            await self._session.close()
            self._session = None
            self._logger.info("HTTP client disconnected")
    
    def _get_headers(self) -> Dict[str, str]:
        """Get authentication headers for requests."""
        return {
            "APCA-API-KEY-ID": self._api_key,
            "APCA-API-SECRET-KEY": self._api_secret,
            "Content-Type": "application/json",
        }
    
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute HTTP request with error handling."""
        if not self._session:
            await self.connect()
        
        url = urljoin(self._base_url, endpoint)
        headers = self._get_headers()
        
        try:
            async with self._session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_data,
            ) as response:
                if response.status >= 400:
                    error_text = await response.text()
                    self._logger.error(f"HTTP {response.status}: {error_text}")
                    raise Exception(f"HTTP {response.status}: {error_text}")
                
                return await response.json()
                
        except Exception as e:
            self._logger.error(f"HTTP request failed: {e}")
            raise
    
    # Account endpoints
    async def get_account(self) -> Dict[str, Any]:
        """Get account information."""
        return await self._request("GET", "/v2/account")
    
    async def get_positions(self) -> List[Dict[str, Any]]:
        """Get all positions."""
        return await self._request("GET", "/v2/positions")
    
    async def get_position(self, symbol: str) -> Dict[str, Any]:
        """Get position for specific symbol."""
        return await self._request("GET", f"/v2/positions/{symbol}")
    
    # Order endpoints  
    async def get_orders(
        self,
        status: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Get orders with optional filters."""
        params = {}
        if status:
            params["status"] = status
        if limit:
            params["limit"] = limit
        return await self._request("GET", "/v2/orders", params=params)
    
    async def get_order(self, order_id: str) -> Dict[str, Any]:
        """Get specific order by ID."""
        return await self._request("GET", f"/v2/orders/{order_id}")
    
    async def submit_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """Submit new order."""
        return await self._request("POST", "/v2/orders", json_data=order_data)
    
    async def cancel_order(self, order_id: str) -> None:
        """Cancel specific order."""
        await self._request("DELETE", f"/v2/orders/{order_id}")
    
    async def cancel_all_orders(self) -> List[Dict[str, Any]]:
        """Cancel all orders."""
        return await self._request("DELETE", "/v2/orders")
    
    # Asset endpoints
    async def get_assets(
        self,
        asset_class: Optional[str] = None,
        status: str = "active",
    ) -> List[Dict[str, Any]]:
        """Get tradable assets."""
        params = {"status": status}
        if asset_class:
            params["asset_class"] = asset_class
        return await self._request("GET", "/v2/assets", params=params)
    
    async def get_asset(self, symbol: str) -> Dict[str, Any]:
        """Get specific asset."""
        return await self._request("GET", f"/v2/assets/{symbol}")
    
    # Market data endpoints (through Alpaca Data API)
    async def get_bars(
        self,
        symbols: List[str],
        timeframe: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Get historical bars."""
        # Note: This uses the data API, different base URL
        data_url = "https://data.alpaca.markets"
        
        params = {
            "symbols": ",".join(symbols),
            "timeframe": timeframe,
        }
        if start:
            params["start"] = start.isoformat()
        if end:
            params["end"] = end.isoformat()
        if limit:
            params["limit"] = limit
            
        # Temporarily change base URL for data request
        original_base = self._base_url
        self._base_url = data_url
        try:
            result = await self._request("GET", "/v2/stocks/bars", params=params)
        finally:
            self._base_url = original_base
            
        return result