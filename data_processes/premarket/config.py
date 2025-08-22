"""
Configuration for premarket scanner
"""
from dataclasses import dataclass
from typing import Dict, Tuple

@dataclass
class ScannerConfig:
    """Configuration for Ross Cameron style premarket scanner"""
    
    # Ross Cameron criteria
    min_gap_percent: float = 10.0           # Already up 10% on the day
    relative_volume_threshold: float = 5.0   # 5x relative average volume (50-day)
    price_range: Tuple[float, float] = (2.0, 20.0)  # $2-$20 price range
    shares_outstanding_range: Tuple[int, int] = (10_000_000, 20_000_000)  # 10-20M shares
    
    # Additional filtering
    min_volume: int = 100_000               # Minimum daily volume threshold
    
    # Data sources
    quote_page: str = 'http://thestockmarketwatch.com/markets/pre-market/today.aspx'
    yahoo_stats_template: str = 'https://finance.yahoo.com/quote/{}/key-statistics?p={}'
    
    # Web scraping headers
    headers: Dict[str, str] = None
    
    def __post_init__(self):
        if self.headers is None:
            self.headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }