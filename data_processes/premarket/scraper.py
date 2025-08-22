"""
Web scraping functions for premarket data
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
import yfinance as yf
from urllib.request import urlopen, Request
from typing import Optional, Dict, List
import time
import logging
from .config import ScannerConfig

logger = logging.getLogger(__name__)

class PremarketScraper:
    """Web scraper for premarket gainer data"""
    
    def __init__(self, config: ScannerConfig):
        self.config = config
    
    def _make_soup(self, url: str) -> Optional[BeautifulSoup]:
        """Create BeautifulSoup object from URL with error handling"""
        try:
            req = Request(url=url, headers=self.config.headers)
            page = urlopen(req, timeout=15).read()
            return BeautifulSoup(page, 'html.parser')
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None
    
    def _parse_number_string(self, num_str: str) -> int:
        """Convert number string (e.g., '2.5M') to integer"""
        if not num_str or num_str == '-':
            return 0
        
        num_str = num_str.lower().strip().replace(',', '')
        
        try:
            if 'k' in num_str:
                return int(float(num_str[:-1]) * 1_000)
            elif 'm' in num_str:
                return int(float(num_str[:-1]) * 1_000_000)
            elif 'b' in num_str:
                return int(float(num_str[:-1]) * 1_000_000_000)
            else:
                return int(float(num_str))
        except (ValueError, IndexError):
            logger.warning(f"Could not parse number: {num_str}")
            return 0
    
    def scrape_premarket_movers(self) -> pd.DataFrame:
        """Scrape initial premarket movers from thestockmarketwatch"""
        logger.info("Scraping premarket movers...")
        
        soup = self._make_soup(self.config.quote_page)
        if not soup:
            return pd.DataFrame()
        
        try:
            table = soup.find('table', {'id': 'tblMoversDesktop'})
            if not table:
                logger.error("Could not find premarket movers table")
                return pd.DataFrame()
            
            candidates = []
            rows = table.find_all('tr')[1:]  # Skip header
            
            for row in rows:
                try:
                    ticker_elem = row.find('a', {'class': 'symbol'})
                    company_elem = row.find('a', {'class': 'company'})
                    price_elem = row.find('div', {'class': 'lastPrice'})
                    change_elem = row.find('div', {'class': 'chgUp'})
                    volume_elem = row.find('td', {'class': 'tdVolume'})
                    
                    if not all([ticker_elem, price_elem, change_elem, volume_elem]):
                        continue
                    
                    ticker = ticker_elem.text.strip()
                    company_name = company_elem.text.strip() if company_elem else ""
                    current_price = float(price_elem.text.strip().replace('$', ''))
                    gap_percent = float(change_elem.text.strip().rstrip('%'))
                    volume = int(volume_elem.text.strip().replace(',', ''))
                    
                    # Apply initial filters (price range and gap threshold)
                    if (gap_percent >= self.config.min_gap_percent and 
                        self.config.price_range[0] <= current_price <= self.config.price_range[1] and
                        volume >= self.config.min_volume):
                        
                        candidates.append({
                            'ticker': ticker,
                            'company_name': company_name,
                            'current_price': current_price,
                            'gap_percent': gap_percent,
                            'current_volume': volume
                        })
                        
                except (ValueError, AttributeError) as e:
                    logger.debug(f"Error parsing row: {e}")
                    continue
            
            df = pd.DataFrame(candidates)
            logger.info(f"Found {len(df)} initial candidates")
            return df
            
        except Exception as e:
            logger.error(f"Error scraping premarket data: {e}")
            return pd.DataFrame()
    
    def enhance_with_yfinance_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enhance with yfinance data for volume and shares outstanding"""
        if df.empty:
            return df
        
        logger.info(f"Enhancing {len(df)} candidates with yfinance data...")
        enhanced_candidates = []
        
        for _, row in df.iterrows():
            try:
                ticker = row['ticker']
                stock = yf.Ticker(ticker)
                
                # Get basic info
                info = stock.info
                
                # Get historical data for volume analysis
                hist = stock.history(period="60d")  # 60 days to ensure 50-day average
                if hist.empty or len(hist) < 50:
                    logger.debug(f"Insufficient historical data for {ticker}")
                    continue
                
                # Calculate previous close and verify gap
                previous_close = hist['Close'].iloc[-1]  # Most recent close
                actual_gap = ((row['current_price'] - previous_close) / previous_close) * 100
                
                # Calculate 50-day average volume
                avg_volume_50d = int(hist['Volume'].tail(50).mean())
                relative_volume = row['current_volume'] / avg_volume_50d if avg_volume_50d > 0 else 0
                
                # Get shares outstanding
                shares_outstanding = info.get('sharesOutstanding', 0) or info.get('impliedSharesOutstanding', 0)
                
                # Apply Ross Cameron filters
                if (relative_volume >= self.config.relative_volume_threshold and
                    self.config.shares_outstanding_range[0] <= shares_outstanding <= self.config.shares_outstanding_range[1]):
                    
                    enhanced_row = {
                        **row.to_dict(),
                        'previous_close': previous_close,
                        'gap_percent': actual_gap,  # Use calculated gap
                        'avg_volume_50d': avg_volume_50d,
                        'relative_volume': relative_volume,
                        'shares_outstanding': shares_outstanding,
                        'sector': info.get('sector', ''),
                        'market_cap': info.get('marketCap', ''),
                        'float_shares': info.get('floatShares', 0),
                        'float_display': f"{info.get('floatShares', 0)/1e6:.1f}M" if info.get('floatShares') else 'N/A'
                    }
                    
                    enhanced_candidates.append(enhanced_row)
                    logger.info(f"✓ {ticker}: {actual_gap:.1f}% gap, {relative_volume:.1f}x volume, {shares_outstanding/1e6:.1f}M shares")
                else:
                    logger.debug(f"✗ {ticker}: Failed volume or shares filter")
                
                # Rate limiting
                time.sleep(0.3)
                
            except Exception as e:
                logger.warning(f"Error enhancing {row['ticker']}: {e}")
                continue
        
        result_df = pd.DataFrame(enhanced_candidates)
        logger.info(f"Final qualified stocks: {len(result_df)}")
        return result_df