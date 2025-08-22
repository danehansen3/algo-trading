"""
Main premarket scanner logic
"""
from datetime import datetime
import logging
import pandas as pd
from .config import ScannerConfig
from .scraper import PremarketScraper
from ..postgres.premarket_db import PremarketDB

logger = logging.getLogger(__name__)

class PremarketScanner:
    """Main premarket scanner orchestrator"""
    
    def __init__(self, config: ScannerConfig = None, db: PremarketDB = None):
        self.config = config or ScannerConfig()
        self.db = db or PremarketDB()
        self.scraper = PremarketScraper(self.config)
    
    def run_scan(self) -> pd.DataFrame:
        """Execute complete premarket scan"""
        scan_start = datetime.now()
        logger.info(f"Starting premarket scan at {scan_start}")
        
        try:
            # Step 1: Get initial premarket movers
            initial_candidates = self.scraper.scrape_premarket_movers()
            if initial_candidates.empty:
                logger.info("No initial candidates found")
                return pd.DataFrame()
            
            # Step 2: Enhance with yfinance data and apply final filters
            qualified_stocks = self.scraper.enhance_with_yfinance_data(initial_candidates)
            
            # Step 3: Save results to database
            if not qualified_stocks.empty:
                self.db.save_scan_results(qualified_stocks, scan_start)
                self._log_results(qualified_stocks, scan_start)
            else:
                logger.info("No stocks met all criteria")
            
            return qualified_stocks
            
        except Exception as e:
            logger.error(f"Premarket scan failed: {e}")
            return pd.DataFrame()
    
    def _log_results(self, df: pd.DataFrame, scan_time: datetime):
        """Log scan results summary"""
        logger.info(f"SCAN COMPLETE - {scan_time.strftime('%H:%M:%S')} EST")
        logger.info(f"Found {len(df)} qualified stocks:")
        
        for _, row in df.iterrows():
            logger.info(
                f"  {row['ticker']}: {row['gap_percent']:.1f}% gap, "
                f"{row['relative_volume']:.1f}x volume, "
                f"${row['current_price']:.2f}"
            )

def main():
    """Main entry point for running premarket scan"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    scanner = PremarketScanner()
    results = scanner.run_scan()
    
    if not results.empty:
        print(f"\nFound {len(results)} qualified premarket gainers")
    else:
        print("No qualified stocks found")

if __name__ == "__main__":
    main()