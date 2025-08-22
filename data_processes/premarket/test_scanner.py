"""
Test script to verify scanner functionality
Run this to see sample output before deploying
"""
import logging
from datetime import datetime
from data_processes.premarket.scanner import PremarketScanner

def setup_test_logging():
    """Setup logging for test run"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def main():
    """Test the scanner locally"""
    setup_test_logging()
    logger = logging.getLogger(__name__)
    
    print("=" * 70)
    print("TESTING PREMARKET SCANNER")
    print("=" * 70)
    
    try:
        # Note: This will fail without database, but you can see scraping output
        scanner = PremarketScanner()
        
        # Test just the scraping part
        from data_processes.premarket.config import ScannerConfig
        from data_processes.premarket.scraper import PremarketScraper
        
        config = ScannerConfig()
        scraper = PremarketScraper(config)
        
        print("\n1. Testing initial premarket scraping...")
        initial_candidates = scraper.scrape_premarket_movers()
        
        if not initial_candidates.empty:
            print(f"Found {len(initial_candidates)} initial candidates:")
            print(initial_candidates[['ticker', 'current_price', 'gap_percent', 'current_volume']].head())
            
            print(f"\n2. Testing Yahoo Finance enhancement (first 2 stocks only)...")
            sample = initial_candidates.head(2)
            enhanced = scraper.enhance_with_yfinance_data(sample)
            
            if not enhanced.empty:
                print("Enhanced results:")
                print(enhanced[['ticker', 'gap_percent', 'relative_volume', 'shares_outstanding']].to_string())
            else:
                print("No stocks passed the enhanced filtering")
        else:
            print("No initial candidates found (market may be closed)")
            
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()