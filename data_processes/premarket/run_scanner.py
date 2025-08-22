import sys
import os
import logging
from datetime import datetime
import traceback

# Ensure the data_processes module can be imported
sys.path.insert(0, '/app')

def setup_logging():
    """Setup logging configuration for production"""
    os.makedirs('/app/logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s EST - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/app/logs/premarket_scanner.log'),
            logging.StreamHandler()
        ]
    )

def test_database_connection():
    """Test database connection before running scanner"""
    try:
        from data_processes.postgres.premarket_db import PremarketDB
        db = PremarketDB()
        
        # Test basic connectivity
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            logging.info(f"Database connected successfully: {version[:50]}...")
            
        return True
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return False

def main():
    """Main execution function - all scanner logic runs here"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Log startup banner
    logger.info("=" * 70)
    logger.info(f"PREMARKET SCANNER STARTING - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} EST")
    logger.info("=" * 70)
    
    try:
        # Test database connection first
        if not test_database_connection():
            logger.error("Cannot proceed without database connection")
            sys.exit(1)
        
        # Import and run scanner
        from data_processes.premarket import PremarketScanner
        
        logger.info("Initializing premarket scanner...")
        scanner = PremarketScanner()
        
        # Run the complete scan
        logger.info("Executing premarket scan...")
        results = scanner.run_scan()
        
        # Log final results
        if not results.empty:
            logger.info("=" * 50)
            logger.info(f"SCAN SUCCESSFUL - FOUND {len(results)} QUALIFYING STOCKS")
            logger.info("=" * 50)
            
            for _, stock in results.iterrows():
                logger.info(
                    f"{stock['ticker']:>6} | "
                    f"${stock['current_price']:>6.2f} | "
                    f"{stock['gap_percent']:>6.1f}% gap | "
                    f"{stock['relative_volume']:>5.1f}x vol | "
                    f"{stock['shares_outstanding']/1e6:>4.1f}M shares"
                )
            logger.info("=" * 50)
        else:
            logger.info("SCAN COMPLETE - No stocks met all criteria")
        
        logger.info("Scanner execution completed successfully")
        
    except Exception as e:
        logger.error(f"Scanner execution failed: {e}")
        logger.error("Full traceback:")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()