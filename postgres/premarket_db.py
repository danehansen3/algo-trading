"""
Premarket scanner specific database operations
"""
from .database import PostgresDB
import pandas as pd
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)

class PremarketDB(PostgresDB):
    """Database operations specific to premarket scanner"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.init_schema()
    
    def init_schema(self):
        """Initialize premarket scanner database schema"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS premarket_scans (
            id SERIAL PRIMARY KEY,
            scan_date DATE NOT NULL,
            scan_time TIMESTAMP WITH TIME ZONE NOT NULL,
            ticker TEXT NOT NULL,
            company_name TEXT,
            current_price DECIMAL(10,4) NOT NULL,
            previous_close DECIMAL(10,4) NOT NULL,
            gap_percent DECIMAL(8,4) NOT NULL,
            current_volume BIGINT NOT NULL,
            avg_volume_50d BIGINT,
            relative_volume DECIMAL(8,4),
            shares_outstanding BIGINT,
            float_shares BIGINT,
            float_display TEXT,
            sector TEXT,
            market_cap TEXT,
            news_catalyst TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            
            CONSTRAINT unique_scan_entry UNIQUE(scan_date, scan_time, ticker)
        );
        
        CREATE INDEX IF NOT EXISTS idx_premarket_scan_date ON premarket_scans(scan_date);
        CREATE INDEX IF NOT EXISTS idx_premarket_ticker ON premarket_scans(ticker);
        CREATE INDEX IF NOT EXISTS idx_premarket_gap_percent ON premarket_scans(gap_percent);
        CREATE INDEX IF NOT EXISTS idx_premarket_scan_time ON premarket_scans(scan_time);
        """
        
        try:
            self.execute_command(create_table_sql)
            logger.info("Premarket database schema initialized")
        except Exception as e:
            logger.error(f"Failed to initialize schema: {e}")
            raise
    
    def save_scan_results(self, results_df: pd.DataFrame, scan_time: datetime) -> int:
        """Save premarket scan results to database"""
        if results_df.empty:
            logger.info("No scan results to save")
            return 0
        
        # Prepare dataframe for insertion
        results_df = results_df.copy()
        results_df['scan_date'] = scan_time.date()
        results_df['scan_time'] = scan_time
        
        try:
            # Handle conflicts by updating existing records
            insert_sql = """
            INSERT INTO premarket_scans 
            (scan_date, scan_time, ticker, company_name, current_price, previous_close,
             gap_percent, current_volume, avg_volume_50d, relative_volume, 
             shares_outstanding, float_shares, float_display, sector, market_cap, news_catalyst)
            VALUES %s
            ON CONFLICT (scan_date, scan_time, ticker) 
            DO UPDATE SET
                current_price = EXCLUDED.current_price,
                gap_percent = EXCLUDED.gap_percent,
                current_volume = EXCLUDED.current_volume,
                relative_volume = EXCLUDED.relative_volume
            """
            
            # Use insert_dataframe for simplicity, handling conflicts at application level
            rows_inserted = self.insert_dataframe(results_df, 'premarket_scans', if_exists='append')
            logger.info(f"Saved {rows_inserted} premarket scan records")
            return rows_inserted
            
        except Exception as e:
            logger.error(f"Failed to save scan results: {e}")
            return 0
    
    def get_todays_scans(self) -> pd.DataFrame:
        """Get today's premarket scans"""
        query = """
        SELECT * FROM premarket_scans 
        WHERE scan_date = %s 
        ORDER BY scan_time DESC, gap_percent DESC
        """
        return self.read_sql(query, (date.today(),))
    
    def get_historical_scans(self, days_back: int = 30) -> pd.DataFrame:
        """Get historical premarket scans"""
        query = """
        SELECT * FROM premarket_scans 
        WHERE scan_date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY scan_date DESC, gap_percent DESC
        """
        return self.read_sql(query, (days_back,))
    
    def get_ticker_history(self, ticker: str, days_back: int = 7) -> pd.DataFrame:
        """Get scan history for specific ticker"""
        query = """
        SELECT * FROM premarket_scans 
        WHERE ticker = %s AND scan_date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY scan_date DESC, scan_time DESC
        """
        return self.read_sql(query, (ticker, days_back))