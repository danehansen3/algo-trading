"""
PostgreSQL database abstraction layer
"""
import psycopg2
import psycopg2.extras
import pandas as pd
from typing import Optional, Dict, Any, List
import logging
import os
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class PostgresDB:
    """PostgreSQL database connection and operations"""
    
    def __init__(self, 
                 host: str = None,
                 port: int = None,
                 database: str = None,
                 user: str = None,
                 password: str = None):
        
        # Use environment variables with defaults
        self.host = host or os.getenv('POSTGRES_HOST', 'localhost')
        self.port = port or int(os.getenv('POSTGRES_PORT', 5432))
        self.database = database or os.getenv('POSTGRES_DB', 'trading_data')
        self.user = user or os.getenv('POSTGRES_USER', 'postgres')
        self.password = password or os.getenv('POSTGRES_PASSWORD', 'postgres')
        
        self.connection_params = {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict]:
        """Execute a SELECT query and return results"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def execute_command(self, command: str, params: Optional[tuple] = None) -> int:
        """Execute an INSERT/UPDATE/DELETE command and return affected rows"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(command, params)
            return cursor.rowcount
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        if_exists: str = 'append') -> int:
        """Insert pandas DataFrame into PostgreSQL table"""
        if df.empty:
            return 0
        
        with self.get_connection() as conn:
            # Use pandas to_sql with PostgreSQL engine
            rows_inserted = df.to_sql(
                table_name, 
                conn, 
                if_exists=if_exists,
                index=False,
                method='multi'
            )
            return len(df) if rows_inserted is None else rows_inserted
    
    def read_sql(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """Execute query and return pandas DataFrame"""
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = %s
            )
        """
        result = self.execute_query(query, (table_name,))
        return result[0]['exists']