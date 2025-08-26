'''
Function that retrieves current gainers from finviz matching the criteria of the following URL:
https://finviz.com/screener.ashx?v=111&f=ta_gap_u10,sh_relvol_o5,sh_price_o2,sh_price_u20,sh_float_o10,sh_float_u20&ft=4
'''

import pandas as pd
from finvizfinance.screener.overview import Overview
from finvizfinance.quote import finvizfinance
from datetime import datetime, timedelta
import time
from typing import List, Dict, Any

def get_current_news(tickers: List[str], days_back: int = 2) -> Dict[str, Dict[str, Any]]:
    """
    Check for recent news for given tickers within specified days
    
    Args:
        tickers (List[str]): List of stock tickers to check
        days_back (int): Number of days to look back for news (default: 2)
    
    Returns:
        Dict[str, Dict[str, Any]]: Dictionary with ticker as key and news info as value
                                   Format: {ticker: {'has_news': bool, 'news_titles': List[str]}}
    """
    news_results = {}
    cutoff_date = datetime.now() - timedelta(days=days_back)
    
    print(f"Checking news for {len(tickers)} tickers within last {days_back} days...")
    
    
    for ticker in tickers:
        try:
            current_stock = finvizfinance(ticker)
            
            # Add small delay to avoid rate limiting
            time.sleep(0.5)
            
            # Get news data for specific ticker
            news_df = current_stock.ticker_news()
            
            if news_df is not None and not news_df.empty:
                # Filter news by date (within last X days)
                recent_news = []
                news_titles = []
                
                for idx, row in news_df.iterrows():
                    try:
                        # Parse the date from finviz format
                        news_date_str = row.get('Date', '')
                        if news_date_str:
                            # finviz date format is typically "MMM-DD-YY HH:MM[AM/PM]"
                            news_date = datetime.strptime(news_date_str.split()[0], '%b-%d-%y')
                            
                            if news_date >= cutoff_date.replace(hour=0, minute=0, second=0, microsecond=0):
                                recent_news.append(row)
                                news_titles.append(row.get('Title', 'No title available'))
                    except:
                        # If date parsing fails, include the news item to be safe
                        recent_news.append(row)
                        news_titles.append(row.get('Title', 'No title available'))
                
                has_news = len(recent_news) > 0
                news_results[ticker] = {
                    'has_news': has_news,
                    'news_titles': news_titles[:3]  # Limit to first 3 news items
                }
                
                print(f"{ticker}: {'✓' if has_news else '✗'} ({'Found' if has_news else 'No'} recent news)")
                
            else:
                news_results[ticker] = {
                    'has_news': False,
                    'news_titles': []
                }
                print(f"{ticker}: ✗ (No news data available)")
                
        except Exception as e:
            print(f"Error getting news for {ticker}: {e}")
            news_results[ticker] = {
                'has_news': False,
                'news_titles': []
            }
    
    return news_results


def get_current_gainers() -> pd.DataFrame:
    """
    Screen stocks based on specific criteria using finvizfinance
    Also checks for recent news and appends to dataframe
    
    Criteria:
    - Gapped up 10%+
    - 5x+ relative volume
    - Price between $2-$20
    - Shares outstanding between 10M-20M
    
    Returns:
        pd.DataFrame: Filtered stocks with news information
    """
    try:
        # Initialize the screener
        fviz = Overview()
        
        # Set filters based on requirements
        filters_dict = {
            'Gap': 'Up 10%',           # Gapped up 10%+
            'Relative Volume': 'Over 5', # Relative volume over 5
            'Price': 'Over $2',       # Price between $2-$20
            'Price': 'Under $20',       # Price between $2-$20
            'Shares Outstanding': 'Over 10M',  # Shares outstanding 10M-20M
            'Shares Outstanding': 'Under 20M'  # Shares outstanding 10M-20M
        }
        
        # Apply filters
        fviz.set_filter(filters_dict=filters_dict)
        
        # Get the filtered data
        df = fviz.screener_view()
        
        if df is not None and not df.empty:
            print(f"Found {len(df)} stocks matching criteria")
            
            # Extract tickers for news checking
            tickers = df['Ticker'].tolist()
            
            # Get news for filtered tickers
            news_results = get_current_news(tickers, days_back=2)
            
            # Add news information to the dataframe
            df['has_recent_news'] = df['Ticker'].map(
                lambda x: news_results.get(x, {}).get('has_news', False)
            )
            df['recent_news_titles'] = df['Ticker'].map(
                lambda x: ' | '.join(news_results.get(x, {}).get('news_titles', []))
            )
            
            return df
        else:
            print("No stocks found matching the criteria")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"Error in stock screening: {e}")
        return pd.DataFrame()

# Simple execution
if __name__ == "__main__":
    # Execute screen_stocks function
    result_df = get_current_gainers()
    
    # Print results
    if not result_df.empty:
        print(f"\n=== SCREENING RESULTS ===")
        print(f"Total stocks found: {len(result_df)}")
        print(f"Stocks with recent news: {sum(result_df['has_recent_news'])}")
        print(f"\nDetailed Results:")
        print("-" * 80)
        
        # Print each stock with its info
        for _, row in result_df.iterrows():
            print(f"Ticker: {row['Ticker']}")
            print(f"Company: {row.get('Company', 'N/A')}")
            print(f"Price: ${row.get('Price', 'N/A')}")
            print(f"Change: {row.get('Change', 'N/A')}")
            print(f"Has Recent News: {row['has_recent_news']}")
            if row['recent_news_titles']:
                print(f"News Titles: {row['recent_news_titles'][:200]}...")
            print("-" * 40)
    else:
        print("No results to display")