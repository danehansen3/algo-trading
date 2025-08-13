import polars as pl
from pathlib import Path

def load_message_file(path: str) -> pl.DataFrame:
    msg_cols = ["time", "type", "order_id", "size", "price", "direction"]
    return pl.read_csv(path, new_columns=msg_cols)

def load_orderbook_file(path: str, level: int = 5) -> pl.DataFrame:
    ob_cols = []
    for i in range(1, level + 1):
        ob_cols += [f"ask_price_{i}", f"ask_size_{i}", f"bid_price_{i}", f"bid_size_{i}"]
    return pl.read_csv(path, new_columns=ob_cols)

def normalize_prices(df: pl.DataFrame, level: int = 5) -> pl.DataFrame:
    normalized = [
        (pl.col("price") / 10_000).alias("price_normalized")
    ]
    for i in range(1, level + 1):
        normalized += [
            (pl.col(f"ask_price_{i}") / 10_000).alias(f"ask_price_{i}_normalized"),
            (pl.col(f"bid_price_{i}") / 10_000).alias(f"bid_price_{i}_normalized")
        ]
    return df.with_columns(normalized)

def add_engineered_features(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns([
        ((pl.col("bid_size_1") - pl.col("ask_size_1")) / 
         (pl.col("bid_size_1") + pl.col("ask_size_1") + 1e-9)).alias("imbalance_1"),
        ((pl.col("ask_price_1_normalized") + pl.col("bid_price_1_normalized")) / 2).alias("mid_price"),
        (pl.col("ask_price_1_normalized") - pl.col("bid_price_1_normalized")).alias("spread"),
        (pl.col("ask_size_1") + pl.col("bid_size_1")).alias("depth")
    ])

def preprocess_lobster_data(message_path: str, orderbook_path: str, level: int = 5) -> pl.DataFrame:
    messages = load_message_file(message_path)
    orderbook = load_orderbook_file(orderbook_path, level=level)
    df = messages.with_columns(orderbook)  # Align 1:1
    df = normalize_prices(df, level=level)
    df = add_engineered_features(df)
    return df

# === Usage Example ===
message_path = "/Users/danehansen/Desktop/algo-trading/scratchpad/ob/MSFT_2012-06-21_34200000_57600000_message_5.csv"
orderbook_path = "/Users/danehansen/Desktop/algo-trading/scratchpad/ob/MSFT_2012-06-21_34200000_57600000_orderbook_5.csv"

df = preprocess_lobster_data(message_path, orderbook_path, level=5)
print(df.head())