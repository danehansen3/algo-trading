"""
Implementation of the statistical arbitrage distance approach using Polars
Based on the original work by Gatev, E., Goetzmann, W. N., and Rouwenhorst, K. G.
"Pairs trading: Performance of a relative-value arbitrage rule" (2006)
"""

import polars as pl
import numpy as np
import matplotlib.pyplot as plt
from typing import Optional, Dict, List, Tuple, Union

class DistanceStrategy:
    """
    Polars-optimized implementation of the distance pairs trading strategy.
    """
    
    def __init__(self):
        """
        Initialize Distance strategy with Polars data structures.
        """
        self.min_normalize: Optional[pl.Series] = None
        self.max_normalize: Optional[pl.Series] = None
        self.pairs: Optional[List[Tuple[str, str]]] = None
        self.train_std: Optional[Dict[Tuple[str, str], float]] = None
        self.normalized_data: Optional[pl.DataFrame] = None
        self.portfolios: Optional[pl.DataFrame] = None
        self.train_portfolio: Optional[pl.DataFrame] = None
        self.trading_signals: Optional[pl.DataFrame] = None
        self.num_crossing: Optional[Dict[Tuple[str, str], int]] = None

    def form_pairs(self, 
                  train_data: Union[pl.DataFrame, np.ndarray],
                  method: str = 'standard',
                  industry_dict: Optional[Dict[str, str]] = None,
                  num_top: int = 5,
                  skip_top: int = 0,
                  selection_pool: int = 50,
                  list_names: Optional[List[str]] = None) -> None:
        """
        Forms pairs based on input training data using Polars.
        """
        # Convert numpy array to Polars DataFrame if needed
        if isinstance(train_data, np.ndarray):
            if list_names is None:
                list_names = [f"col_{i}" for i in range(train_data.shape[1])]
            train_data = pl.DataFrame(train_data, schema=list_names)
        
        # Normalize prices
        normalized, self.min_normalize, self.max_normalize = self._normalize_prices(train_data)
        
        # Drop null values
        normalized = normalized.drop_nulls()
        
        # Find all possible pairs
        all_pairs = self._find_pairs(normalized, industry_dict)
        
        # Sort and select pairs
        self.pairs = self._sort_pairs(all_pairs, selection_pool)
        
        # Calculate historical volatility
        self.train_std = self._find_volatility(normalized, self.pairs)
        
        # Create training portfolios
        self.train_portfolio = self._find_portfolios(normalized, self.pairs)
        
        # Count zero crossings
        self.num_crossing = self._count_zero_crossings()
        
        # Apply selection method
        self._selection_method(method, num_top, skip_top)
        
        # Filter stored data to only selected pairs
        if self.num_crossing:
            self.num_crossing = {pair: self.num_crossing[pair] for pair in self.pairs}
        if self.train_std:
            self.train_std = {pair: self.train_std[pair] for pair in self.pairs}
        if self.train_portfolio is not None and self.pairs:
            cols_to_keep = [str(pair) for pair in self.pairs]
            self.train_portfolio = self.train_portfolio.select(cols_to_keep)

    def trade_pairs(self, 
                   test_data: Union[pl.DataFrame, np.ndarray],
                   divergence: float = 2) -> None:
        """
        Generates trading signals for formed pairs using Polars.
        """
        # Convert numpy array to Polars DataFrame if needed
        if isinstance(test_data, np.ndarray):
            test_data = pl.DataFrame(
                test_data, 
                schema=self.min_normalize.to_frame().columns
            )
        
        if self.pairs is None:
            raise ValueError("Pairs are not defined. Call form_pairs() first.")
        
        # Normalize test data using training min/max
        self.normalized_data, _, _ = self._normalize_prices(
            test_data, 
            self.min_normalize, 
            self.max_normalize
        )
        
        # Create portfolios
        self.portfolios = self._find_portfolios(self.normalized_data, self.pairs)
        
        # Generate trading signals
        self.trading_signals = self._generate_signals(self.portfolios, self.train_std, divergence)

    # =====================
    # Public Getter Methods
    # =====================
    
    def get_signals(self) -> pl.DataFrame:
        """Returns trading signals DataFrame"""
        return self.trading_signals

    def get_portfolios(self) -> pl.DataFrame:
        """Returns portfolios DataFrame"""
        return self.portfolios

    def get_scaling_parameters(self) -> pl.DataFrame:
        """Returns min/max normalization values"""
        return pl.concat([
            self.min_normalize.to_frame().rename({"column_0": "min_value"}),
            self.max_normalize.to_frame().rename({"column_0": "max_value"})
        ], how="horizontal")

    def get_pairs(self) -> List[Tuple[str, str]]:
        """Returns list of pairs"""
        return self.pairs

    def get_num_crossing(self) -> Dict[Tuple[str, str], int]:
        """Returns zero crossing counts"""
        return self.num_crossing

    # =====================
    # Plotting Methods
    # =====================
    
    def plot_portfolio(self, num_pair: int) -> plt.Figure:
        """Plots portfolio values and signals"""
        pair_name = self.trading_signals.columns[num_pair]
        
        fig, axs = plt.subplots(2, 1, gridspec_kw={'height_ratios': [3, 1]}, figsize=(10, 7))
        fig.suptitle(f'Distance Strategy results for portfolio {pair_name}')
        
        # Portfolio value plot
        axs[0].plot(self.portfolios[pair_name].to_numpy())
        axs[0].set_title('Portfolio value (difference between element prices)')
        
        # Signals plot
        axs[1].plot(self.trading_signals[pair_name].to_numpy(), '#b11a21')
        axs[1].set_title('Number of portfolio units to hold')
        
        return fig

    def plot_pair(self, num_pair: int) -> plt.Figure:
        """Plots normalized prices and signals"""
        pair_name = self.trading_signals.columns[num_pair]
        pair_val = tuple(pair_name.strip('\')(\'').split('\', \''))
        
        fig, axs = plt.subplots(2, 1, gridspec_kw={'height_ratios': [3, 1]}, figsize=(10, 7))
        fig.suptitle(f'Distance Strategy results for pair {pair_name}')
        
        # Prices plot
        axs[0].plot(
            self.normalized_data[pair_val[0]].to_numpy(), 
            label=f"Long asset - {pair_val[0]}"
        )
        axs[0].plot(
            self.normalized_data[pair_val[1]].to_numpy(), 
            label=f"Short asset - {pair_val[1]}"
        )
        axs[0].legend()
        axs[0].set_title('Normalized prices of elements in portfolio')
        
        # Signals plot
        axs[1].plot(self.trading_signals[pair_name].to_numpy(), '#b11a21')
        axs[1].set_title('Number of portfolio units to hold')
        
        return fig

    # =====================
    # Core Private Methods
    # =====================
    
    def _selection_method(self, method: str, num_top: int, skip_top: int) -> None:
        """Select pairs based on specified method"""
        if method not in ['standard', 'zero_crossing', 'variance']:
            raise ValueError("Method must be 'standard', 'zero_crossing', or 'variance'")
        
        if method == 'standard':
            self.pairs = self.pairs[skip_top:(skip_top + num_top)]
        elif method == 'zero_crossing':
            sorted_pairs = sorted(self.num_crossing.items(), key=lambda x: x[1], reverse=True)
            self.pairs = [x[0] for x in sorted_pairs[skip_top:(skip_top + num_top)]]
        else:  # variance
            sorted_pairs = sorted(self.train_std.items(), key=lambda x: x[1], reverse=True)
            self.pairs = [x[0] for x in sorted_pairs[skip_top:(skip_top + num_top)]]

    def _count_zero_crossings(self) -> Dict[Tuple[str, str], int]:
        """Count zero crossings in training portfolios"""
        zero_crossings = {}
        
        for pair in self.train_portfolio.columns:
            pair_val = tuple(pair.strip('\')(\'').split('\', \''))
            portfolio = self.train_portfolio[pair]
            
            # Calculate zero crossings
            shifted = portfolio.shift(1)
            crossings = ((portfolio * shifted) <= 0).sum() - 1  # Subtract initial NaN
            
            zero_crossings[pair_val] = crossings
            
        return zero_crossings

    @staticmethod
    def _normalize_prices(
        data: pl.DataFrame,
        min_values: Optional[pl.Series] = None,
        max_values: Optional[pl.Series] = None
    ) -> Tuple[pl.DataFrame, pl.Series, pl.Series]:
        """Normalize price data using Polars"""
        if min_values is None or max_values is None:
            min_values = data.min()
            max_values = data.max()
        
        normalized = (data - min_values) / (max_values - min_values)
        return normalized, min_values, max_values

    @staticmethod
    def _find_pairs(
        data: pl.DataFrame,
        industry_dict: Optional[Dict[str, str]] = None
    ) -> Dict[Tuple[str, str], float]:
        """Find all possible pairs with their distances"""
        pairs = {}
        columns = data.columns
        
        for i, col1 in enumerate(columns):
            # Filter by industry if dictionary provided
            if industry_dict:
                industry = industry_dict.get(col1)
                eligible_cols = [
                    col for col in columns[i+1:] 
                    if industry_dict.get(col) == industry
                ]
            else:
                eligible_cols = columns[i+1:]
            
            # Calculate squared differences
            for col2 in eligible_cols:
                diff = (data[col1] - data[col2]).pow(2).sum()
                pairs[(col1, col2)] = diff
                
        return pairs

    @staticmethod
    def _sort_pairs(
        pairs: Dict[Tuple[str, str], float],
        num_top: int,
        skip_top: int = 0
    ) -> List[Tuple[str, str]]:
        """Sort pairs by distance and select top ones"""
        sorted_pairs = sorted(pairs.items(), key=lambda x: x[1])
        return [pair[0] for pair in sorted_pairs[skip_top:(skip_top + num_top)]]

    @staticmethod
    def _find_volatility(
        data: pl.DataFrame,
        pairs: List[Tuple[str, str]]
    ) -> Dict[Tuple[str, str], float]:
        """Calculate historical volatility for each pair"""
        volatility = {}
        
        # DEBUGGING: Print the data DataFrame being passed in
        print("\nDEBUG_VOLATILITY: Data DataFrame head (first 5 rows):")
        print(data.head())
        print(f"DEBUG_VOLATILITY: Data DataFrame shape: {data.shape}")
        
        for pair in pairs:
            print(f"\nDEBUG_VOLATILITY: Processing pair: {pair}")
            
            # Check if columns exist in the data
            if pair[0] not in data.columns or pair[1] not in data.columns:
                print(f"DEBUG_VOLATILITY ERROR: One or both columns ({pair[0]}, {pair[1]}) not found in data.")
                volatility[pair] = None # Explicitly set to None for debugging visibility
                continue

            spread = data[pair[0]] - data[pair[1]]
            
            # DEBUGGING: Print the spread Series information
            print("DEBUG_VOLATILITY: Spread Series head:")
            print(spread.head())
            print(f"DEBUG_VOLATILITY: Spread Series length: {len(spread)}")
            print(f"DEBUG_VOLATILITY: Number of nulls in spread: {spread.is_null().sum()}")
            print(f"DEBUG_VOLATILITY: All values in spread are NaN/Null: {spread.is_null().all()}") # This is the critical check
            
            std_dev = spread.std()
            volatility[pair] = std_dev
            
            # DEBUGGING: Print the calculated std_dev
            print(f"DEBUG_VOLATILITY: Calculated std_dev for {pair}: {std_dev}")
            
        return volatility

    @staticmethod
    def _find_portfolios(
        data: pl.DataFrame,
        pairs: List[Tuple[str, str]]
    ) -> pl.DataFrame:
        """Create portfolio spreads for each pair"""
        portfolio_data = {}
        
        for pair in pairs:
            portfolio_data[str(pair)] = data[pair[0]] - data[pair[1]]
            
        return pl.DataFrame(portfolio_data)

    @staticmethod
    def _generate_signals(
        portfolios: pl.DataFrame,
        variation: Dict[Tuple[str, str], float],
        divergence: float
    ) -> pl.DataFrame:
        """Generate trading signals based on divergence thresholds"""
        signals = {}
        
        for pair in portfolios.columns:
            pair_val = tuple(pair.strip('\')(\'').split('\', \''))
            st_dev = variation[pair_val]
            portfolio = portfolios[pair]
            
            # Calculate signals
            shifted = portfolio.shift(1)
            cross_zero = (portfolio * shifted) <= 0
            
            # Long signals (spread is too low)
            long_entries = portfolio < -divergence * st_dev
            long_exits = cross_zero & (shifted < 0)  # Was negative before crossing
            
            # Short signals (spread is too high)
            short_entries = portfolio > divergence * st_dev
            short_exits = cross_zero & (shifted > 0)  # Was positive before crossing
            
            # Combine signals
            signals[pair] = (
                pl.when(long_entries).then(1)
                .when(short_entries).then(-1)
                .when(long_exits | short_exits).then(0)
                .otherwise(None)
                .forward_fill()
                .fill_null(0)
            )
            
        return pl.DataFrame(signals)