"""
Polars-optimized dependence and distance matrix calculations
"""

import polars as pl
import numpy as np
from typing import Union

from .correlation import (angular_distance, absolute_angular_distance, 
                         squared_angular_distance, distance_correlation)
from .information import (get_mutual_info, variation_of_information_score)
from .gnpr_distance import (spearmans_rho, gpr_distance, gnpr_distance)
from .optimal_transport import optimal_transport_dependence

def get_dependence_matrix(df: pl.DataFrame, dependence_method: str, 
                         theta: float = 0.5, n_bins: int = None, 
                         normalize: bool = True, estimator: str = 'standard',
                         target_dependence: str = 'comonotonicity',
                         gaussian_corr: float = 0.7, 
                         var_threshold: float = 0.2) -> pl.DataFrame:
    """
    Polars-optimized dependence matrix calculation.
    """
    features = df.columns
    n = len(features)
    np_data = df.to_numpy().T  # Transposed for column access
    
    # Map method to function
    method_map = {
        'information_variation': lambda x, y: variation_of_information_score(
            pl.Series(x), pl.Series(y), n_bins, normalize),
        'mutual_information': lambda x, y: get_mutual_info(
            pl.Series(x), pl.Series(y), n_bins, normalize, estimator),
        'distance_correlation': lambda x, y: distance_correlation(
            pl.Series(x), pl.Series(y)),
        'spearmans_rho': lambda x, y: spearmans_rho(
            pl.Series(x), pl.Series(y)),
        'gpr_distance': lambda x, y: gpr_distance(
            pl.Series(x), pl.Series(y), theta),
        'gnpr_distance': lambda x, y: gnpr_distance(
            pl.Series(x), pl.Series(y), theta, n_bins or 50),
        'optimal_transport': lambda x, y: optimal_transport_dependence(
            pl.Series(x), pl.Series(y), target_dependence, 
            gaussian_corr, var_threshold)
    }
    
    dep_function = method_map.get(dependence_method)
    if not dep_function:
        raise ValueError(f"Unsupported method: {dependence_method}")
    
    # Build matrix efficiently
    matrix = np.zeros((n, n))
    for i in range(n):
        for j in range(i+1):  # Only compute lower triangle
            matrix[i,j] = dep_function(np_data[i], np_data[j])
    
    # Make symmetric
    matrix = matrix + matrix.T - np.diag(matrix.diagonal())
    
    # Convert to Polars DataFrame
    if dependence_method == 'information_variation':
        matrix = 1 - matrix
        
    return pl.DataFrame(matrix, schema=features).with_columns(
        pl.Series("symbol", features)
    ).select(["symbol"] + features)

def get_distance_matrix(X: Union[pl.DataFrame, np.ndarray], 
                      distance_metric: str = 'angular') -> pl.DataFrame:
    """
    Polars-optimized distance matrix calculation.
    """
    if isinstance(X, pl.DataFrame):
        X = X.drop("symbol").to_numpy() if "symbol" in X.columns else X.to_numpy()
    
    if distance_metric == 'angular':
        dist_fun = lambda x: ((1 - x).round(5)/2)**0.5
    elif distance_metric == 'abs_angular':
        dist_fun = lambda x: ((1 - abs(x)).round(5)/2)**0.5
    elif distance_metric == 'squared_angular':
        dist_fun = lambda x: ((1 - x**2).round(5)/2)**0.5
    else:
        raise ValueError(f"Unknown metric: {distance_metric}")
    
    dist_matrix = dist_fun(X)
    return pl.DataFrame(np.nan_to_num(dist_matrix, nan=0.0), 
                      schema=X.columns if hasattr(X, 'columns') else None)