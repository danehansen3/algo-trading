"""
Correlation based distances and various modifications (angular, absolute, squared) described in Cornell lecture notes:
Codependence: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3512994&download=yes
"""

import polars as pl
import numpy as np
from scipy.spatial.distance import squareform, pdist

def angular_distance(x: pl.Series, y: pl.Series) -> float:
    """
    Returns angular distance between two vectors using Polars.
    
    Formula: sqrt(0.5 * (1 - correlation))
    """
    corr_coef = x.corr(y)
    return np.sqrt(0.5 * (1 - corr_coef))

def absolute_angular_distance(x: pl.Series, y: pl.Series) -> float:
    """
    Returns absolute angular distance between two vectors using Polars.
    
    Formula: sqrt(0.5 * (1 - abs(correlation)))
    """
    corr_coef = x.corr(y)
    return np.sqrt(0.5 * (1 - abs(corr_coef)))

def squared_angular_distance(x: pl.Series, y: pl.Series) -> float:
    """
    Returns squared angular distance between two vectors using Polars.
    
    Formula: sqrt(0.5 * (1 - correlation^2))
    """
    corr_coef = x.corr(y)
    return np.sqrt(0.5 * (1 - corr_coef ** 2))

def distance_correlation(x: pl.Series, y: pl.Series) -> float:
    """
    Returns distance correlation between two vectors using Polars.
    
    Formula: dCov[X,Y] / sqrt(dCov[X,X] * dCov[Y,Y])
    """
    # Convert to numpy for the scipy operations (temporary - see optimization note below)
    x_np = x.to_numpy()[:, None]
    y_np = y.to_numpy()[:, None]

    a = squareform(pdist(x_np))
    b = squareform(pdist(y_np))

    A = a - a.mean(axis=0)[None, :] - a.mean(axis=1)[:, None] + a.mean()
    B = b - b.mean(axis=0)[None, :] - b.mean(axis=1)[:, None] + b.mean()

    d_cov_xx = (A * A).sum() / (x_np.shape[0] ** 2)
    d_cov_xy = (A * B).sum() / (x_np.shape[0] ** 2)
    d_cov_yy = (B * B).sum() / (x_np.shape[0] ** 2)

    return np.sqrt(d_cov_xy) / np.sqrt(np.sqrt(d_cov_xx) * np.sqrt(d_cov_yy))

def kullback_leibler_distance(corr_a: pl.DataFrame | np.ndarray, 
                             corr_b: pl.DataFrame | np.ndarray) -> float:
    """
    Returns Kullback-Leibler distance between two correlation matrices.
    """
    # Convert Polars DataFrame to numpy if needed
    if isinstance(corr_a, pl.DataFrame):
        corr_a = corr_a.to_numpy()
    if isinstance(corr_b, pl.DataFrame):
        corr_b = corr_b.to_numpy()
        
    n = corr_a.shape[0]
    return 0.5 * (np.log(np.linalg.det(corr_b) / np.linalg.det(corr_a)) +
                 np.trace(np.linalg.inv(corr_b).dot(corr_a)) - n)

def norm_distance(matrix_a: pl.DataFrame | np.ndarray,
                 matrix_b: pl.DataFrame | np.ndarray,
                 r_val: int = 2) -> float:
    """
    Returns normalized distance between two matrices.
    """
    # Convert Polars DataFrame to numpy if needed
    if isinstance(matrix_a, pl.DataFrame):
        matrix_a = matrix_a.to_numpy()
    if isinstance(matrix_b, pl.DataFrame):
        matrix_b = matrix_b.to_numpy()
        
    return np.linalg.norm(matrix_b - matrix_a, r_val)