"""
Implementation of distance using Generic Non-Parametric Representation approach
Now using Polars for better performance
"""

import polars as pl
import numpy as np
from scipy.stats import spearmanr
import ot

def spearmans_rho(x: pl.Series, y: pl.Series) -> float:
    """
    Calculates Spearman's rho using Polars optimized operations.
    """
    rho, _ = spearmanr(x.to_numpy(), y.to_numpy())
    return rho

def gpr_distance(x: pl.Series, y: pl.Series, theta: float) -> float:
    """
    Calculates GPR distance with Polars.
    """
    rho = spearmans_rho(x, y)
    x_std = x.std()
    y_std = y.std()
    mean_diff = (x.mean() - y.mean())**2
    
    distance = theta * (1 - rho)/2 + (1 - theta) * (
        1 - ((2 * x_std * y_std) / (x_std**2 + y_std**2))**0.5 *
        np.exp(-0.25 * mean_diff / (x_std**2 + y_std**2))
    )
    return distance**0.5

def gnpr_distance(x: pl.Series, y: pl.Series, theta: float, n_bins: int = 50) -> float:
    """
    Calculates GNPR distance with Polars and optimized binning.
    """
    num_obs = len(x)
    
    # Calculate d1 distance using Polars
    dist_1 = 3/(num_obs*(num_obs**2-1)) * (
        ((x - y)**2).sum()
    )
    
    # Binning with Polars
    x_np = x.to_numpy()
    y_np = y.to_numpy()
    
    x_binned = pl.Series(np.histogram(x_np, bins=n_bins)[0]) / num_obs
    y_binned = pl.Series(np.histogram(y_np, bins=n_bins)[0]) / num_obs
    
    # Optimal transport (still using numpy/scipy)
    bins = np.linspace(0, 1, n_bins)
    loss_matrix = ot.dist(bins.reshape((n_bins, 1)), bins.reshape((n_bins, 1)))
    loss_matrix /= loss_matrix.max()
    
    ot_matrix = ot.emd(
        x_binned.sort().to_numpy(), 
        y_binned.sort().to_numpy(), 
        loss_matrix
    )
    
    dist_0 = np.trace(np.dot(ot_matrix.T, loss_matrix))
    distance = theta * dist_1 + (1 - theta) * dist_0
    
    return distance**0.5