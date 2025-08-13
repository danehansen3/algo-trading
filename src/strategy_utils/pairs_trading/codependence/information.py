"""
Implementations of mutual information and variation of information
Now using Polars
"""

import polars as pl
import numpy as np
import scipy.stats as ss
from sklearn.metrics import mutual_info_score

def get_optimal_number_of_bins(num_obs: int, corr_coef: float = None) -> int:
    """Unchanged from original (pure math function)"""
    if corr_coef is None or abs(corr_coef - 1) <= 1e-4:
        z = (8 + 324*num_obs + 12*(36*num_obs + 729*num_obs**2)**0.5)**(1/3)
        bins = round(z/6 + 2/(3*z) + 1/3)
    else:
        bins = round(2**-0.5 * (1 + (1 + 24*num_obs/(1 - corr_coef**2))**0.5)**0.5)
    return int(bins)

def get_mutual_info(x: pl.Series, y: pl.Series, n_bins: int = None, 
                   normalize: bool = False, estimator: str = 'standard') -> float:
    """
    Polars-optimized mutual information calculation.
    """
    if n_bins is None:
        corr_coef = x.corr(y)
        n_bins = get_optimal_number_of_bins(len(x), corr_coef)
    
    x_np = x.to_numpy()
    y_np = y.to_numpy()
    
    if estimator == 'standard':
        contingency = np.histogram2d(x_np, y_np, n_bins)[0]
        mutual_info = mutual_info_score(None, None, contingency=contingency)
    
    elif estimator == 'standard_copula':
        x_unif = ss.rankdata(x_np)/len(x_np)
        y_unif = ss.rankdata(y_np)/len(y_np)
        contingency = np.histogram2d(x_unif, y_unif, n_bins)[0]
        mutual_info = mutual_info_score(None, None, contingency=contingency)
    
    else:  # copula_entropy
        x_unif = ss.rankdata(x_np)/len(x_np)
        y_unif = ss.rankdata(y_np)/len(y_np)
        copula_density = np.histogram2d(x_unif, y_unif, bins=n_bins, density=True)[0]
        bin_area = 1/n_bins**2
        probabilities = copula_density.ravel() + 1e-9
        mutual_info = sum(probabilities * np.log(probabilities) * bin_area)
    
    if normalize:
        x_norm = ss.rankdata(x_np)/len(x_np) if estimator != 'standard' else x_np
        y_norm = ss.rankdata(y_np)/len(y_np) if estimator != 'standard' else y_np
        marginal_x = ss.entropy(np.histogram(x_norm, n_bins)[0])
        marginal_y = ss.entropy(np.histogram(y_norm, n_bins)[0])
        mutual_info /= min(marginal_x, marginal_y)
    
    return mutual_info

def variation_of_information_score(x: pl.Series, y: pl.Series, 
                                 n_bins: int = None, normalize: bool = False) -> float:
    """
    Polars-optimized VI calculation.
    """
    if n_bins is None:
        corr_coef = x.corr(y)
        n_bins = get_optimal_number_of_bins(len(x), corr_coef)
    
    x_np = x.to_numpy()
    y_np = y.to_numpy()
    
    contingency = np.histogram2d(x_np, y_np, n_bins)[0]
    mutual_info = mutual_info_score(None, None, contingency=contingency)
    marginal_x = ss.entropy(np.histogram(x_np, n_bins)[0])
    marginal_y = ss.entropy(np.histogram(y_np, n_bins)[0])
    score = marginal_x + marginal_y - 2 * mutual_info
    
    if normalize:
        joint_dist = marginal_x + marginal_y - mutual_info
        score /= joint_dist
    
    return score