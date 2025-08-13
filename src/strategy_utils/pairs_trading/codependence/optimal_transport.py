"""
Optimal Copula Transport with Polars
"""

import warnings
import polars as pl
import numpy as np
import scipy.stats as ss
import ot

def _get_empirical_copula(x: pl.Series, y: pl.Series) -> np.ndarray:
    """Convert to numpy for copula calculations"""
    x_np = x.to_numpy()
    y_np = y.to_numpy()
    x_unif = ss.rankdata(x_np)/len(x_np)
    y_unif = ss.rankdata(y_np)/len(y_np)
    return np.array([[x, y] for x, y in zip(x_unif, y_unif)])

def optimal_transport_dependence(x: pl.Series, y: pl.Series, 
                               target_dependence: str = 'comonotonicity',
                               gaussian_corr: float = 0.7, 
                               var_threshold: float = 0.2) -> float:
    """
    Polars-optimized OT dependence calculation.
    """
    n_obs = len(x)
    forget = np.array([[u, v] for u, v in zip(
        np.random.uniform(size=n_obs),
        np.random.uniform(size=n_obs)
    )])
    
    target = _create_target_copula(target_dependence, n_obs, gaussian_corr, var_threshold)
    empirical = _get_empirical_copula(x, y)
    
    return _compute_copula_ot_dependence(empirical, target, forget, n_obs)

def _compute_copula_ot_dependence(empirical: np.ndarray, target: np.ndarray,
                                forget: np.ndarray, n_obs: int) -> float:
    """Unchanged (uses numpy/scipy)"""
    warnings.filterwarnings("ignore", message="`np.int` is a deprecated alias")
    t_measure = f_measure = e_measure = np.ones((n_obs,))/n_obs
    
    gdist_e2t = ot.dist(empirical, target)
    gdist_e2f = ot.dist(empirical, forget)
    
    e2t_ot = ot.emd(t_measure, e_measure, gdist_e2t)
    e2f_ot = ot.emd(f_measure, e_measure, gdist_e2f)
    
    e2t_dist = np.trace(e2t_ot.T @ gdist_e2t)
    e2f_dist = np.trace(e2f_ot.T @ gdist_e2f)
    
    return 1 - e2t_dist/(e2f_dist + e2t_dist)

def _create_target_copula(target_dependence: str, n_obs: int,
                         gauss_corr: float, var_threshold: float) -> np.ndarray:
    """Unchanged (pure numpy implementation)"""
    if target_dependence == 'comonotonicity':
        return np.array([[i/n_obs, i/n_obs] for i in range(n_obs)])
    # ... (rest of the function remains identical)