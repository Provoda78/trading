import numpy as np


class GaussianMixtureEM:
    def __init__(
        self,
        n_components=2,
        max_iter=100,
        tol=1e-4,
        reg_covar=1e-6,
        random_state=None
    ):
        self.n_components = n_components
        self.max_iter = max_iter
        self.tol = tol
        self.reg_covar = reg_covar
        self.random_state = random_state
        self.rng = np.random.default_rng(random_state)

        self.weights_ = None   # pi_k
        self.means_ = None     # mu_k
        self.covariances_ = None  # Sigma_k
        self.resp_ = None      # responsibilities r_nk
        self.lower_bound_ = None
        
        