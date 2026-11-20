import numpy as np
from itertools import product
from abc import abstractmethod, ABC

class BaseHMM(ABC):
    
    @abstractmethod
    def forward(self, obs, distributions=None):
        pass
    
    @abstractmethod
    def backward(self, obs, c, distributions=None):
        pass
    
    @abstractmethod
    def e_step(self, obs, distributions=None):
        pass
    
    @abstractmethod
    def m_step(self, sequences, e_results):
        pass

    @abstractmethod
    def fit(self, sequences, n_iter=100, tol=1e-4):
        pass
    
    @abstractmethod
    def predict(self, obs):
        pass

class HMM(BaseHMM):
    """HMM с дискретными наблюдениями 0..n_symbols-1."""

    def __init__(self, n_states, n_symbols, startprob=None,
                 transmat=None, emissionprob=None, random_state=None):
        
        self.n_states = n_states #число скрытых состояний N
        self.n_symbols = n_symbols #размер алфавита наблюдений K
        self.rng = np.random.default_rng(random_state)

        self.pi = self._init_dist((n_states,), startprob, "startprob")
        self.A = self._init_dist((n_states, n_states), transmat, "transmat")
        self.B = self._init_dist((n_states, n_symbols), emissionprob, "emissionprob")

    def _init_dist(self, shape, given, name):
        if given is None:
            M = self.rng.random(shape) + 0.1
        else:
            M = np.array(given, dtype=float)
            if M.shape != shape:
                raise ValueError(f"{name}: ожидается форма {shape}, получена {M.shape}")
            if np.any(M < 0):
                raise ValueError(f"{name}: есть отрицательные вероятности")
        M /= M.sum(axis=-1, keepdims=True)
        return M
    
    def forward(self, obs, distributions=None):

        obs = np.asarray(obs)
        T = len(obs)
        N = self.n_states

        alpha = np.zeros((T, N))
        c = np.zeros(T)

        # t = 0: инициализация
        if distributions is None:
            distribution_0 = self.B[:, obs[0]]
        else:
            distribution_0 = distributions[0]
        alpha[0] = np.multiply(self.pi, distribution_0)
        c[0] = np.sum(alpha[0])
        alpha[0] /= c[0]

        # t = 1 .. T-1: рекуррентный шаг
        for t in range(1, T):
            if distributions is None:
                distribution_t = self.B[:, obs[t]]
            else:
                distribution_t = distributions[t]
                            
            alpha[t] = (alpha[t-1] @ self.A) * distribution_t  
            c[t] = np.sum(alpha[t])
            alpha[t] /= c[t]

        loglik = np.sum(np.log(c))
        return alpha, c, loglik
    
    def backward(self, obs, c, distributions=None):
    
        obs = np.asarray(obs)
        T = len(obs)
        N = self.n_states

        beta = np.zeros((T, N))

        # t = T-1: инициализация
        beta[T-1] = np.ones(N)

        # t = T-2 .. 0: рекуррентный шаг справа налево
        for t in range(T-2, -1, -1):
            if distributions is None:
                distribution = self.B[:, obs[t+1]]
            else:
                distribution = distributions[t+1]
                            
            beta[t] = self.A @ (beta[t+1] * distribution)
            beta[t] /= c[t+1]

        return beta
    
    def e_step(self, obs, distributions=None):
  
        obs = np.asarray(obs)
        T = len(obs)
        N = self.n_states

        alpha, c, loglik = self.forward(obs, distributions=distributions)
        beta = self.backward(obs, c, distributions=distributions)
        
        #gamma [T N]
        gamma = alpha * beta

        xi = np.zeros((T-1, N, N))
        for t in range(T-1):
            if distributions is None:
                distribution = self.B[:, obs[t+1]]
            else:
                distribution = distributions[t+1]
            
            #xi[N-1 N N]
            # xi_t(i,j) ~ alpha_t(i) * a_ij * b_j(o_{t+1}) * beta_{t+1}(j)
            xi[t] = alpha[t][:, None] * self.A * (distribution * beta[t+1])
            xi[t] /= c[t+1]

        return gamma, xi, loglik
    
    def m_step(self, sequences, e_results):
    
        N, K = self.n_states, self.n_symbols

        pi_acc = np.zeros(N)         # числитель Pi
        xi_acc = np.zeros((N, N))    # числиьель a_ij
        gam_trans = np.zeros(N)      # знаменатель a_ij
        emit_acc = np.zeros((N, K))  # числитель b_ij
        gam_all = np.zeros(N)        # знаменатель b_ij

        for obs, (gamma, xi, _) in zip(sequences, e_results):
            obs = np.asarray(obs)

            pi_acc += gamma[0]
            xi_acc += np.sum(xi, axis=0)
            gam_trans += np.sum(gamma[:-1], axis=0)
            for k in range(K):
                emit_acc[:, k] += np.sum(gamma[obs==k], axis=0)             
            gam_all += np.sum(gamma, axis=0)

        self.pi = pi_acc / len(sequences)
        self.A = xi_acc / gam_trans[:, None]
        self.B = emit_acc / gam_all[:, None]
    
    def fit(self, sequences, n_iter=100, tol=1e-4):
        history = []
        for i in range(n_iter):
            e_results = [self.e_step(obs) for obs in sequences]
            history.append(sum(r[2] for r in e_results))
            self.m_step(sequences, e_results)
            if i > 0 and abs(history[-1] - history[-2]) < tol:
                break
        return history
    
    def predict(self, obs):
                obs = np.asarray(obs)
                T = len(obs)
                N = self.n_states
        
                with np.errstate(divide="ignore"):
                    logpi = np.log(self.pi)
                    logA = np.log(self.A)
                    logB = np.log(self.B)
        
                delta = np.zeros((T, N))
                psi = np.zeros((T, N), dtype=int)    # psi[t, j]: лучшее предыдущее состояние для (t, j)
        
                # t = 0: инициализация
                delta[0] = logpi + logB[:, obs[0]]
        
                # t = 1 .. T-1: рекуррентный шаг
                for t in range(1, T):
                    scores = delta[t-1][:, None] + logA
                    psi[t] = np.argmax(scores, axis=0)
                    delta[t] = np.max(scores, axis=0) + logB[:, obs[t]]
        
                path = np.zeros(T, dtype=int)
                path[T-1] = np.argmax(delta[T-1])
                logprob = np.max(delta[T-1])
                for t in range(T-2, -1, -1):
                    path[t] = psi[t+1][path[t+1]]
        
                return path, logprob
    
class GaussianHMM(HMM):
    def __init__(self, n_states, n_features, startprob=None, transmat=None,
                 random_state=None, reg=1e-4):
        super().__init__(
            n_states,
            n_features,
            startprob=startprob,
            transmat=transmat,
            random_state=random_state,
        )
        self.D = n_features
        self.reg = reg
        self.means = None
        self.covars = None

    def init_emissions(self, sequences):
        X = np.concatenate(sequences)
        N, D = self.n_states, self.D

        idx = self.rng.choice(len(X), size=N, replace=False)
        self.means = X[idx].astype(float) # (N, D)

        cov = np.atleast_2d(np.cov(X.T)) + self.reg * np.eye(D)
        self.covars = np.tile(cov, (N, 1, 1)) # (N, D, D)

        assert self.means.shape == (N, D)

    def _emission(self, obs):
        """Возвращает (T, N): [t, i] = N(obs[t]; means[i], covars[i])."""
        return np.exp(self._log_emission(obs))

    def _log_emission(self, obs):
        T = len(obs)
        logp = np.zeros((T, self.n_states))
        for i in range(self.n_states):
            diff = obs - self.means[i] # (T, D)

            logp[:, i] = - 0.5 * (self.D * np.log(2 * np.pi) + 
                                  np.linalg.slogdet(self.covars[i])[1] + 
                                  np.sum(diff.T * np.linalg.solve(self.covars[i], diff.T), axis=0))
            
        return logp

    def _m_step_emissions(self, sequences, e_results):
        X = np.concatenate(sequences)
        G = np.concatenate([r[0] for r in e_results])
        Nk = G.sum(axis=0)
    
        self.means = G.T @ X / Nk[:, None]
        for i in range(self.n_states):
            diff = X - self.means[i]
            self.covars[i] = ((G[:, i, None] * diff).T @ diff / Nk[i] 
                              + self.reg * np.eye(self.D))
            
    def forward(self, obs, distributions=None):
        eb = self._emission(obs) if distributions is None else distributions
        return super().forward(obs, distributions=eb)
        
    def backward(self, obs, c, distributions=None):
        eb = self._emission(obs) if distributions is None else distributions
        return super().backward(obs, c, distributions=eb)
        
    def e_step(self, obs, distributions=None):
        eb = self._emission(obs) if distributions is None else distributions
        return super().e_step(obs, distributions=eb)   
    
    def m_step(self, sequences, e_results):
        N = self.n_states
        pi_acc = np.zeros(N)
        xi_acc = np.zeros((N, N))
        gam_trans = np.zeros(N)

        for gamma, xi, _ in e_results:
            pi_acc += gamma[0]
            xi_acc += np.sum(xi, axis=0)
            gam_trans += np.sum(gamma[:-1], axis=0)

        self.pi = pi_acc / len(sequences)
        self.A = xi_acc / gam_trans[:, None]
        self._m_step_emissions(sequences, e_results)
    
    def fit(self, sequences, n_iter=100, tol=1e-4):
        history = []
        if self.means is None:
            self.init_emissions(sequences)

        for i in range(n_iter):
            e_results = [self.e_step(obs) for obs in sequences]
            history.append(sum(r[2] for r in e_results))
            self.m_step(sequences, e_results)
            if i > 0 and abs(history[-1] - history[-2]) < tol:
                break
        return history
    
    def predict(self, obs):
            obs = np.asarray(obs)
            T = len(obs)
            N = self.n_states
            log_emissions = self._log_emission(obs)
    
            with np.errstate(divide="ignore"):
                logpi = np.log(self.pi)
                logA = np.log(self.A)
    
            delta = np.zeros((T, N))
            psi = np.zeros((T, N), dtype=int)
            delta[0] = logpi + log_emissions[0]
    
            for t in range(1, T):
                scores = delta[t - 1][:, None] + logA
                psi[t] = np.argmax(scores, axis=0)
                delta[t] = np.max(scores, axis=0) + log_emissions[t]
    
            path = np.zeros(T, dtype=int)
            path[T - 1] = np.argmax(delta[T - 1])
            logprob = np.max(delta[T - 1])
            for t in range(T - 2, -1, -1):
                path[t] = psi[t + 1][path[t + 1]]
    
            return path, logprob
        
        
def brute_force(m, obs):
    total, best, best_q = 0.0, -1.0, None
    for q in product(range(m.n_states), repeat=len(obs)):
        p = m.pi[q[0]] * m.B[q[0], obs[0]]
        for t in range(1, len(obs)):
            p *= m.A[q[t-1], q[t]] * m.B[q[t], obs[t]]
        total += p
        if p > best:
            best, best_q = p, q
    return total, list(best_q), np.log(best)

m = HMM(3, 4, random_state=1)
obs = np.array([0, 2, 1, 3, 3, 0])

# 1. forward и viterbi против брутфорса
alpha, c, ll = m.forward(obs)
total, bq, blp = brute_force(m, obs)
path, lp = m.predict(obs)
print(np.isclose(np.exp(ll), total))
print(path.tolist() == bq, np.isclose(lp, blp))

# 2. согласованность forward/backward/e_step
beta = m.backward(obs, c)
gamma, xi, _ = m.e_step(obs)
print(np.allclose((alpha * beta).sum(1), 1))
print(np.allclose(gamma.sum(1), 1), np.allclose(xi.sum((1, 2)), 1))
print(np.allclose(xi.sum(2), gamma[:-1]))

# 3. обучение: правдоподобие не убывает, параметры нормированы
seqs = [np.array([0,1,2,2,1,0,0,1]), np.array([2,2,1,0,0,0,1,2,2])]
m2 = HMM(2, 3, random_state=0)
hist = m2.fit(seqs, n_iter=50, tol=0)
print(np.all(np.diff(hist) > -1e-9))
print(m2.pi.sum(), m2.A.sum(1), m2.B.sum(1))