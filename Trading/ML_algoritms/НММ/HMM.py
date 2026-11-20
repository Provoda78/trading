import numpy as np
from itertools import product
from abc import abstractmethod, ABC

class BaseHMM(ABC):
    def __init__(self, n_states, startprob, transmat, random_state):
        self.n_states = n_states
        self.rng = np.random.default_rng(random_state)
        self.pi = self._init_dist((n_states,), startprob, "startprob")
        self.A = self._init_dist((n_states, n_states), transmat, "transmat")

    def _init_dist(self, shape, given, name):
        if given is None:
            values = self.rng.random(shape) + 0.1
        else:
            values = np.array(given, dtype=float)
            if values.shape != shape:
                raise ValueError(f"{name}: ожидается форма {shape}, получена {values.shape}")
            if np.any(values < 0):
                raise ValueError(f"{name}: есть отрицательные вероятности")
        values /= values.sum(axis=-1, keepdims=True)
        return values

    @abstractmethod
    def _emission_probabilities(self, obs):
        pass

    @abstractmethod
    def _log_emission_probabilities(self, obs):
        pass

    @abstractmethod
    def _initialize_emissions(self, sequences):
        pass

    @abstractmethod
    def _m_step_emissions(self, sequences, e_results):
        pass

    def forward(self, obs, distributions=None):
        obs = np.asarray(obs)
        T = len(obs)
        N = self.n_states
        if distributions is None:
            distributions = self._emission_probabilities(obs)

        alpha = np.zeros((T, N))
        c = np.zeros(T)

        alpha[0] = np.multiply(self.pi, distributions[0])
        c[0] = np.sum(alpha[0])
        alpha[0] /= c[0]

        for t in range(1, T):
            alpha[t] = (alpha[t-1] @ self.A) * distributions[t]
            c[t] = np.sum(alpha[t])
            alpha[t] /= c[t]

        loglik = np.sum(np.log(c))
        return alpha, c, loglik

    def backward(self, obs, c, distributions=None):
        obs = np.asarray(obs)
        T = len(obs)
        N = self.n_states
        if distributions is None:
            distributions = self._emission_probabilities(obs)

        beta = np.zeros((T, N))
        beta[T-1] = np.ones(N)

        for t in range(T-2, -1, -1):
            beta[t] = self.A @ (beta[t+1] * distributions[t+1])
            beta[t] /= c[t+1]

        return beta

    def e_step(self, obs, distributions=None):
        obs = np.asarray(obs)
        T = len(obs)
        N = self.n_states
        if distributions is None:
            distributions = self._emission_probabilities(obs)

        alpha, c, loglik = self.forward(obs, distributions=distributions)
        beta = self.backward(obs, c, distributions=distributions)
        gamma = alpha * beta

        xi = np.zeros((T-1, N, N))
        for t in range(T-1):
            xi[t] = (
                alpha[t][:, None]
                * self.A
                * (distributions[t+1] * beta[t+1])
            )
            xi[t] /= c[t+1]

        return gamma, xi, loglik

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
        self._initialize_emissions(sequences)
        for i in range(n_iter):
            e_results = [self.e_step(obs) for obs in sequences]
            history.append(sum(result[2] for result in e_results))
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
        log_emissions = self._log_emission_probabilities(obs)

        delta = np.zeros((T, N))
        psi = np.zeros((T, N), dtype=int)
        delta[0] = logpi + log_emissions[0]

        for t in range(1, T):
            scores = delta[t-1][:, None] + logA
            psi[t] = np.argmax(scores, axis=0)
            delta[t] = np.max(scores, axis=0) + log_emissions[t]

        path = np.zeros(T, dtype=int)
        path[T-1] = np.argmax(delta[T-1])
        logprob = np.max(delta[T-1])
        for t in range(T-2, -1, -1):
            path[t] = psi[t+1][path[t+1]]

        return path, logprob


class CategoricalEmission:
    def __init__(self, n_states, n_symbols, rng, emissionprob=None):
        self.n_symbols = n_symbols
        self.B = self._init_dist(
            (n_states, n_symbols), emissionprob, "emissionprob", rng
        )

    @staticmethod
    def _init_dist(shape, given, name, rng):
        if given is None:
            values = rng.random(shape) + 0.1
        else:
            values = np.array(given, dtype=float)
            if values.shape != shape:
                raise ValueError(f"{name}: ожидается форма {shape}, получена {values.shape}")
            if np.any(values < 0):
                raise ValueError(f"{name}: есть отрицательные вероятности")
        values /= values.sum(axis=-1, keepdims=True)
        return values

    def probabilities(self, obs):
        return self.B[:, obs].T

    def log_probabilities(self, obs):
        with np.errstate(divide="ignore"):
            return np.log(self.probabilities(obs))

    def initialize(self, sequences):
        pass

    def m_step(self, sequences, e_results):
        n_states = self.B.shape[0]
        emit_acc = np.zeros_like(self.B)
        gam_all = np.zeros(n_states)

        for obs, (gamma, _, _) in zip(sequences, e_results):
            obs = np.asarray(obs)
            for symbol in range(self.n_symbols):
                emit_acc[:, symbol] += np.sum(gamma[obs == symbol], axis=0)
            gam_all += np.sum(gamma, axis=0)

        self.B = emit_acc / gam_all[:, None]


class GaussianEmission:
    def __init__(self, n_states, n_features, rng, reg=1e-4):
        self.n_states = n_states
        self.n_features = n_features
        self.rng = rng
        self.reg = reg
        self.means = None
        self.covars = None

    def initialize(self, sequences):
        X = np.concatenate(sequences)
        N, D = self.n_states, self.n_features

        idx = self.rng.choice(len(X), size=N, replace=False)
        self.means = X[idx].astype(float)

        cov = np.atleast_2d(np.cov(X.T)) + self.reg * np.eye(D)
        self.covars = np.tile(cov, (N, 1, 1))

        assert self.means.shape == (N, D)

    def log_probabilities(self, obs):
        T = len(obs)
        logp = np.zeros((T, self.n_states))
        for i in range(self.n_states):
            diff = obs - self.means[i]
            logp[:, i] = -0.5 * (
                self.n_features * np.log(2 * np.pi)
                + np.linalg.slogdet(self.covars[i])[1]
                + np.sum(
                    diff.T * np.linalg.solve(self.covars[i], diff.T), axis=0
                )
            )
        return logp

    def probabilities(self, obs):
        return np.exp(self.log_probabilities(obs))

    def m_step(self, sequences, e_results):
        X = np.concatenate(sequences)
        G = np.concatenate([result[0] for result in e_results])
        Nk = G.sum(axis=0)

        self.means = G.T @ X / Nk[:, None]
        for i in range(self.n_states):
            diff = X - self.means[i]
            self.covars[i] = (
                (G[:, i, None] * diff).T @ diff / Nk[i]
                + self.reg * np.eye(self.n_features)
            )

class HMM(BaseHMM):
    """HMM с дискретными наблюдениями 0..n_symbols-1."""

    def __init__(self, n_states, n_symbols, startprob=None,
                 transmat=None, emissionprob=None, random_state=None):
        
        super().__init__(n_states, startprob, transmat, random_state)
        self.n_symbols = n_symbols
        self.emission_model = CategoricalEmission(
            n_states, n_symbols, self.rng, emissionprob
        )

    @property
    def B(self):
        return self.emission_model.B

    @B.setter
    def B(self, value):
        self.emission_model.B = value

    def _emission_probabilities(self, obs):
        return self.emission_model.probabilities(obs)

    def _log_emission_probabilities(self, obs):
        return self.emission_model.log_probabilities(obs)

    def _initialize_emissions(self, sequences):
        self.emission_model.initialize(sequences)

    def _m_step_emissions(self, sequences, e_results):
        self.emission_model.m_step(sequences, e_results)
    
class GaussianHMM(BaseHMM):
    def __init__(self, n_states, n_features, startprob=None, transmat=None,
                 random_state=None, reg=1e-4):
        super().__init__(n_states, startprob, transmat, random_state)
        self.n_features = n_features
        self.D = n_features
        self.emission_model = GaussianEmission(n_states, n_features, self.rng, reg)

    @property
    def means(self):
        return self.emission_model.means

    @means.setter
    def means(self, value):
        self.emission_model.means = value

    @property
    def covars(self):
        return self.emission_model.covars

    @covars.setter
    def covars(self, value):
        self.emission_model.covars = value

    def init_emissions(self, sequences):
        self.emission_model.initialize(sequences)

    def _emission(self, obs):
        return self.emission_model.probabilities(obs)

    def _log_emission(self, obs):
        return self.emission_model.log_probabilities(obs)

    def _m_step_emissions(self, sequences, e_results):
        self.emission_model.m_step(sequences, e_results)

    def _emission_probabilities(self, obs):
        return self.emission_model.probabilities(obs)

    def _log_emission_probabilities(self, obs):
        return self.emission_model.log_probabilities(obs)

    def _initialize_emissions(self, sequences):
        if self.means is None:
            self.init_emissions(sequences)
        
        
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