from hmmlearn import hmm
import numpy as np
from HMM import HMM

m = HMM(2, 3, random_state=0)
start = (m.pi.copy(), m.A.copy(), m.B.copy())

seqs = [np.array([0,1,2,2,1,0,0,1]), np.array([2,2,1,0,0,0,1,2,2])]
m2 = HMM(2, 3, random_state=0)
hist = m2.fit(seqs, n_iter=50, tol=0)
assert np.all(np.diff(hist) > -1e-9), "Log-likelihood decreased during training"
assert np.isclose(m2.pi.sum(), 1)
assert np.allclose(m2.A.sum(axis=1), 1)
assert np.allclose(m2.B.sum(axis=1), 1)
print("Custom HMM training and probability normalization: OK")

h = hmm.CategoricalHMM(n_components=2, n_features=3, n_iter=20, tol=-np.inf,
                       init_params="", random_state=0)
h.startprob_, h.transmat_, h.emissionprob_ = start

X = np.concatenate(seqs).reshape(-1, 1)
lengths = [len(s) for s in seqs]

m.fit(seqs, n_iter=20, tol=0)
h.fit(X, lengths)

checks = {
    "start probabilities": np.allclose(m.pi, h.startprob_),
    "transition probabilities": np.allclose(m.A, h.transmat_),
    "emission probabilities": np.allclose(m.B, h.emissionprob_),
    "log-likelihood": np.isclose(
        sum(m.forward(s)[2] for s in seqs), h.score(X, lengths)
    ),
}
for name, passed in checks.items():
    print(f"{name}: {'OK' if passed else 'FAILED'}")

assert all(checks.values()), "Custom HMM and hmmlearn results differ"