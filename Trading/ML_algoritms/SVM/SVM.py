import numpy as np
import matplotlib.pyplot as plt


class SVM:
    def __init__(self, C=1.0, tol=1e-3, max_passes=10, seed=None,
                 kernel_type="linear", gamma=None):
        self.C = C
        self.tol = tol
        self.max_passes = max_passes
        self.seed = seed
        self.kernel_type = kernel_type
        self.gamma = gamma
        self.rng = np.random.default_rng(self.seed)

        self.w = None
        self.b = 0.0
        self.alphas: np.ndarray = None
        self.X: np.ndarray = None
        self.y: np.ndarray = None
        
    def kernel(self, x1: np.ndarray, x2: np.ndarray):
        x1 = np.atleast_2d(x1)
        x2 = np.atleast_2d(x2)

        if self.kernel_type == "linear":
            values = x1 @ x2.T
        elif self.kernel_type == "rbf":
            squared_distances = np.sum((x1[:, np.newaxis] - x2) ** 2, axis=2)
            values = np.exp(-self.gamma * squared_distances)
        else:
            raise ValueError("kernel_type must be 'linear' or 'rbf'")

        return values[0, 0] if values.size == 1 else values
        
    def decision_single(self, i):
        return np.sum(
            self.alphas * self.y @ self.kernel(self.X, self.X[i])
        ) + self.b
    
    def select_second_alpha(self, i):
        n = len(self.X)

        j = i
        while j == i:
            j = self.rng.integers(n)

        return j


    def error(self, i):
        return self.decision_single(i) - self.y[i]
    
    def violates_kkt(self, i):
        alpha = self.alphas[i]
        y = self.y[i]
        E = self.error(i)

        return (
            (y * E < -self.tol and alpha < self.C)
            or
            (y * E > self.tol and alpha > 0)
        )
    
    def update_pair(self, i: int, j: int) -> bool:
        erri = self.error(i)
        errj = self.error(j)

        alpha_i_old = self.alphas[i]
        alpha_j_old = self.alphas[j]
        
        if self.y[i] != self.y[j]:
            L = max(0, alpha_j_old - alpha_i_old)
            H = min(self.C, self.C + alpha_j_old - alpha_i_old)
        
        else:
            L = max(0.0, alpha_i_old + alpha_j_old - self.C)
            H = min(self.C, alpha_i_old + alpha_j_old)
        
        Kii = self.kernel(self.X[i], self.X[i])
        Kjj = self.kernel(self.X[j], self.X[j])
        Kij = self.kernel(self.X[i], self.X[j])
        
        eta = 2.0 * Kij - Kii - Kjj
        
        
        if eta >= 0:
            return False
        
        alpha_j_new = alpha_j_old - self.y[j] * (erri - errj) / eta
        
        alpha_j_new = np.clip(alpha_j_new, L, H)
        if abs(alpha_j_new - alpha_j_old) < self.tol:
            return False

        alpha_i_new = (alpha_i_old + self.y[i] * 
                       self.y[j] * 
                       (alpha_j_old - alpha_j_new))
        
        b1 = (self.b - erri - self.y[i] * (alpha_i_new - alpha_i_old) * Kii
                - self.y[j]
                * (alpha_j_new - alpha_j_old)
                * Kij
            )

        b2 = (self.b - errj - self.y[i] * (alpha_i_new - alpha_i_old) * Kij
                - self.y[j]
                * (alpha_j_new - alpha_j_old)
                * Kjj
            )

        if 0 < alpha_i_new < self.C:
            b_new = b1
        elif 0 < alpha_j_new < self.C:
            b_new = b2
        else:
            b_new = 0.5 * (b1 + b2)


        self.alphas[i] = alpha_i_new
        self.alphas[j] = alpha_j_new
        self.b = b_new
        
        return True
    
    def smo(self):
        passes = 0
        n = len(self.X)

        while passes < self.max_passes:
            changed = 0

            for i in range(n):

                if not self.violates_kkt(i):
                    continue

                j = self.select_second_alpha(i)

                if self.update_pair(i, j):
                    changed += 1

            if changed == 0:
                passes += 1
            else:
                passes = 0

        
    def weights(self):
        self.w = np.sum((self.alphas * self.y)[:, np.newaxis] * self.X, axis=0)
        
    def fit(self, X, y):
        X = np.asarray(X, dtype=float)
        y = np.asarray(y, dtype=float)

    
        if y.ndim != 1:
            raise ValueError("y must be a 1D array")

        if len(X) != len(y):
            raise ValueError("X and y must have the same number of samples")

        labels = set(np.unique(y))
        if labels - {-1, 1}:
            raise ValueError("Labels must be encoded as -1 and +1")

        self.X = X
        self.y = y

        n_samples, n_features = X.shape

        if self.gamma is None:
            self.gamma = 1.0 / n_features

        self.alphas = np.zeros(n_samples)
        self.b = 0.0
        self.w = np.zeros(n_features)
        self.rng = np.random.default_rng(self.seed)

        self.smo()
        self.weights()

        return self
        
    def predict(self, x):
        x = np.asarray(x, dtype=float)
        if x.ndim == 1:
            x = x[np.newaxis, :]
        f = (self.alphas * self.y) @ self.kernel(self.X, x) + self.b
        
        return np.where(f >= 0, 1, -1)

    def plot(self, ax=None, show=True):
        if self.X is None or self.w is None:
            raise ValueError("Fit the model before plotting")
        if self.X.shape[1] != 2:
            raise ValueError("Plotting is only supported for 2D input data")

        if ax is None:
            _, ax = plt.subplots(figsize=(16, 9))

        x_min, x_max = self.X[:, 0].min(), self.X[:, 0].max()
        y_min, y_max = self.X[:, 1].min(), self.X[:, 1].max()
        x_padding = max((x_max - x_min) * 0.1, 1.0)
        y_padding = max((y_max - y_min) * 0.1, 1.0)
        x_values = np.linspace(x_min - x_padding, x_max + x_padding, 200)
        y_values = np.linspace(y_min - y_padding, y_max + y_padding, 200)
        xx, yy = np.meshgrid(x_values, y_values)
        grid = np.column_stack((xx.ravel(), yy.ravel()))
        decision = (
            (self.alphas * self.y) @ self.kernel(self.X, grid) + self.b
        ).reshape(xx.shape)

        contour = ax.contour(
            xx,
            yy,
            decision,
            levels=[-1, 0, 1],
            colors=["#2878B5", "#222222", "#D9534F"],
            linestyles=["--", "-", "--"],
        )
        ax.clabel(
            contour,
            fmt={-1: "margin -1", 0: "decision boundary", 1: "margin +1"},
        )

        for label, color in ((-1, "#2878B5"), (1, "#D9534F")):
            points = self.X[self.y == label]
            ax.scatter(points[:, 0], points[:, 1], color=color, label=f"class {label}")

        support_vectors = self.alphas > self.tol
        ax.scatter(
            self.X[support_vectors, 0],
            self.X[support_vectors, 1],
            s=150,
            facecolors="none",
            edgecolors="#222222",
            linewidths=1.5,
            label="support vectors",
        )
        ax.set_xlabel("Feature 1")
        ax.set_ylabel("Feature 2")
        ax.set_title(f"SVM ({self.kernel_type} kernel) with margins")
        ax.legend()
        ax.set_aspect("equal", adjustable="box")

        if show:
            plt.show()

        return ax