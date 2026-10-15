import numpy as np
import matplotlib.pyplot as plt
from typing import Tuple


def _plot_logistic_surface(ax, X, y, weights, bias, title):
    x_min, x_max = X[:, 0].min(), X[:, 0].max()
    y_min, y_max = X[:, 1].min(), X[:, 1].max()
    x_padding = max((x_max - x_min) * 0.1, 1.0)
    y_padding = max((y_max - y_min) * 0.1, 1.0)
    x_values = np.linspace(x_min - x_padding, x_max + x_padding, 200)
    y_values = np.linspace(y_min - y_padding, y_max + y_padding, 200)
    xx, yy = np.meshgrid(x_values, y_values)
    probabilities = 1 / (
        1 + np.exp(-np.clip(weights[0] * xx + weights[1] * yy + bias, -709, 709))
    )

    contour = ax.contour(
        xx,
        yy,
        probabilities,
        levels=[0.25, 0.5, 0.75],
        colors=["#2878B5", "#222222", "#D9534F"],
        linestyles=["--", "-", "--"],
    )
    ax.clabel(contour, fmt={0.25: "P=0.25", 0.5: "P=0.50", 0.75: "P=0.75"})

    for label, color in ((0, "#2878B5"), (1, "#D9534F")):
        points = X[y == label]
        ax.scatter(points[:, 0], points[:, 1], color=color, label=f"class {label}")

    ax.set_xlabel("Feature 1")
    ax.set_ylabel("Feature 2")
    ax.set_title(title)
    ax.legend()
    ax.set_aspect("equal", adjustable="box")
    return ax

class LogisticRegression:

    def __init__(self, n_features):
        self.X: np.ndarray = None
        self.y: np.ndarray = None
        self.weights = np.zeros(n_features)
        self.bias = 0.0
        self.loss_history = []

    
    def sigmoid(self, z: np.ndarray) -> np.ndarray:
        z = np.asarray(z, dtype=float)
        return np.exp(-np.logaddexp(0, -z))
    
    def predict_probability(self, X: np.ndarray = None) -> np.ndarray:
        if self.X is None:
            raise ValueError("Fit the model before predicting")

        if X is None:
            X = self.X
        else:
            X = np.asarray(X, dtype=float)
            if X.ndim != 2 or X.shape[1] != len(self.weights):
                raise ValueError("X must be a 2D array with the fitted feature count")

        z = X @ self.weights + self.bias
        return self.sigmoid(z)
    
    def loss(self, targets: np.ndarray, p: np.ndarray) -> float:
        
        eps = 1e-15
        p = np.clip(p, eps, 1 - eps)
        
        return np.mean((-targets * np.log(p) - (1 - targets) * np.log(1 - p)))
    
    def history(self, epochs: int = 1000) -> None:
        if not self.loss_history:
            raise ValueError("Fit the model before plotting its loss history")

        _, axes = plt.subplots(figsize=(16, 9))
        axes: plt.Axes
        abscissa = np.arange(len(self.loss_history))
        axes.plot(abscissa, np.array(self.loss_history))
        
        axes.set_title(label='Loss Function', fontsize=24)
        axes.set_ylabel('Loss', fontsize=14)
        axes.set_xlabel('Epochs', fontsize=14)
        
        axes.grid()
        plt.show()
    
    def grad(self, p: np.ndarray) -> Tuple[np.ndarray, float]:
        m = self.X.shape[0]
        dw = self.X.T @ (p - self.y) / m
        db = np.mean(p - self.y)
        return dw, db
        
    def fit(
        self, X: np.ndarray, y: np.ndarray, lr: float = 0.01, epochs: int = 1000
    ) -> "LogisticRegression":
        
        X = np.asarray(X, dtype=float)
        y = np.asarray(y, dtype=float)

        if X.ndim != 2:
            raise ValueError("X must be a 2D array")
        if y.ndim != 1:
            raise ValueError("y must be a 1D array")
        if X.shape[0] == 0:
            raise ValueError("X must contain at least one sample")
        if X.shape[0] != y.shape[0]:
            raise ValueError("X and y must have the same number of samples")
        if X.shape[1] != len(self.weights):
            raise ValueError("X feature count must match n_features")
        if not np.all(np.isfinite(X)) or not np.all(np.isfinite(y)):
            raise ValueError("X and y must contain only finite values")
        if set(np.unique(y)) != {0.0, 1.0}:
            raise ValueError("y must contain both binary labels 0 and 1")
        if lr <= 0 or epochs <= 0:
            raise ValueError("lr and epochs must be positive")

        self.y = y
        self.X = X
        self.weights = np.zeros(X.shape[1])
        self.bias = 0.0
        self.loss_history = []

        for epoch in range(epochs):
            p = self.predict_probability()
            dw, db = self.grad(p)
            self.loss_history.append(self.loss(self.y, p))

            if np.linalg.norm(dw) < 1e-6 and abs(db) < 1e-6:
                break

            self.weights -= lr * dw
            self.bias -= lr * db

        return self

    def plot_logreg(self, ax=None, show: bool = True):
        if self.X is None or self.y is None:
            raise ValueError("Fit the model before plotting")
        if self.X.shape[1] != 2:
            raise ValueError("Plotting is only supported for 2D input data")

        if ax is None:
            _, ax = plt.subplots(figsize=(9, 7))

        _plot_logistic_surface(
            ax, self.X, self.y, self.weights, self.bias, "Custom Logistic Regression"
        )
        if show:
            plt.show()
        return ax

    def polt_logreg(self, ax=None, show: bool = True):
        return self.plot_logreg(ax=ax, show=show)

        
if __name__ == '__main__':
    X = np.array([
    [1, 0],
    [0, 1],
    [1, 1],
    [0, 0]
    ])

    y = np.array([1, 0, 0, 0])
    
    model = LogisticRegression(2)
    model.fit(X, y)
    weights = model.weights
    predict = model.predict_probability()
    model.polt_logreg()
        
    print(f'Вероятность: {predict}', sep='\n')
    print(f'Веса: {weights}')
