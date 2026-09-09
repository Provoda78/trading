import numpy as np
import matplotlib.pyplot as plt
from typing import Tuple, Sequence

class LogisticRegression:

    def __init__(self, n_features):
        self.weights = np.zeros(n_features)
        self.bias = 0.0
        self.loss_history = []

    
    def sigmoid(self, z: np.ndarray) -> np.ndarray:
        return 1 / (1 + np.exp(-z))
    
    def predict_probability(self, X: np.ndarray) -> np.ndarray:
        z = X @ self.weights + self.bias
        return self.sigmoid(z)
    
    def loss(self, targets: np.ndarray, p: np.ndarray) -> float:
        
        eps = 1e-15
        p = np.clip(p, eps, 1 - eps)
        
        return np.mean((-targets * np.log(p) - (1 - targets) * np.log(1 - p)))
    
    def history(self, epochs: int = 1000) -> None:
        
        _, axes = plt.subplots(figsize=(16, 9))
        axes: plt.Axes
        abscissa = np.linspace(0, epochs, len(self.loss_history))
        axes.plot(abscissa, np.array(self.loss_history))
        
        axes.set_title(label='Loss Function', fontsize=24)
        axes.set_ylabel('Loss', fontsize=14)
        axes.set_xlabel('Epochs', fontsize=14)
        
        axes.grid()
        plt.show()
    
    def grad(self, targets: np.ndarray, X: np.ndarray, p: np.ndarray) -> Tuple[np.ndarray,
                                                                float]:
        m = X.shape[0]
        dw = X.T @ (p - targets) / m
        db = np.mean(p - targets)
        return dw, db
        
    def fit(self, X: np.ndarray, y: np.ndarray, lr: float = 0.01, epochs: int = 1000) -> None:
            
        for epoch in range(epochs):

            p = self.predict_probability(X)

            dw, db = self.grad(y, X, p)
            
            loss = self.loss(y, p)
            if epoch % 100 == 0:
                print(loss)
                self.loss_history.append(loss)
                            
            if np.linalg.norm(dw) < 1e-6:
                break
            

            self.weights -= lr * dw
            self.bias -= lr * db
        
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
    predict = model.predict_probability(X)
        
    print(f'Вероятность: {predict}', sep='\n')
    print(f'Веса: {weights}')
