import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LogisticRegression as SklearnLogisticRegression

from logreg import LogisticRegression, _plot_logistic_surface


def run_comparison(show: bool = True):
    rng = np.random.default_rng(42)
    negative_points = rng.normal(loc=(-2, -2), scale=1.0, size=(100, 2))
    positive_points = rng.normal(loc=(2, 2), scale=1.0, size=(100, 2))
    X = np.vstack((negative_points, positive_points))
    y = np.concatenate((np.zeros(100), np.ones(100)))

    custom_model = LogisticRegression(n_features=2)
    custom_model.fit(X, y, lr=0.1, epochs=2000)

    sklearn_model = SklearnLogisticRegression(random_state=42, max_iter=1000)
    sklearn_model.fit(X, y)

    figure, axes = plt.subplots(1, 2, figsize=(14, 6), constrained_layout=True)
    _plot_logistic_surface(
        axes[0], X, y, custom_model.weights, custom_model.bias,
        "Custom Logistic Regression",
    )
    _plot_logistic_surface(
        axes[1], X, y, sklearn_model.coef_[0], sklearn_model.intercept_[0],
        "scikit-learn LogisticRegression",
    )

    custom_predictions = (custom_model.predict_probability(X) >= 0.5).astype(int)
    sklearn_predictions = sklearn_model.predict(X)
    custom_accuracy = np.mean(custom_predictions == y)
    sklearn_accuracy = np.mean(sklearn_predictions == y)

    if show:
        plt.show()

    return figure, axes, custom_accuracy, sklearn_accuracy


if __name__ == "__main__":
    _, _, custom_accuracy, sklearn_accuracy = run_comparison()
    print(f"Custom LogisticRegression accuracy: {custom_accuracy:.1%}")
    print(f"scikit-learn LogisticRegression accuracy: {sklearn_accuracy:.1%}")