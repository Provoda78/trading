import numpy as np
import matplotlib.pyplot as plt
from sklearn.datasets import make_moons
from sklearn.model_selection import train_test_split

from SVM import SVM


def run_moons_demo(show_plot=False):
    X, y = make_moons(n_samples=240, noise=0.18, random_state=42)
    y = np.where(y == 0, -1, 1)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y
    )

    model = SVM(C=0.35, max_passes=20, seed=42, kernel_type="rbf", gamma=2)
    model.fit(X_train, y_train)
    ax = model.plot(show=False)
    for label, color in ((-1, "#2878B5"), (1, "#D9534F")):
        test_points = X_test[y_test == label]
        ax.scatter(
            test_points[:, 0],
            test_points[:, 1],
            marker="x",
            color=color,
            label=f"test class {label}",
        )
    ax.legend()
    if show_plot:
        plt.show()
    else:
        plt.close(ax.figure)

    return np.mean(model.predict(X_test) == y_test)


def test_rbf_svm_classifies_moons():
    accuracy = run_moons_demo()
    assert accuracy >= 0.85


def test_decision_single_matches_kernel_expansion():
    model = SVM(kernel_type="rbf", gamma=2)
    model.X = np.array([[0.0, 0.0], [1.0, 0.0], [0.0, 1.0]])
    model.y = np.array([-1.0, 1.0, 1.0])
    model.alphas = np.array([0.2, 0.4, 0.6])
    model.b = 0.3
    index = 1

    expected = np.sum(
        model.alphas * model.y * model.kernel(model.X, model.X[index]).ravel()
    ) + model.b
    assert np.isclose(model.decision_single(index), expected)


if __name__ == "__main__":
    accuracy = run_moons_demo(show_plot=True)
    print(f"Test accuracy: {accuracy:.0%}")