import numpy as np
import matplotlib.pyplot as plt
from sklearn.datasets import make_moons
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC

from SVM import SVM


def plot_classifier(ax, X_train, y_train, X_test, y_test, support_vectors,
                    decision_function, title, x_values, y_values):
    xx, yy = np.meshgrid(x_values, y_values)
    grid = np.column_stack((xx.ravel(), yy.ravel()))
    decision = decision_function(grid).reshape(xx.shape)
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
        fmt={-1: "margin -1", 0: "boundary", 1: "margin +1"},
    )

    for label, color in ((-1, "#2878B5"), (1, "#D9534F")):
        train_points = X_train[y_train == label]
        test_points = X_test[y_test == label]
        ax.scatter(
            train_points[:, 0],
            train_points[:, 1],
            color=color,
            label=f"train class {label}",
        )
        ax.scatter(
            test_points[:, 0],
            test_points[:, 1],
            color=color,
            marker="x",
            label=f"test class {label}",
        )

    ax.scatter(
        support_vectors[:, 0],
        support_vectors[:, 1],
        s=130,
        facecolors="none",
        edgecolors="#222222",
        linewidths=1.4,
        label="support vectors",
    )
    ax.set_title(title)
    ax.set_xlabel("Feature 1")
    ax.set_ylabel("Feature 2")
    ax.set_aspect("equal", adjustable="box")
    ax.legend(fontsize=8)


def main():
    X, y = make_moons(n_samples=240, noise=0.18, random_state=42)
    y = np.where(y == 0, -1, 1)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y
    )

    C = 1.0
    gamma = 2
    custom_model = SVM(
        C=C,
        max_passes=40,
        seed=42,
        kernel_type="rbf",
        gamma=gamma,
    ).fit(X_train, y_train)
    sklearn_model = SVC(C=C, kernel="rbf", gamma=gamma).fit(X_train, y_train)

    custom_accuracy = np.mean(custom_model.predict(X_test) == y_test)
    sklearn_accuracy = np.mean(sklearn_model.predict(X_test) == y_test)

    padding = 0.5
    x_values = np.linspace(X[:, 0].min() - padding, X[:, 0].max() + padding, 300)
    y_values = np.linspace(X[:, 1].min() - padding, X[:, 1].max() + padding, 300)

    fig, axes = plt.subplots(1, 2, figsize=(16, 7), sharex=True, sharey=True)
    plot_classifier(
        axes[0],
        X_train,
        y_train,
        X_test,
        y_test,
        X_train[custom_model.alphas > custom_model.tol],
        lambda points: (
            (custom_model.alphas * custom_model.y)
            @ custom_model.kernel(custom_model.X, points)
            + custom_model.b
        ),
        f"Custom SVM | test accuracy: {custom_accuracy:.1%}",
        x_values,
        y_values,
    )
    plot_classifier(
        axes[1],
        X_train,
        y_train,
        X_test,
        y_test,
        sklearn_model.support_vectors_,
        sklearn_model.decision_function,
        f"sklearn SVC | test accuracy: {sklearn_accuracy:.1%}",
        x_values,
        y_values,
    )

    fig.suptitle("RBF SVM comparison on make_moons")
    fig.tight_layout()
    print(f"Custom SVM test accuracy: {custom_accuracy:.1%}")
    print(f"sklearn SVC test accuracy: {sklearn_accuracy:.1%}")
    plt.show()


if __name__ == "__main__":
    main()