from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Ellipse
from sklearn.metrics import adjusted_rand_score
from sklearn.mixture import GaussianMixture


def make_dataset(random_state=42, samples_per_component=250):
    rng = np.random.default_rng(random_state)
    means = np.array([[-4.0, -1.5], [0.0, 3.0], [3.8, -1.0]])
    covariances = np.array(
        [
            [[1.8, 0.9], [0.9, 0.8]],
            [[0.9, -0.55], [-0.55, 1.4]],
            [[0.75, 0.1], [0.1, 1.6]],
        ]
    )

    components = [
        rng.multivariate_normal(mean, covariance, size=samples_per_component)
        for mean, covariance in zip(means, covariances)
    ]
    X = np.vstack(components)
    y_true = np.repeat(np.arange(len(means)), samples_per_component)
    permutation = rng.permutation(len(X))
    return X[permutation], y_true[permutation]


def add_covariance_ellipses(ax, model, colors):
    for component, (mean, covariance) in enumerate(
        zip(model.means_, model.covariances_)
    ):
        eigenvalues, eigenvectors = np.linalg.eigh(covariance)
        order = np.argsort(eigenvalues)[::-1]
        eigenvalues = eigenvalues[order]
        eigenvectors = eigenvectors[:, order]
        angle = np.degrees(np.arctan2(eigenvectors[1, 0], eigenvectors[0, 0]))

        for standard_deviations in (1, 2):
            ellipse = Ellipse(
                xy=mean,
                width=2 * standard_deviations * np.sqrt(eigenvalues[0]),
                height=2 * standard_deviations * np.sqrt(eigenvalues[1]),
                angle=angle,
                fill=False,
                edgecolor=colors[component],
                linestyle="-" if standard_deviations == 1 else "--",
                linewidth=1.5,
                alpha=0.9,
            )
            ax.add_patch(ellipse)

    ax.plot([], [], color="#333333", linestyle="-", label="1σ covariance")
    ax.plot([], [], color="#333333", linestyle="--", label="2σ covariance")


def main():
    X, y_true = make_dataset()
    model = GaussianMixture(
        n_components=3,
        covariance_type="full",
        n_init=5,
        random_state=42,
    ).fit(X)
    labels = model.predict(X)
    ari = adjusted_rand_score(y_true, labels)

    x_min, x_max = X[:, 0].min() - 2, X[:, 0].max() + 2
    y_min, y_max = X[:, 1].min() - 2, X[:, 1].max() + 2
    x_grid, y_grid = np.meshgrid(
        np.linspace(x_min, x_max, 240),
        np.linspace(y_min, y_max, 240),
    )
    grid = np.column_stack((x_grid.ravel(), y_grid.ravel()))
    density = np.exp(model.score_samples(grid)).reshape(x_grid.shape)

    colors = plt.get_cmap("tab10")(np.arange(model.n_components))
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.contour(
        x_grid,
        y_grid,
        density,
        levels=8,
        colors="#777777",
        linewidths=0.8,
        alpha=0.55,
    )
    for component in range(model.n_components):
        points = X[labels == component]
        ax.scatter(
            points[:, 0],
            points[:, 1],
            s=16,
            color=colors[component],
            alpha=0.65,
            label=f"component {component} (n={len(points)})",
        )

    add_covariance_ellipses(ax, model, colors)
    ax.scatter(
        model.means_[:, 0],
        model.means_[:, 1],
        marker="x",
        s=100,
        linewidths=2.5,
        color="#111111",
        label="component means",
    )
    ax.set(
        title=f"sklearn Gaussian Mixture Model (ARI={ari:.3f})",
        xlabel="Feature 1",
        ylabel="Feature 2",
        xlim=(x_min, x_max),
        ylim=(y_min, y_max),
    )
    ax.set_aspect("equal", adjustable="box")
    ax.legend(loc="upper right")
    fig.tight_layout()

    output_path = Path(__file__).with_name("gmm_sklearn_example.png")
    fig.savefig(output_path, dpi=160)
    print(f"Samples: {len(X)}")
    print(f"Converged: {model.converged_} after {model.n_iter_} iterations")
    print(f"Adjusted Rand index: {ari:.3f}")
    print(f"Plot saved to: {output_path}")
    plt.show()


if __name__ == "__main__":
    main()