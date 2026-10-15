import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from compare_logreg import run_comparison


def test_custom_and_sklearn_plots_render():
    figure, axes, custom_accuracy, sklearn_accuracy = run_comparison(show=False)

    try:
        assert len(axes) == 2
        assert custom_accuracy > 0.95
        assert sklearn_accuracy > 0.95

        expected_levels = {"P=0.25", "P=0.50", "P=0.75"}
        for axis in axes:
            assert expected_levels <= {text.get_text() for text in axis.texts}
            assert len(axis.collections) >= 3
    finally:
        plt.close(figure)