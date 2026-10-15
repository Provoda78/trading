import numpy as np
import pytest

from Decision_tree import DecisionTree


@pytest.mark.parametrize("criterion", ["gini", "entropy"])
def test_predicts_separable_classes(criterion):
    X = np.array([[0.0], [1.0], [2.0], [3.0]])
    y = np.array(["low", "low", "high", "high"])
    model = DecisionTree(criterion=criterion).fit(X, y)

    assert np.array_equal(model.predict(X), y)


def test_max_depth_zero_predicts_majority_class():
    X = np.array([[0.0], [1.0], [2.0], [3.0]])
    y = np.array([0, 0, 0, 1])
    model = DecisionTree(max_depth=0).fit(X, y)

    assert np.array_equal(model.predict(X), np.array([0, 0, 0, 0]))