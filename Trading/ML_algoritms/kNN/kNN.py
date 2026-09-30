from collections import defaultdict
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import List, Tuple


_tree_spec = spec_from_file_location(
    "_kd_tree", Path(__file__).with_name("KD-Tree.py")
)
if _tree_spec is None or _tree_spec.loader is None:
    raise ImportError("Unable to load KD-Tree.py")

_tree_module = module_from_spec(_tree_spec)
_tree_spec.loader.exec_module(_tree_module)
KD_Tree = _tree_module.KD_Tree


class kNN:
    def __init__(self, k=5):
        self.tree = None
        self.root = None
        self.k = k

    def fit(self, X: List[Tuple], y: Tuple) -> None:
        points = list(zip(X, y))
        self.tree = KD_Tree(self.k, points)
        self.root = self.tree.build(0, len(points) - 1)

    def vote(self, neighbors: tuple):
        votes = defaultdict(int)

        for _, _, label in neighbors:
            votes[label] += 1

        return max(votes, key=votes.get)

    def predict(self, Q: Tuple, K: int = 5):
        neighbors = self.tree.find_k_nearest(self.root, Q, K)
        return self.vote(neighbors)
    
if __name__ == '__main__':
    
    X = [(2, 3), (5, 4), (9, 6), (7, 4), (10, 3), (4, 2)]
    Y = ["A", "B", "A", "B", "B", "A"]
    
    Q = (6, 7)
    K = 5
    
    model = kNN(k=2)
    model.fit(X, Y)
    predict = model.predict(Q, K)
    print(f'Точка {Q} принадлежит классу {predict}')