import numpy as np

class Node:
    def __init__(self, feature_index=None, threshold=None, value=None):
        self.value = value
        self.feature_index = feature_index
        self.threshold = threshold
        self.right = None 
        self.left = None

class DecisionTree:
    def __init__(self, criterion="gini", max_depth=None, tol = 1e-7, min_samples_split=2):
        self.tol = tol
        self.criterion = criterion
        self.max_depth = max_depth
        self.min_samples_split = min_samples_split
        self.tree = None
        
    def impurity(self, y: np.ndarray):
        _, counts = np.unique(y, return_counts=True)
        p = counts / y.size

        if self.criterion == "entropy":
            return -np.sum(p * np.log(p))
        elif self.criterion == "gini":
            return 1.0 - np.sum(p ** 2)
        else:
            raise ValueError("criterion must be 'entropy' or 'gini'")
        
        
    def split_score(self, y_left: np.ndarray, y_right: np.ndarray, n_parent):
        return (self.impurity(y_left) * (y_left.size / n_parent) + 
                self.impurity(y_right) * (y_right.size / n_parent))
        
    def best_split_one_feature(self, X_column: np.ndarray, y: np.ndarray):
        order = np.argsort(X_column)
        x_sorted = X_column[order]
        y_sorted = y[order]

        best_threshold = None
        best_score = np.inf

        for i in range(1, len(x_sorted)):
            if x_sorted[i] == x_sorted[i - 1]:
                continue

            threshold = (x_sorted[i] + x_sorted[i - 1]) / 2.0

            y_left = y_sorted[:i]
            y_right = y_sorted[i:]

            score = self.split_score(y_left, y_right, y.size)

            if score < best_score:
                best_score = score
                best_threshold = threshold

        return best_threshold, best_score
    
    
    def best_split(self, X: np.ndarray, y: np.ndarray):
        best_feature = None
        best_threshold = None
        best_score = np.inf

        n_features = X.shape[1]

        for feature_idx in range(n_features):
            threshold, score = self.best_split_one_feature(X[:, feature_idx], y)

            if threshold is not None and score < best_score:
                best_score = score
                best_feature = feature_idx
                best_threshold = threshold

        return best_feature, best_threshold, best_score
    
    def majority_class(self, y):
        classes, counts = np.unique(y, return_counts=True)
        return classes[np.argmax(counts)]    
        
    def build_tree(self, X, y, depth=0):
        
        if len(np.unique(y)) == 1:
            return Node(value=self.majority_class(y))
        
        if ( self.max_depth is not None and depth >= self.max_depth or
            len(y) < self.min_samples_split):
            
            return Node(value=self.majority_class(y))
        
        feature, threshold, score = self.best_split(X, y)
        
        if feature is None:
            return Node(value=self.majority_class(y))
        
        parent_score = self.impurity(y)
        gain = parent_score - score

        if gain < self.tol:
            return Node(value=self.majority_class(y))

        
        left = X[:, feature] <= threshold
        right = ~left
        
        X_left, y_left = X[left], y[left]
        X_right, y_right = X[right], y[right]
        
        if len(y_left) == 0 or len(y_right) == 0:
            return Node(value=self.majority_class(y))
        
        root = Node(feature_index=feature, threshold=threshold)
        root.left = self.build_tree(X_left, y_left, depth + 1)
        root.right = self.build_tree(X_right, y_right, depth + 1)
        
        return root
    
    def fit(self, X, y):
        X = np.asarray(X, dtype=float)
        y = np.asarray(y)
        self.tree = self.build_tree(X, y)
        return self
    
    def predict_one(self, x):
        root = self.tree

        while root.value is None:
            if x[root.feature_index] > root.threshold:
                root = root.right
            else:
                root = root.left

        return root.value
        
    def predict(self, X):
        X = np.asarray(X, dtype=float)
        return np.array([self.predict_one(x) for x in X])