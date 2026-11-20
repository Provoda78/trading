import numpy as np


class Node:
    def __init__(
        self,
        feature_index=None,
        threshold=None,
        left=None,
        right=None,
        value=None
    ):
        self.feature_index = feature_index
        self.threshold = threshold
        self.left = left
        self.right = right
        self.value = value
        
class XGBoost:
    def __init__(self, n_estimators=100, learning_rate=0.1, max_depth=3, min_samples_split=2,
            reg_lambda=1.0,
            gamma=0.0,
            min_child_weight=1e-6,
            subsample=1.0,
            colsample_bytree=1.0,
            objective="binary:logistic",
            random_state=None
        ):
        self.targets: np.ndarray = None
        self.n_estimators = n_estimators
        self.learning_rate = learning_rate
        self.max_depth = max_depth
        self.min_samples_split = min_samples_split

        self.reg_lambda = reg_lambda
        self.gamma = gamma
        self.min_child_weight = min_child_weight

        self.subsample = subsample
        self.colsample_bytree = colsample_bytree

        self.objective = objective
        self.random_state = random_state
        self.rng = np.random.default_rng(random_state)

        self.trees = []
        self.base_score = None
        self.n_features_ = None
        
    def sigmoid(self, z: np.ndarray) -> np.ndarray:
            z = np.asarray(z, dtype=float)
            return np.exp(-np.logaddexp(0, -z))
        
    def grad_hess_regression(self, y_pred: np.ndarray, targets: np.ndarray):
        y_pred = np.asarray(y_pred, dtype=float)
        targets = np.asarray(targets, dtype=float)

        g = y_pred - targets
        h = np.ones_like(y_pred, dtype=float)

        return g, h

    def grad_hess_binary(self, y_pred: np.ndarray, targets: np.ndarray) -> float:
        
        y_pred = np.asarray(y_pred, dtype=float)
        targets = np.asarray(targets, dtype=float)

        p = self.sigmoid(y_pred)
        g = (p - targets)
        h = (p * (1.0 - p))
        return g, h
    
    def leaf_weight(self, G, H):
        return -G / (H + self.reg_lambda)
    
    def Gain(self, G_left, H_left, G_right, H_right):
        return 0.5 * (
            (G_left ** 2) / (H_left + self.reg_lambda)
            + (G_right ** 2) / (H_right + self.reg_lambda)
            - ((G_left + G_right)** 2) / (H_left + H_right+ self.reg_lambda)
        ) - self.gamma
    
    def best_split_one_feature(self, X_column: np.ndarray, g, h):
            order = np.argsort(X_column)
            x_sorted = X_column[order]
            g_sorted = g[order]
            h_sorted = h[order]
    
            best_threshold = None
            best_gain = -np.inf
    
            for i in range(1, len(x_sorted)):
                if x_sorted[i] == x_sorted[i - 1]:
                    continue
    
                threshold = (x_sorted[i] + x_sorted[i - 1]) / 2.0
    
                g_left, h_left = g_sorted[:i], h_sorted[:i]
                g_right, h_right = g_sorted[i:], h_sorted[i:]                    
                
                G_left, H_left = np.sum(g_left), np.sum(h_left)
                G_right, H_right = np.sum(g_right), np.sum(h_right)

                gain = self.Gain(G_left, H_left, G_right, H_right)
    
                if gain > best_gain:
                    best_gain = gain
                    best_threshold = threshold
    
            return best_threshold, best_gain
    
    def best_split(self, X: np.ndarray, g: np.ndarray, h: np.ndarray):
            best_feature = None
            best_threshold = None
            best_gain = -np.inf
    
            n_features = X.shape[1]
    
            for feature_idx in range(n_features):
                threshold, gain = self.best_split_one_feature(X[:, feature_idx], g, h)
    
                if threshold is not None and gain > best_gain:
                    best_gain = gain
                    best_feature = feature_idx
                    best_threshold = threshold
    
            return best_feature, best_threshold, best_gain
    
    def build_tree(self, X, y_pred, targets, depth=0):
        
        if len(targets) == 0:
            return Node(value=0.0)
        
        if self.objective == 'binary:logistic':
            g, h = self.grad_hess_binary(y_pred, targets)
        elif self.objective == 'regression':
            g, h = self.grad_hess_regression(y_pred, targets)
        else:
            raise ValueError("Unsupported objective")
        
        G, H = np.sum(g), np.sum(h)
        leaf_value = self.leaf_weight(G, H)
        
        if self.max_depth is not None and depth >= self.max_depth:
            return Node(value=leaf_value)

        if len(targets) < self.min_samples_split:
            return Node(value=leaf_value)

        if H < self.min_child_weight:
            return Node(value=leaf_value)
        
        feature, threshold, gain = self.best_split(X, g, h)
        
        if feature is None or gain <= 0:
            return Node(value=leaf_value)
        
        left = X[:, feature] <= threshold
        right = ~left
                
        X_left, y_pred_left, targets_left = X[left], y_pred[left], targets[left]
        X_right, y_pred_right, targets_right = X[right], y_pred[right], targets[right]
        
        if left.sum() == 0 or right.sum() == 0:
            return Node(value=leaf_value)
        
        if len(y_pred_left) == 0 or len(y_pred_right) == 0:
            return Node(value=leaf_value)
        
        root = Node(feature_index=feature, threshold=threshold)
        root.left = self.build_tree(X_left, y_pred_left, targets_left, depth + 1)
        root.right = self.build_tree(X_right, y_pred_right, targets_right, depth + 1)
                
        return root
    
    def fit_one_tree(self, X, y_pred, targets):
        X = np.asarray(X, dtype=float)
        y_pred = np.asarray(y_pred)
        targets = np.asarray(targets)
        
        tree = self.build_tree(X, y_pred, targets, depth=0)
        return tree

    def predict_one_x_tree(self, x, root: Node):
    
        while root.value is None:
            if x[root.feature_index] > root.threshold:
                root = root.right
            else:
                root = root.left
    
        return root.value
            
    def predict_tree(self, tree, X):
        X = np.asarray(X, dtype=float)
        return np.array([self.predict_one_x_tree(x, tree) for x in X])
    
    def fit(self, X, y):
        X = np.asarray(X, dtype=float)
        y = np.asarray(y, dtype=float)

        self.trees = []
        self.n_features_ = X.shape[1]
        
        # стартовое предсказание
        if self.objective == "binary:logistic":
            p = np.clip(y.mean(), 1e-15, 1 - 1e-15)
            self.base_score = np.log(p / (1 - p))
            
        elif self.objective == "regression":
            self.base_score = y.mean()
            
        else:
            raise ValueError("Unsupported objective")
        
        y_pred = np.full_like(y, fill_value=self.base_score, dtype=float)
        
        for _ in range(self.n_estimators):
            tree = self.fit_one_tree(X, y_pred, y)
            self.trees.append(tree)
            y_pred += self.learning_rate * self.predict_tree(tree, X)
            
        return self
    
    def raw_predict(self, X):
        X = np.asarray(X, dtype=float)
        y_pred = np.full(X.shape[0], self.base_score, dtype=float)

        for tree in self.trees:
            y_pred += self.learning_rate * self.predict_tree(tree, X)

        return y_pred


    def predict_proba(self, X):
        if self.objective != "binary:logistic":
            raise ValueError("predict_proba is available only for binary:logistic objective")

        raw_pred = self.raw_predict(X)
        proba_pos = self.sigmoid(raw_pred)
        proba_neg = 1.0 - proba_pos

        return np.column_stack([proba_neg, proba_pos])


    def predict(self, X):
        raw_pred = self.raw_predict(X)

        if self.objective == "binary:logistic":
            proba_pos = self.sigmoid(raw_pred)
            return (proba_pos >= 0.5).astype(int)

        elif self.objective == "regression":
            return raw_pred

        else:
            raise ValueError("Unsupported objective")