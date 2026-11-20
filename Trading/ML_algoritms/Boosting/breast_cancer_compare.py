from sklearn.datasets import load_breast_cancer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, balanced_accuracy_score, f1_score
from sklearn.model_selection import train_test_split

from XGBoost import XGBoost


def main():
    dataset = load_breast_cancer()
    X = dataset.data
    y = dataset.target
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    custom_model = XGBoost(
        n_estimators=100,
        learning_rate=0.05,
        max_depth=3,
        min_samples_split=10,
        reg_lambda=1.0,
        gamma=0.0,
        objective="binary:logistic",
        random_state=42,
    ).fit(X_train, y_train)
    sklearn_model = GradientBoostingClassifier(
        n_estimators=100,
        learning_rate=0.05,
        max_depth=3,
        min_samples_split=10,
        random_state=42,
    ).fit(X_train, y_train)

    predictions = {
        "Custom XGBoost": custom_model.predict(X_test),
        "sklearn GradientBoosting": sklearn_model.predict(X_test),
    }

    print("Dataset: Breast Cancer Wisconsin Diagnostic")
    print(f"Samples: {len(y)}; features: {X.shape[1]}")
    print(f"Train samples: {len(X_train)}, test samples: {len(X_test)}")
    print(f"{'Model':<28} {'Accuracy':>10} {'Balanced':>10} {'Macro F1':>10}")
    for model_name, y_pred in predictions.items():
        print(
            f"{model_name:<28} "
            f"{accuracy_score(y_test, y_pred):>9.1%} "
            f"{balanced_accuracy_score(y_test, y_pred):>9.1%} "
            f"{f1_score(y_test, y_pred, average='macro'):>9.1%}"
        )


if __name__ == "__main__":
    main()