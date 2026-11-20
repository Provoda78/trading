import argparse

import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, balanced_accuracy_score, f1_score
from sklearn.model_selection import train_test_split

from XGBoost import XGBoost
from titanic_example import load_data


def make_features(data, age_median, fare_median):
    features = pd.DataFrame(index=data.index)
    features["Pclass"] = data["Pclass"]
    features["Sex_male"] = (data["Sex"] == "male").astype(float)
    features["Age"] = data["Age"].fillna(age_median)
    features["Fare"] = data["Fare"].fillna(fare_median)
    features["Family_size"] = data["SibSp"] + data["Parch"] + 1
    features["Embarked_C"] = (data["Embarked"] == "C").astype(float)
    features["Embarked_Q"] = (data["Embarked"] == "Q").astype(float)
    return features.to_numpy(dtype=float)


def main():
    parser = argparse.ArgumentParser(
        description="Compare custom XGBoost with sklearn gradient boosting on Titanic"
    )
    parser.add_argument("csv", nargs="?", help="Path to a local Titanic train.csv")
    args = parser.parse_args()

    data = load_data(args.csv)
    targets = data["Survived"].astype(int).to_numpy()
    train_data, test_data, y_train, y_test = train_test_split(
        data,
        targets,
        test_size=0.2,
        random_state=42,
        stratify=targets,
    )

    age_median = train_data["Age"].median()
    fare_median = train_data["Fare"].median()
    X_train = make_features(train_data, age_median, fare_median)
    X_test = make_features(test_data, age_median, fare_median)

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

    model_predictions = {
        "Custom XGBoost": custom_model.predict(X_test),
        "sklearn GradientBoosting": sklearn_model.predict(X_test),
    }

    print(f"Titanic samples: {len(data)}")
    print(f"Train samples: {len(X_train)}, test samples: {len(X_test)}")
    print(f"{'Model':<28} {'Accuracy':>10} {'Balanced':>10} {'F1':>10}")
    for model_name, predictions in model_predictions.items():
        print(
            f"{model_name:<28} "
            f"{accuracy_score(y_test, predictions):>9.1%} "
            f"{balanced_accuracy_score(y_test, predictions):>9.1%} "
            f"{f1_score(y_test, predictions):>9.1%}"
        )


if __name__ == "__main__":
    main()