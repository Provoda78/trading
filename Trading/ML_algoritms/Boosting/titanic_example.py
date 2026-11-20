import argparse

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

from XGBoost import XGBoost


DEFAULT_DATA_URL = (
    "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
)


def load_data(csv_path=None):
    source = csv_path if csv_path else DEFAULT_DATA_URL
    data = pd.read_csv(source)
    required_columns = {"Survived", "Pclass", "Sex", "Age", "Fare", "Embarked", "SibSp", "Parch"}
    missing_columns = required_columns.difference(data.columns)
    if missing_columns:
        raise ValueError(f"Missing Titanic columns: {sorted(missing_columns)}")
    return data


def preprocess(data):
    features = pd.DataFrame()
    features["Pclass"] = data["Pclass"]
    features["Sex_male"] = (data["Sex"] == "male").astype(float)
    features["Age"] = data["Age"].fillna(data["Age"].median())
    features["Fare"] = data["Fare"].fillna(data["Fare"].median())
    features["Family_size"] = data["SibSp"] + data["Parch"] + 1
    features["Embarked_C"] = (data["Embarked"] == "C").astype(float)
    features["Embarked_Q"] = (data["Embarked"] == "Q").astype(float)

    features = features.astype(float)
    targets = data["Survived"].astype(int).to_numpy()
    return features.to_numpy(dtype=float), targets


def main():
    parser = argparse.ArgumentParser(description="Run the custom XGBoost on Titanic data")
    parser.add_argument("csv", nargs="?", help="Path to a local Titanic train.csv")
    args = parser.parse_args()

    data = load_data(args.csv)
    X, y = preprocess(data)
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    model = XGBoost(
        n_estimators=100,
        learning_rate=0.05,
        max_depth=3,
        min_samples_split=10,
        reg_lambda=1.0,
        gamma=0.0,
        subsample=1.0,
        colsample_bytree=1.0,
        objective="binary:logistic",
        random_state=42,
    ).fit(X_train, y_train)

    predictions = model.predict(X_test)
    accuracy = accuracy_score(y_test, predictions)
    print(f"Titanic samples: {len(data)}")
    print(f"Train samples: {len(X_train)}, test samples: {len(X_test)}")
    print(f"Accuracy: {accuracy:.1%}")


if __name__ == "__main__":
    main()