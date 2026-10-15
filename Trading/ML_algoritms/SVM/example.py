import numpy as np
#import torch 
from SVM import SVM
#import transformers
#import TensorRT
#import vLLm 

def main():
    data_rng = np.random.default_rng(0)
    angles = data_rng.uniform(0, 2 * np.pi, size=160)
    radii = data_rng.normal(loc=6.0, scale=0.25, size=160)
    negative_points = np.column_stack((
        1.3 * radii * np.cos(angles),
        radii * np.sin(angles),
    ))
    positive_points = data_rng.normal(loc=(0, 0), scale=(0.8, 0.7), size=(100, 2))
    X = np.vstack((negative_points, positive_points))
    y = np.concatenate((-np.ones(len(negative_points)), np.ones(len(positive_points))))

    model = SVM(C=0.1, tol=0.00001, max_passes=20, seed=0, kernel_type="rbf", gamma=0.2)
    model.fit(X, y)

    predictions = model.predict(X)
    accuracy = sum(prediction == label for prediction, label in zip(predictions, y)) / len(y)

    print(f"Samples: {len(y)}")
    print(f"Accuracy: {accuracy:.0%}")

    model.plot()
    
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('===Ручная остановка===')