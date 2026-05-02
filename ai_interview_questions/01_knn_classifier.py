"""
Question 1: K-Nearest Neighbors Classifier
==========================================

Implement a K-Nearest Neighbors (KNN) classifier from scratch using only
NumPy. Given a training set of points with labels, classify each query
point by majority vote among its K closest neighbors (Euclidean distance).

Tie-breaking: if there is a tie in vote counts, return the smaller label.

Function signature:
    knn_predict(X_train, y_train, X_query, k) -> np.ndarray
        X_train : (n_train, d) float array
        y_train : (n_train,)   int array of class labels
        X_query : (n_query, d) float array
        k       : int, number of neighbors
    Returns: (n_query,) int array of predicted labels.

Constraints:
    - You may use numpy. Do NOT use sklearn.
    - Vectorize the distance computation (no Python loops over training points).
"""
import numpy as np


def knn_predict(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_query: np.ndarray,
    k: int,
) -> np.ndarray:
    raise NotImplementedError("Implement knn_predict")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    # Test 1: trivial 1-D, k=1 — each query gets its closest training label.
    X_train = np.array([[0.0], [1.0], [2.0], [10.0], [11.0], [12.0]])
    y_train = np.array([0, 0, 0, 1, 1, 1])
    X_query = np.array([[0.5], [11.5], [6.0]])
    expected = np.array([0, 1, 0])  # 6.0 is closer to 2.0 than to 10.0
    print("Test 1 expected:", expected)
    # print("Test 1 got:     ", knn_predict(X_train, y_train, X_query, k=1))

    # Test 2: 2-D, k=3 — majority vote.
    X_train = np.array([[0, 0], [0, 1], [1, 0], [10, 10], [10, 11], [11, 10]])
    y_train = np.array([0, 0, 0, 1, 1, 1])
    X_query = np.array([[0.5, 0.5], [10.5, 10.5], [5, 5]])
    expected = np.array([0, 1, 0])  # 5,5 is closer to cluster 0 overall
    print("Test 2 expected:", expected)
    # print("Test 2 got:     ", knn_predict(X_train, y_train, X_query, k=3))

    # Test 3: tie-breaking — k=2 with one neighbor per class -> smaller label.
    X_train = np.array([[0.0], [1.0]])
    y_train = np.array([5, 2])
    X_query = np.array([[0.5]])
    expected = np.array([2])
    print("Test 3 expected:", expected)
    # print("Test 3 got:     ", knn_predict(X_train, y_train, X_query, k=2))
