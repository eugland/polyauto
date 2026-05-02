"""
Question 2: K-Means Clustering
==============================

Implement Lloyd's algorithm for K-Means clustering from scratch.

Steps:
    1. Initialize k centroids (use the first k points of X for determinism).
    2. Repeat until convergence (or max_iters):
        a. Assign each point to its nearest centroid.
        b. Recompute each centroid as the mean of its assigned points.
    3. Stop when assignments stop changing or max_iters is reached.

Edge case: if a cluster ends up empty, leave its centroid unchanged.

Function signature:
    kmeans(X, k, max_iters=100) -> (centroids, labels)
        X         : (n, d) float array
        k         : int, number of clusters
        max_iters : int
    Returns:
        centroids : (k, d) float array
        labels    : (n,)   int array, each in [0, k-1]

Constraints:
    - numpy only, no sklearn.
"""
import numpy as np


def kmeans(
    X: np.ndarray, k: int, max_iters: int = 100
) -> tuple[np.ndarray, np.ndarray]:
    raise NotImplementedError("Implement kmeans")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    # Test 1: two well-separated clusters, k=2.
    X = np.array([
        [0.0, 0.0], [0.1, 0.1], [-0.1, 0.0],
        [10.0, 10.0], [10.1, 9.9], [9.9, 10.1],
    ])
    # Expected: points 0,1,2 in one cluster, points 3,4,5 in the other.
    # Centroids should be near (0,0) and (10,10) (label IDs may swap).
    print("Test 1: expect two clusters near (0,0) and (10,10)")
    # centroids, labels = kmeans(X, k=2)
    # print("centroids:", centroids)
    # print("labels:   ", labels)

    # Test 2: three clusters in 1D.
    X = np.array([[0.], [0.5], [5.], [5.5], [10.], [10.5]])
    print("Test 2: expect three clusters near 0.25, 5.25, 10.25")
    # centroids, labels = kmeans(X, k=3)
    # print("centroids:", centroids.ravel())

    # Test 3: degenerate — k equals n, every point is its own cluster.
    X = np.array([[1., 2.], [3., 4.], [5., 6.]])
    print("Test 3: expect centroids == X (each point its own cluster)")
    # centroids, labels = kmeans(X, k=3)
    # print("centroids:", centroids)
