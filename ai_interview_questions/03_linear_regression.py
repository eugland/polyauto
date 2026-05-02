"""
Question 3: Linear Regression via Gradient Descent
==================================================

Implement linear regression y = X @ w + b trained with batch gradient
descent on Mean Squared Error (MSE) loss.

    MSE = (1 / n) * sum((y_pred - y)^2)

Function signature:
    train_linear_regression(X, y, lr=0.01, n_iters=1000) -> (w, b)
        X       : (n, d) float array
        y       : (n,)   float array
        lr      : float, learning rate
        n_iters : int,   number of full-batch gradient steps
    Returns:
        w : (d,) float array
        b : float

Constraints:
    - numpy only.
    - Do NOT use the closed-form / normal-equation solution. Use gradient
      descent. (You can use the closed form to *check* your answer if you like.)
"""
import numpy as np


def train_linear_regression(
    X: np.ndarray, y: np.ndarray, lr: float = 0.01, n_iters: int = 1000
) -> tuple[np.ndarray, float]:
    raise NotImplementedError("Implement train_linear_regression")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    rng = np.random.default_rng(0)

    # Test 1: y = 3x + 2, no noise. Expect w ~ [3.0], b ~ 2.0.
    X = np.linspace(-1, 1, 50).reshape(-1, 1)
    y = (3 * X.ravel() + 2)
    print("Test 1 expected: w ~ [3.0], b ~ 2.0")
    # w, b = train_linear_regression(X, y, lr=0.1, n_iters=2000)
    # print("got: w =", w, "b =", b)

    # Test 2: y = 2*x1 - x2 + 0.5, with mild noise.
    X = rng.normal(size=(200, 2))
    y = 2 * X[:, 0] - X[:, 1] + 0.5 + rng.normal(scale=0.01, size=200)
    print("Test 2 expected: w ~ [2.0, -1.0], b ~ 0.5")
    # w, b = train_linear_regression(X, y, lr=0.05, n_iters=5000)
    # print("got: w =", w, "b =", b)

    # Test 3: constant target — w should be ~0, b should be the mean.
    X = rng.normal(size=(50, 3))
    y = np.full(50, 7.0)
    print("Test 3 expected: w ~ [0,0,0], b ~ 7.0")
    # w, b = train_linear_regression(X, y, lr=0.05, n_iters=2000)
    # print("got: w =", w, "b =", b)
