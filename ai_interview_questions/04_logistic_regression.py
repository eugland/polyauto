"""
Question 4: Logistic Regression via Gradient Descent
====================================================

Implement binary logistic regression trained with batch gradient descent
on the binary cross-entropy loss.

    p = sigmoid(X @ w + b)
    loss = -(1/n) * sum(y * log(p) + (1 - y) * log(1 - p))

Then implement a `predict` function that returns class labels {0, 1} using
a 0.5 threshold.

Function signatures:
    train_logistic_regression(X, y, lr=0.1, n_iters=1000) -> (w, b)
    predict(X, w, b) -> np.ndarray  # int labels in {0, 1}

Constraints:
    - numpy only. Implement sigmoid yourself.
    - Use a numerically stable formulation (no NaNs/Infs near 0 or 1).
"""
import numpy as np


def train_logistic_regression(
    X: np.ndarray, y: np.ndarray, lr: float = 0.1, n_iters: int = 1000
) -> tuple[np.ndarray, float]:
    raise NotImplementedError("Implement train_logistic_regression")


def predict(X: np.ndarray, w: np.ndarray, b: float) -> np.ndarray:
    raise NotImplementedError("Implement predict")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    rng = np.random.default_rng(0)

    # Test 1: linearly separable in 2D. Expect 100% accuracy.
    X_pos = rng.normal(loc=[2, 2], scale=0.3, size=(50, 2))
    X_neg = rng.normal(loc=[-2, -2], scale=0.3, size=(50, 2))
    X = np.vstack([X_pos, X_neg])
    y = np.concatenate([np.ones(50), np.zeros(50)]).astype(int)
    print("Test 1 expected: accuracy = 1.00")
    # w, b = train_logistic_regression(X, y, lr=0.1, n_iters=2000)
    # acc = (predict(X, w, b) == y).mean()
    # print("got accuracy:", acc)

    # Test 2: noisy but still mostly separable. Expect accuracy >= 0.9.
    X_pos = rng.normal(loc=[1, 1], scale=1.0, size=(200, 2))
    X_neg = rng.normal(loc=[-1, -1], scale=1.0, size=(200, 2))
    X = np.vstack([X_pos, X_neg])
    y = np.concatenate([np.ones(200), np.zeros(200)]).astype(int)
    print("Test 2 expected: accuracy >= 0.90")
    # w, b = train_logistic_regression(X, y, lr=0.1, n_iters=2000)
    # acc = (predict(X, w, b) == y).mean()
    # print("got accuracy:", acc)

    # Test 3: numerical stability — large logits should NOT produce NaN.
    X = np.array([[100.0], [-100.0]])
    y = np.array([1, 0])
    print("Test 3 expected: no NaNs, accuracy = 1.00")
    # w, b = train_logistic_regression(X, y, lr=0.1, n_iters=500)
    # print("w =", w, "b =", b, "predictions =", predict(X, w, b))
