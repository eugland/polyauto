"""
Question 10: 2-Layer MLP — Forward and Backward Pass
====================================================

Implement the forward pass, loss, and backward pass (gradients) for a
small 2-layer MLP for binary classification — by hand, with no autograd.

Architecture:
    z1 = X @ W1 + b1            # (n, h)
    a1 = relu(z1)               # (n, h)
    z2 = a1 @ W2 + b2           # (n, 1)
    p  = sigmoid(z2)            # (n, 1) — predicted P(y=1)
    loss = binary cross-entropy(p, y)   # scalar (mean over batch)

Implement:

    forward(X, params) -> (p, cache)
        params: dict with keys 'W1' (d, h), 'b1' (h,), 'W2' (h, 1), 'b2' (1,)
        Returns predictions p of shape (n, 1) and a cache with anything you
        need for the backward pass.

    bce_loss(p, y) -> float
        y has shape (n, 1) with values in {0, 1}.

    backward(y, cache, params) -> grads
        Returns a dict with the same keys as `params`, holding
        dLoss/dW1, dLoss/db1, dLoss/dW2, dLoss/db2 — same shapes as params.

Constraints:
    - numpy only. NO autograd. Derive the gradients yourself.
    - Use numerically stable sigmoid + BCE.
    - Verify with finite differences in the test below.
"""
import numpy as np


def forward(X: np.ndarray, params: dict) -> tuple[np.ndarray, dict]:
    raise NotImplementedError("Implement forward")


def bce_loss(p: np.ndarray, y: np.ndarray) -> float:
    raise NotImplementedError("Implement bce_loss")


def backward(y: np.ndarray, cache: dict, params: dict) -> dict:
    raise NotImplementedError("Implement backward")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    rng = np.random.default_rng(0)
    n, d, h = 8, 4, 5
    X = rng.normal(size=(n, d))
    y = rng.integers(0, 2, size=(n, 1)).astype(float)
    params = {
        "W1": rng.normal(size=(d, h)) * 0.1,
        "b1": np.zeros(h),
        "W2": rng.normal(size=(h, 1)) * 0.1,
        "b2": np.zeros(1),
    }

    # Test 1: shapes.
    print("Test 1: forward output shape should be (n, 1) = (8, 1)")
    # p, cache = forward(X, params)
    # print("got:", p.shape)

    # Test 2: loss is a finite scalar.
    print("Test 2: bce_loss should return a finite float")
    # print("got:", bce_loss(p, y))

    # Test 3: gradient check via finite differences.
    # Pick W1[0,0]; numerical grad should match analytical grad to ~1e-5.
    print("Test 3: |numerical_grad - analytical_grad| should be < 1e-5")
    # eps = 1e-6
    # p, cache = forward(X, params)
    # grads = backward(y, cache, params)
    # analytical = grads["W1"][0, 0]
    #
    # params["W1"][0, 0] += eps
    # loss_plus = bce_loss(forward(X, params)[0], y)
    # params["W1"][0, 0] -= 2 * eps
    # loss_minus = bce_loss(forward(X, params)[0], y)
    # params["W1"][0, 0] += eps  # restore
    # numerical = (loss_plus - loss_minus) / (2 * eps)
    # print("analytical:", analytical, "numerical:", numerical,
    #       "diff:", abs(analytical - numerical))

    # Test 4: training loop should reduce loss on a separable dataset.
    X = np.vstack([rng.normal(loc=2, size=(50, 2)),
                   rng.normal(loc=-2, size=(50, 2))])
    y = np.vstack([np.ones((50, 1)), np.zeros((50, 1))])
    print("Test 4: after 500 SGD steps, loss should drop and accuracy ~ 1.0")
    # params = {
    #     "W1": rng.normal(size=(2, 8)) * 0.1, "b1": np.zeros(8),
    #     "W2": rng.normal(size=(8, 1)) * 0.1, "b2": np.zeros(1),
    # }
    # lr = 0.05
    # for _ in range(500):
    #     p, cache = forward(X, params)
    #     grads = backward(y, cache, params)
    #     for k in params:
    #         params[k] -= lr * grads[k]
    # p, _ = forward(X, params)
    # print("final loss:", bce_loss(p, y),
    #       "acc:", ((p > 0.5).astype(float) == y).mean())
