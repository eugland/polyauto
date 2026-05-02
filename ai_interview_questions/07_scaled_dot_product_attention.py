"""
Question 7: Scaled Dot-Product Attention
========================================

Implement scaled dot-product attention from the "Attention Is All You
Need" paper:

    Attention(Q, K, V) = softmax( Q @ K^T / sqrt(d_k) ) @ V

Inputs:
    Q : (n_q, d_k) float array
    K : (n_k, d_k) float array
    V : (n_k, d_v) float array
    mask : optional (n_q, n_k) boolean array, where True means
           "this position is allowed to attend." Disallowed positions
           should get -inf logits BEFORE softmax (so they receive 0 weight).

Output:
    (n_q, d_v) float array.

Constraints:
    - numpy only. You may reuse your softmax from question 5.
"""
import numpy as np


def scaled_dot_product_attention(
    Q: np.ndarray,
    K: np.ndarray,
    V: np.ndarray,
    mask: np.ndarray | None = None,
) -> np.ndarray:
    raise NotImplementedError("Implement scaled_dot_product_attention")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    # Test 1: identity-style — Q==K, one query that perfectly matches one key.
    Q = np.array([[1.0, 0.0]])
    K = np.array([[1.0, 0.0], [0.0, 1.0], [-1.0, 0.0]])
    V = np.array([[10.0], [20.0], [30.0]])
    print("Test 1 expected: output ~ 10.0 (heavily attends to first key)")
    # print("got:            ", scaled_dot_product_attention(Q, K, V))

    # Test 2: uniform — all keys identical -> attention is uniform,
    # output is the mean of V.
    Q = np.array([[0.5, 0.5]])
    K = np.array([[1.0, 1.0], [1.0, 1.0], [1.0, 1.0]])
    V = np.array([[1.0], [2.0], [3.0]])
    print("Test 2 expected: 2.0 (mean of V)")
    # print("got:            ", scaled_dot_product_attention(Q, K, V))

    # Test 3: causal mask — query 0 can only see key 0; query 1 can see keys 0,1.
    Q = np.array([[1.0], [1.0]])
    K = np.array([[1.0], [1.0]])
    V = np.array([[10.0], [20.0]])
    mask = np.array([[True, False], [True, True]])
    print("Test 3 expected: row0 = 10.0, row1 = 15.0")
    # print("got:            ", scaled_dot_product_attention(Q, K, V, mask))

    # Test 4: shape check.
    rng = np.random.default_rng(0)
    Q = rng.normal(size=(4, 8))
    K = rng.normal(size=(6, 8))
    V = rng.normal(size=(6, 16))
    print("Test 4 expected: output shape == (4, 16)")
    # print("got shape:      ", scaled_dot_product_attention(Q, K, V).shape)
