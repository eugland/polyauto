"""
Question 5: Numerically Stable Softmax
======================================

Implement the softmax function:

    softmax(x)_i = exp(x_i) / sum_j exp(x_j)

It must be numerically stable — large input values like [1000, 1001, 1002]
must NOT produce inf/NaN. (Hint: subtract the max along the reduction axis
before exponentiating.)

Support an `axis` argument so it works on any tensor shape:
    - 1D input: axis=-1 returns a probability vector summing to 1.
    - 2D input with axis=-1: each row sums to 1.
    - 2D input with axis=0:  each column sums to 1.

Function signature:
    softmax(x, axis=-1) -> np.ndarray

Constraints:
    - numpy only. Do not use scipy.special.softmax.
"""
import numpy as np


def softmax(x: np.ndarray, axis: int = -1) -> np.ndarray:
    raise NotImplementedError("Implement softmax")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    # Test 1: simple 1D — symmetric input gives uniform distribution.
    x = np.array([1.0, 1.0, 1.0])
    print("Test 1 expected: [0.333, 0.333, 0.333]")
    # print("got:           ", softmax(x))

    # Test 2: 1D, peaked input — last element should dominate.
    x = np.array([1.0, 2.0, 3.0])
    expected = np.array([0.09003057, 0.24472847, 0.66524096])
    print("Test 2 expected:", expected)
    # print("got:           ", softmax(x))

    # Test 3: numerical stability — must not overflow.
    x = np.array([1000.0, 1001.0, 1002.0])
    print("Test 3 expected: same as Test 2 (shift-invariant), no inf/NaN")
    # print("got:           ", softmax(x))

    # Test 4: 2D with axis=-1 — each row sums to 1.
    x = np.array([[1.0, 2.0, 3.0], [1.0, 1.0, 1.0]])
    print("Test 4 expected: each row sums to 1.0")
    # out = softmax(x, axis=-1)
    # print("row sums:", out.sum(axis=-1))

    # Test 5: 2D with axis=0 — each column sums to 1.
    print("Test 5 expected: each column sums to 1.0")
    # out = softmax(x, axis=0)
    # print("col sums:", out.sum(axis=0))
