"""
Question 6: Cross-Entropy Loss
==============================

Implement the categorical cross-entropy loss for a batch of predictions.

Inputs:
    logits : (n, c) float array — raw, unnormalized scores per class.
    labels : (n,)   int array   — true class index per example, in [0, c-1].

Output:
    A single scalar — the mean cross-entropy across the batch:

        loss = -(1/n) * sum_i log( softmax(logits_i)[labels_i] )

Constraints:
    - numpy only.
    - Must be numerically stable (no log(0), no overflow on large logits).
      Hint: combine log and softmax into log-softmax with the max-shift trick.
    - Do NOT call any softmax/cross-entropy library function.

Function signature:
    cross_entropy(logits, labels) -> float
"""
import numpy as np


def cross_entropy(logits: np.ndarray, labels: np.ndarray) -> float:
    raise NotImplementedError("Implement cross_entropy")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    # Test 1: perfectly confident, correct prediction -> loss ~ 0.
    logits = np.array([[100.0, 0.0, 0.0]])
    labels = np.array([0])
    print("Test 1 expected: ~0.0")
    # print("got:            ", cross_entropy(logits, labels))

    # Test 2: uniform logits over 3 classes -> loss = log(3) ~ 1.0986.
    logits = np.array([[0.0, 0.0, 0.0]])
    labels = np.array([1])
    print("Test 2 expected: ~1.0986 (= log 3)")
    # print("got:            ", cross_entropy(logits, labels))

    # Test 3: confidently wrong — large loss, but no overflow.
    logits = np.array([[1000.0, 0.0]])
    labels = np.array([1])
    print("Test 3 expected: ~1000.0, no inf/NaN")
    # print("got:            ", cross_entropy(logits, labels))

    # Test 4: batch — average over examples.
    logits = np.array([[2.0, 1.0, 0.1], [0.1, 1.0, 2.0]])
    labels = np.array([0, 2])
    # Both are confident-correct in the same way: per-example loss should match.
    print("Test 4 expected: ~0.4170 (mean of two equal losses)")
    # print("got:            ", cross_entropy(logits, labels))
