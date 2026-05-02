"""
Question 9: Top-K and Top-P (Nucleus) Sampling
==============================================

LLMs sample the next token from a probability distribution over the
vocabulary. Two common filters before sampling:

    Top-K:   Keep only the k tokens with the highest probability,
             zero out the rest, and renormalize to sum to 1.

    Top-P:   Sort tokens by descending probability. Keep the smallest
             prefix whose cumulative probability is >= p. Always keep at
             least one token. Zero out the rest and renormalize.

Implement both functions. They take logits (raw scores) and return a
probability distribution of the same shape.

Function signatures:
    top_k_filter(logits, k) -> np.ndarray   # 1-D probabilities, sums to 1
    top_p_filter(logits, p) -> np.ndarray   # 1-D probabilities, sums to 1

Constraints:
    - numpy only.
    - You may use your softmax from question 5.
    - Don't actually draw a random sample — just return the filtered
      probability distribution.
"""
import numpy as np


def top_k_filter(logits: np.ndarray, k: int) -> np.ndarray:
    raise NotImplementedError("Implement top_k_filter")


def top_p_filter(logits: np.ndarray, p: float) -> np.ndarray:
    raise NotImplementedError("Implement top_p_filter")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    logits = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    # Full softmax probs ~ [0.0117, 0.0317, 0.0861, 0.2341, 0.6364]

    # Test 1: top_k with k=2 — only top 2 logits (4.0, 5.0) survive.
    print("Test 1 expected: probabilities[:3] all 0; last two sum to 1")
    # out = top_k_filter(logits, k=2)
    # print("got:", out, "sum =", out.sum())

    # Test 2: top_k with k=1 — distribution is one-hot on the argmax.
    print("Test 2 expected: [0, 0, 0, 0, 1.0]")
    # print("got:", top_k_filter(logits, k=1))

    # Test 3: top_p with p=0.9 — likely keeps top 2 tokens
    # (0.6364 + 0.2341 = 0.8705 < 0.9, so we also include 0.0861 -> top 3).
    print("Test 3 expected: top 3 tokens kept, others 0, sum to 1")
    # out = top_p_filter(logits, p=0.9)
    # print("got:", out, "nonzero count =", (out > 0).sum())

    # Test 4: top_p with p=0.1 — even the largest single token (0.6364)
    # already exceeds 0.1, so we keep just one token.
    print("Test 4 expected: only one nonzero entry (the argmax)")
    # print("got:", top_p_filter(logits, p=0.1))

    # Test 5: top_p with p=1.0 — keep everything.
    print("Test 5 expected: all 5 entries nonzero, sum to 1")
    # print("got:", top_p_filter(logits, p=1.0))
