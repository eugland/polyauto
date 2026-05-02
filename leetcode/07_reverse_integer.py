"""
LeetCode 7. Reverse Integer  (Medium)
=====================================

Given a signed 32-bit integer `x`, return `x` with its digits reversed.

If reversing causes the value to go OUTSIDE the signed 32-bit range
[-2^31, 2^31 - 1], return 0.

Constraints:
    - -2^31 <= x <= 2^31 - 1
    - Assume the environment does NOT allow you to store 64-bit integers
      (this constraint is more meaningful in C/C++ than in Python — but
       the spirit of the question is "detect overflow without relying on
       big-int arithmetic").

Function signature:
    reverse(x: int) -> int
"""


def reverse(x: int) -> int:
    raise NotImplementedError("Implement reverse")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: 321")
    # print("got:           ", reverse(123))

    print("Test 2 expected: -321")
    # print("got:           ", reverse(-123))

    print("Test 3 expected: 21        (trailing zero dropped)")
    # print("got:           ", reverse(120))

    print("Test 4 expected: 0         (overflow: 9646324351 > 2^31 - 1)")
    # print("got:           ", reverse(1534236469))

    print("Test 5 expected: 0")
    # print("got:           ", reverse(0))
