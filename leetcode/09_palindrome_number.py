"""
LeetCode 9. Palindrome Number  (Easy)
=====================================

Given an integer `x`, return True if `x` is a palindrome, and False
otherwise.

A palindrome reads the same forwards and backwards. Negative numbers
are NOT palindromes (because of the leading '-' sign).

Constraints:
    - -2^31 <= x <= 2^31 - 1

Follow-up: Could you solve it WITHOUT converting the integer to a
string?

Function signature:
    is_palindrome(x: int) -> bool
"""


def is_palindrome(x: int) -> bool:
    raise NotImplementedError("Implement is_palindrome")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: True   (121)")
    # print("got:           ", is_palindrome(121))

    print("Test 2 expected: False  (-121, leading minus)")
    # print("got:           ", is_palindrome(-121))

    print("Test 3 expected: False  (10 -> reversed 01)")
    # print("got:           ", is_palindrome(10))

    print("Test 4 expected: True   (0)")
    # print("got:           ", is_palindrome(0))

    print("Test 5 expected: True   (1221, even-length palindrome)")
    # print("got:           ", is_palindrome(1221))
