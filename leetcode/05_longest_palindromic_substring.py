"""
LeetCode 5. Longest Palindromic Substring  (Medium)
===================================================

Given a string `s`, return the longest palindromic SUBSTRING (contiguous)
in `s`. If multiple substrings tie for longest, returning any one of them
is acceptable.

Constraints:
    - 1 <= len(s) <= 1000
    - `s` consists of digits and English letters.

Common approaches:
    - Expand-around-center: O(n^2) time, O(1) extra space.
    - DP: O(n^2) time and space.
    - Manacher's algorithm: O(n) time (advanced).

Function signature:
    longest_palindrome(s: str) -> str
"""


def longest_palindrome(s: str) -> str:
    raise NotImplementedError("Implement longest_palindrome")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: \"bab\" or \"aba\"")
    # print("got:           ", longest_palindrome("babad"))

    print("Test 2 expected: \"bb\"")
    # print("got:           ", longest_palindrome("cbbd"))

    print("Test 3 expected: \"a\"")
    # print("got:           ", longest_palindrome("a"))

    print("Test 4 expected: any single character (e.g. \"a\" or \"c\")")
    # print("got:           ", longest_palindrome("ac"))

    print("Test 5 expected: \"anana\"")
    # print("got:           ", longest_palindrome("bananas"))
