"""
LeetCode 10. Regular Expression Matching  (Hard)
================================================

Given an input string `s` and a pattern `p`, implement regular
expression matching with support for '.' and '*'.

    '.' matches any SINGLE character.
    '*' matches ZERO or more of the PRECEDING element.

The match must cover the ENTIRE input string (not partial).

Constraints:
    - 1 <= len(s) <= 20, 1 <= len(p) <= 20
    - `s` contains only lowercase English letters.
    - `p` contains only lowercase English letters, '.', and '*'.
    - It is guaranteed that for each appearance of '*', there will be
      a previous valid character to match.

Function signature:
    is_match(s: str, p: str) -> bool

Hint: think about recursion + memoization, or 2-D DP indexed by (i, j).
"""


def is_match(s: str, p: str) -> bool:
    raise NotImplementedError("Implement is_match")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: False  (\"aa\" vs \"a\")")
    # print("got:           ", is_match("aa", "a"))

    print("Test 2 expected: True   (\"aa\" vs \"a*\")")
    # print("got:           ", is_match("aa", "a*"))

    print("Test 3 expected: True   (\"ab\" vs \".*\")")
    # print("got:           ", is_match("ab", ".*"))

    print("Test 4 expected: True   (\"aab\" vs \"c*a*b\")")
    # print("got:           ", is_match("aab", "c*a*b"))

    print("Test 5 expected: False  (\"mississippi\" vs \"mis*is*p*.\")")
    # print("got:           ", is_match("mississippi", "mis*is*p*."))

    print("Test 6 expected: True   (empty s vs \"a*\" matches zero a's)")
    # print("got:           ", is_match("", "a*"))
