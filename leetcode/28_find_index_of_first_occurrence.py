"""
LeetCode 28. Find the Index of the First Occurrence in a String  (Easy)
========================================================================

Given two strings `haystack` and `needle`, return the index of the
first occurrence of `needle` in `haystack`, or -1 if `needle` is not
part of `haystack`.

Example:
    haystack = "sadbutsad",  needle = "sad"  →  0
    haystack = "leetcode",   needle = "leeto" →  -1

Constraints:
    - 1 <= len(haystack), len(needle) <= 10^4
    - haystack and needle consist of only lowercase English letters.

Function signature:
    str_str(haystack: str, needle: str) -> int

Hint: a naive O(n*m) sliding window is accepted within constraints.
      For an O(n+m) solution, look up the KMP algorithm.
"""


def str_str(haystack: str, needle: str) -> int:
    for i in range(len(haystack) - len(needle)+1):
        if haystack[i:i+len(needle)] == needle:
            return i
    return -1 


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: 0   (\"sadbutsad\", \"sad\")")
    print("got:           ", str_str("sadbutsad", "sad"))

    print("Test 2 expected: -1  (\"leetcode\", \"leeto\")")
    print("got:           ", str_str("leetcode", "leeto"))

    print("Test 3 expected: 2   (\"hello\", \"ll\")")
    print("got:           ", str_str("hello", "ll"))

    print("Test 4 expected: 4   (second occurrence ignored, \"aabaabaaf\", \"aabaaf\")")
    print("got:           ", str_str("aabaabaaf", "aabaaf"))
