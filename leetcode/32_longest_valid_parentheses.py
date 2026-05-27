"""
LeetCode 32. Longest Valid Parentheses  (Hard)
===============================================

Given a string containing just '(' and ')', return the length of the longest
valid (well-formed) parentheses substring.

Example:
    "(()"    →  2
    ")()())" →  4
    ""       →  0

Constraints:
    - 0 <= len(s) <= 3 * 10^4
    - s[i] is '(' or ')'

Function signature:
    longest_valid_parentheses(s: str) -> int

Hint: use a stack that stores the index of the last unmatched ')'.
      Alternatively, two-pass left-to-right / right-to-left with counters.
"""


def longest_valid_parentheses(s: str) -> int:
    stack = [-1]
    max_len = 0

    for i, ch in enumerate(s):
        if ch == '(':
            stack.append(i)
        else:
            stack.pop()
            if not stack:
                stack.append(i)
            else:
                max_len = max(max_len, i - stack[-1])

    return max_len


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: 2   ('(()')")
    print("got:           ", longest_valid_parentheses("(()"))

    print("Test 2 expected: 4   (')()())')")
    print("got:           ", longest_valid_parentheses(")()())"))

    print("Test 3 expected: 0   ('')")
    print("got:           ", longest_valid_parentheses(""))

    print("Test 4 expected: 6   ('()()()')")
    print("got:           ", longest_valid_parentheses("()()()"))
