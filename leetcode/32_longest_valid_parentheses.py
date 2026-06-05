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
    # ( is a valid start, keep going, ((())()
    # as opens keep track of open an position, when closing, read previous bracket to get last valid length? 
    "))()"
    max_len = 0
    last = [-1]
    for i in range(len(s)):
        if s[i] == '(':
            last.append(i)
        else:
            last.pop()
            if not last:
                last.append(i)
            else:
                last_invalid = last[-1]
                max_len = max(max_len, i - last_invalid)
            # print(last_invalid, i, last)
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
