"""
LeetCode 22. Generate Parentheses  (Medium)
============================================

Given n pairs of parentheses, write a function to generate all
combinations of well-formed parentheses.

Example:
    n = 3  →  ["((()))","(()())","(())()","()(())","()()()"]

Constraints:
    - 1 <= n <= 8

Function signature:
    generate_parenthesis(n: int) -> list[str]

Hint: backtracking — at each step you can add '(' if open < n, or ')'
      if close < open. Leaf nodes (open == close == n) are valid strings.
"""
from typing import List


def generate_parenthesis(n: int) -> List[str]:
    res = []
    def brackets(on=0, off=0, stack = []):
        if on == n and off == n:
            res.append("".join(stack))
        if 0 <= on < n:
            stack.append("(")
            brackets(on+1, off, stack)
            stack.pop()
        if off < on:
            stack.append(")")
            brackets(on, off+1, stack)
            stack.pop()
    brackets()
    return res


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: ['()']  (n=1)")
    print("got:           ", generate_parenthesis(1))

    print("Test 2 expected: ['(())', '()()']  (n=2, any order)")
    print("got:           ", sorted(generate_parenthesis(2)))

    print("Test 3 expected: 5 combinations  (n=3)")
    result = generate_parenthesis(3)
    print("got:           ", sorted(result), "  count:", len(result))
