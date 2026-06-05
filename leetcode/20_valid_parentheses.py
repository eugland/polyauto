"""
LeetCode 20. Valid Parentheses  (Easy)
======================================

Given a string `s` containing just the characters '(', ')', '{', '}',
'[' and ']', determine if the input string is valid.

An input string is valid if:
    1. Open brackets must be closed by the same type of brackets.
    2. Open brackets must be closed in the correct order.
    3. Every close bracket has a corresponding open bracket of the
       same type.

Constraints:
    - 1 <= len(s) <= 10^4
    - s consists of parentheses characters only: '()[]{}'

Function signature:
    is_valid(s: str) -> bool

Hint: use a stack — push opening brackets; on a closing bracket,
      check if the top of the stack is the matching opener.
"""


def is_valid(s: str) -> bool:
    arr = []
    dic = {
        ")": "(",
        "]": "[",
        ">": "<", 
        "}": "{"
    }

    for ch in s: 
        if ch in "{[(":
            arr.append(ch)
        elif ch in dic:
            op = dic[ch]
            if not arr or arr[-1] != op:
                return False
            arr.pop()
    return not arr



# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: True   (\"()\")")
    print("got:           ", is_valid("()"))

    print("Test 2 expected: True   (\"()[]{}\")")
    print("got:           ", is_valid("()[]{}"))

    print("Test 3 expected: False  (\"(]\")")
    print("got:           ", is_valid("(]"))

    print("Test 4 expected: True   (\"{[]}\")")
    print("got:           ", is_valid("{[]}"))

    print("Test 5 expected: False  (\"([)]\")")
    print("got:           ", is_valid("([)]"))
