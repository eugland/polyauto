"""
LeetCode 6. Zigzag Conversion  (Medium)
=======================================

The string "PAYPALISHIRING" is written in a zigzag pattern on a given
number of rows like this (read top-to-bottom, left-to-right):

    P   A   H   N
    A P L S I I G
    Y   I   R

And then read line by line: "PAHNAPLSIIGYIR".

Write the code that takes a string and an int `numRows` and returns the
string in this zigzag form.

Constraints:
    - 1 <= len(s) <= 1000
    - `s` consists of English letters (lower- and uppercase), ',' and '.'
    - 1 <= numRows <= 1000

Function signature:
    convert(s: str, numRows: int) -> str
"""


def convert(s: str, numRows: int) -> str:
    raise NotImplementedError("Implement convert")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: \"PAHNAPLSIIGYIR\"")
    # print("got:           ", convert("PAYPALISHIRING", 3))

    print("Test 2 expected: \"PINALSIGYAHRPI\"")
    # print("got:           ", convert("PAYPALISHIRING", 4))

    print("Test 3 expected: \"A\"           (single row)")
    # print("got:           ", convert("A", 1))

    print("Test 4 expected: \"ABCD\"        (numRows >= len(s))")
    # print("got:           ", convert("ABCD", 4))

    print("Test 5 expected: \"AB\"          (single row, multiple chars)")
    # print("got:           ", convert("AB", 1))
