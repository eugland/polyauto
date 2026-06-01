"""
LeetCode 38. Count and Say  (Medium)
======================================

The count-and-say sequence is a sequence of digit strings defined by the
recursive formula:
  - countAndSay(1) = "1"
  - countAndSay(n) is the run-length encoding of countAndSay(n - 1).

Run-length encoding (RLE) compresses consecutive identical characters by
prefixing them with their count.

Example:
    n = 1  →  "1"
    n = 4  →  "1211"
    n = 5  →  "111221"

Sequence:
    1:  "1"
    2:  "11"
    3:  "21"
    4:  "1211"
    5:  "111221"
    6:  "312211"

Constraints:
    - 1 <= n <= 30

Function signature:
    count_and_say(n: int) -> str
"""


def count_and_say(n: int) -> str:
    pass


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: '1'")
    print("got:           ", count_and_say(1))

    print("Test 2 expected: '11'")
    print("got:           ", count_and_say(2))

    print("Test 3 expected: '21'")
    print("got:           ", count_and_say(3))

    print("Test 4 expected: '1211'")
    print("got:           ", count_and_say(4))

    print("Test 5 expected: '111221'")
    print("got:           ", count_and_say(5))

    print("Test 6 expected: '312211'")
    print("got:           ", count_and_say(6))
