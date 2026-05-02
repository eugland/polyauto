"""
LeetCode 8. String to Integer (atoi)  (Medium)
==============================================

Implement `my_atoi(s)` — convert a string to a signed 32-bit integer,
similar to C/C++'s `atoi`. The algorithm:

    1. Skip any leading whitespace.
    2. Read an optional '+' or '-' sign.
    3. Read digits until a non-digit (or end of string). Convert them
       to an integer.
    4. Clamp the result to the 32-bit signed range
       [-2^31, 2^31 - 1].
    5. Return the result. If no digits were read, return 0.

Constraints:
    - 0 <= len(s) <= 200
    - `s` consists of English letters (lower- and uppercase), digits
      (0-9), ' ', '+', '-', and '.'.

Function signature:
    my_atoi(s: str) -> int
"""


def my_atoi(s: str) -> int:
    raise NotImplementedError("Implement my_atoi")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: 42")
    # print("got:           ", my_atoi("42"))

    print("Test 2 expected: -42       (leading whitespace + sign)")
    # print("got:           ", my_atoi("   -42"))

    print("Test 3 expected: 4193      (stop at first non-digit)")
    # print("got:           ", my_atoi("4193 with words"))

    print("Test 4 expected: 0         (no leading digits)")
    # print("got:           ", my_atoi("words and 987"))

    print("Test 5 expected: -2147483648  (clamp to INT_MIN)")
    # print("got:           ", my_atoi("-91283472332"))

    print("Test 6 expected: 0         (just whitespace)")
    # print("got:           ", my_atoi("   "))

    print("Test 7 expected: 0         ('+' alone, no digits)")
    # print("got:           ", my_atoi("+"))
