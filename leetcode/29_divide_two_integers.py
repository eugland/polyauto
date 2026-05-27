"""
LeetCode 29. Divide Two Integers  (Medium)
===========================================

Given two integers `dividend` and `divisor`, divide them without using
multiplication, division, or mod operator.  Return the quotient, truncated
toward zero.  If the result overflows a 32-bit signed integer, return 2^31 - 1.

Example:
    dividend = 10, divisor = 3  →  3
    dividend = 7,  divisor = -3 →  -2

Constraints:
    - -2^31 <= dividend, divisor <= 2^31 - 1
    - divisor != 0

Function signature:
    divide(dividend: int, divisor: int) -> int

Hint: double the divisor (left-shift) until it exceeds the dividend, then
      subtract and accumulate the corresponding power-of-2 quotient.
"""

INT_MAX = 2**31 - 1
INT_MIN = -(2**31)


def divide(dividend: int, divisor: int) -> int:
    if dividend == INT_MIN and divisor == -1:
        return INT_MAX

    negative = (dividend < 0) != (divisor < 0)
    a, b = abs(dividend), abs(divisor)
    result = 0

    while a >= b:
        temp, multiple = b, 1
        while a >= (temp << 1):
            temp <<= 1
            multiple <<= 1
        a -= temp
        result += multiple

    return -result if negative else result


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: 3   (10 / 3)")
    print("got:           ", divide(10, 3))

    print("Test 2 expected: -2  (7 / -3)")
    print("got:           ", divide(7, -3))

    print("Test 3 expected: 1   (1 / 1)")
    print("got:           ", divide(1, 1))

    print("Test 4 expected: 2147483647  (INT_MIN / -1 clamp)")
    print("got:           ", divide(-2147483648, -1))
