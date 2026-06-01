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
    neg_count = 0
    if divisor == 0:
        return None
    elif divisor < 0:
        neg_count += 1
    
    if dividend == 0:
        return 0
    elif dividend < 0: 
        neg_count += 1

    dividend = abs(dividend)
    divisor = abs(divisor)

    look = [(1, divisor)]
    count, div = look[-1]
    while div < dividend:
        count = count + count
        div = div + div
        look.append((count, div))
    acc = 0
    while look:
        count, div = look.pop()
        if div <= dividend:
            dividend -= div
            acc += count
    if neg_count == 1:
        acc = -acc
    return acc




# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: 3   (10 / 3)")
    print(10//3)
    print("got:           ", divide(10, 3))

    print("Test 2 expected: -2  (7 / -3)")
    print(7//-3)
    print("got:           ", divide(7, -3))

    print("Test 3 expected: 1   (1 / 1)")
    print("got:           ", divide(1, 1))

    print("Test 4 expected: 2147483647  (INT_MIN / -1 clamp)")
    print("got:           ", divide(-2147483648, -1))
