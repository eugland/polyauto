"""
LeetCode 1. Two Sum  (Easy)
===========================

Given an array of integers `nums` and an integer `target`, return the
indices of the two numbers such that they add up to `target`.

You may assume that each input has exactly ONE solution, and you may
not use the same element twice. You can return the answer in any order.

Constraints:
    - 2 <= len(nums) <= 10^4
    - -10^9 <= nums[i] <= 10^9
    - -10^9 <= target <= 10^9
    - Only one valid answer exists.

Follow-up: Can you do it in less than O(n^2) time?

Function signature:
    two_sum(nums: list[int], target: int) -> list[int]
"""
from typing import List


def two_sum(nums: List[int], target: int) -> List[int]:
    set = {}
    for i, num in enumerate(nums):
        if target - num in set:
            return [set[target - num], i]
        set[num] = i
    return [-1, -1]



# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    # Test 1
    print("Test 1 expected: [0, 1]")
    print("got:           ", two_sum([2, 7, 11, 15], 9))

    # Test 2
    print("Test 2 expected: [1, 2]")
    print("got:           ", two_sum([3, 2, 4], 6))

    # Test 3 — duplicates
    print("Test 3 expected: [0, 1]")
    print("got:           ", two_sum([3, 3], 6))

    # Test 4 — negatives (problem guarantees exactly one solution)
    print("Test 4 expected: [0, 2]   (-3 + 8 = 5)")
    print("got:           ", two_sum([-3, 4, 8, 2], 5))
