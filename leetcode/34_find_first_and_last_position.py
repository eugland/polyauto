"""
LeetCode 34. Find First and Last Position of Element in Sorted Array  (Medium)
===============================================================================

Given an array of integers `nums` sorted in non-decreasing order, find the
starting and ending position of a given `target` value.  If target is not found,
return [-1, -1].  Must run in O(log n).

Example:
    nums = [5,7,7,8,8,10], target = 8  →  [3, 4]
    nums = [5,7,7,8,8,10], target = 6  →  [-1, -1]
    nums = [],              target = 0  →  [-1, -1]

Constraints:
    - 0 <= len(nums) <= 10^5
    - -10^9 <= nums[i] <= 10^9
    - nums is non-decreasing.

Function signature:
    search_range(nums: list[int], target: int) -> list[int]
"""

import bisect


def search_range(nums: list, target: int) -> list:
    lo = bisect.bisect_left(nums, target)
    if lo == len(nums) or nums[lo] != target:
        return [-1, -1]
    hi = bisect.bisect_right(nums, target) - 1
    return [lo, hi]


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: [3, 4]")
    print("got:           ", search_range([5,7,7,8,8,10], 8))

    print("Test 2 expected: [-1, -1]")
    print("got:           ", search_range([5,7,7,8,8,10], 6))

    print("Test 3 expected: [-1, -1]")
    print("got:           ", search_range([], 0))

    print("Test 4 expected: [0, 0]")
    print("got:           ", search_range([1], 1))
