"""
LeetCode 4. Median of Two Sorted Arrays  (Hard)
===============================================

Given two sorted arrays `nums1` and `nums2` of size m and n respectively,
return the median of the two sorted arrays.

Constraints:
    - nums1.length == m, nums2.length == n
    - 0 <= m, n <= 1000, 1 <= m + n
    - -10^6 <= nums[i] <= 10^6

Required time complexity: O(log(min(m, n))).
(A merge-based O(m + n) solution exists but does NOT meet the bar.)

Function signature:
    find_median_sorted_arrays(nums1: list[int], nums2: list[int]) -> float
"""
from typing import List


def find_median_sorted_arrays(nums1: List[int], nums2: List[int]) -> float:
    # 1. calculate the midpoint: if odd length 1 + 2 = 3, 3//2 = 1
    # if even： 2 + 2 = 4, 4//2 = 2, mid = 1,2 so -1 too
    # 2. insert until the midpoint
    # done return
    total = len(nums1) + len(nums2)
    midpoint = (len(nums1)  + len(nums2)) // 2




# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    # Odd total length
    print("Test 1 expected: 2.0")
    # print("got:           ", find_median_sorted_arrays([1, 3], [2]))

    # Even total length
    print("Test 2 expected: 2.5")
    # print("got:           ", find_median_sorted_arrays([1, 2], [3, 4]))

    # One empty array
    print("Test 3 expected: 1.0")
    # print("got:           ", find_median_sorted_arrays([], [1]))

    # All same
    print("Test 4 expected: 0.0")
    # print("got:           ", find_median_sorted_arrays([0, 0], [0, 0]))

    # Disjoint ranges
    print("Test 5 expected: 5.5")
    # print("got:           ", find_median_sorted_arrays([1, 2, 3, 4], [5, 6, 7, 8, 9, 10]))
