"""
LeetCode 27. Remove Element  (Easy)
=====================================

Given an integer array `nums` and an integer `val`, remove all
occurrences of `val` in-place. The relative order of the remaining
elements may be changed. Return the number of elements k that are
not equal to `val`.

The first k elements of `nums` must hold values != val; the
remaining elements do not matter.

Example:
    Input:  nums=[3,2,2,3], val=3  →  k=2, nums[:2]=[2,2]
    Input:  nums=[0,1,2,2,3,0,4,2], val=2  →  k=5, nums[:5]=[0,1,3,0,4]

Constraints:
    - 0 <= len(nums) <= 100
    - 0 <= nums[i] <= 50
    - 0 <= val <= 100

Function signature:
    remove_element(nums: list[int], val: int) -> int

Hint: two-pointer — k is the write cursor; copy nums[i] to nums[k]
      whenever nums[i] != val, then increment k.
"""
from typing import List


def remove_element(nums: List[int], val: int) -> int:
    slow = 0 
    for fast in range(len(nums)):
        if nums[fast] == val:
            continue
        nums[slow] = nums[fast]
        slow += 1
    return slow


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    nums1 = [3, 2, 2, 3]
    print("Test 1 expected: k=2, elements are [2,2] (any order)")
    k = remove_element(nums1, 3)
    print("got:           ", k, sorted(nums1[:k]))

    nums2 = [0, 1, 2, 2, 3, 0, 4, 2]
    print("Test 2 expected: k=5, elements are [0,0,1,3,4] (any order)")
    k = remove_element(nums2, 2)
    print("got:           ", k, sorted(nums2[:k]))

    nums3 = []
    print("Test 3 expected: k=0  (empty array)")
    print("got:           ", remove_element(nums3, 1))
