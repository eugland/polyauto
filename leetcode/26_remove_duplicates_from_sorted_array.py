"""
LeetCode 26. Remove Duplicates from Sorted Array  (Easy)
=========================================================

Given an integer array `nums` sorted in non-decreasing order, remove
the duplicates in-place such that each unique element appears only once.
The relative order of the elements should be kept the same. Return the
number of unique elements k.

The first k elements of `nums` must hold the unique values; the
remaining elements do not matter.

Example:
    Input:  nums = [1,1,2]
    Output: 2,  nums = [1,2,_]

    Input:  nums = [0,0,1,1,1,2,2,3,3,4]
    Output: 5,  nums = [0,1,2,3,4,_,_,_,_,_]

Constraints:
    - 1 <= len(nums) <= 3 * 10^4
    - -100 <= nums[i] <= 100
    - nums is sorted in non-decreasing order.

Function signature:
    remove_duplicates(nums: list[int]) -> int

Hint: two-pointer — k tracks the write position; advance the right
      pointer and write whenever nums[right] != nums[k-1].
"""
from typing import List


def remove_duplicates(nums: List[int]) -> int:
    slow = 1
    fast = 1
    for fast in range(1, len(nums)):
        if nums[fast] != nums[fast -1]:
            nums[slow] = nums[fast]
            slow += 1
    return slow


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    nums1 = [1, 1, 2]
    print("Test 1 expected: k=2, nums[:2]=[1,2]")
    k = remove_duplicates(nums1)
    print("got:           ", k, nums1[:k])

    nums2 = [0, 0, 1, 1, 1, 2, 2, 3, 3, 4]
    print("Test 2 expected: k=5, nums[:5]=[0,1,2,3,4]")
    k = remove_duplicates(nums2)
    print("got:           ", k, nums2[:k])

    nums3 = [1]
    print("Test 3 expected: k=1, nums[:1]=[1]")
    k = remove_duplicates(nums3)
    print("got:           ", k, nums3[:k])
