"""
LeetCode 35. Search Insert Position  (Easy)
============================================

Given a sorted array of distinct integers and a target value, return the index
if the target is found.  If not, return the index where it would be inserted in
order.  Must run in O(log n).

Example:
    [1,3,5,6], 5  →  2
    [1,3,5,6], 2  →  1
    [1,3,5,6], 7  →  4

Constraints:
    - 1 <= len(nums) <= 10^4
    - -10^4 <= nums[i] <= 10^4
    - All values in nums are distinct and sorted in ascending order.

Function signature:
    search_insert(nums: list[int], target: int) -> int
"""


def search_insert(nums: list, target: int) -> int:
    lo, hi = 0, len(nums)
    while lo < hi:
        mid = (lo + hi) // 2
        if nums[mid] < target:
            lo = mid + 1
        else:
            hi = mid
    return lo


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: 2")
    print("got:           ", search_insert([1,3,5,6], 5))

    print("Test 2 expected: 1")
    print("got:           ", search_insert([1,3,5,6], 2))

    print("Test 3 expected: 4")
    print("got:           ", search_insert([1,3,5,6], 7))

    print("Test 4 expected: 0")
    print("got:           ", search_insert([1,3,5,6], 0))
