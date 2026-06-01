"""
LeetCode 33. Search in Rotated Sorted Array  (Medium)
======================================================

There is an integer array `nums` sorted in ascending order (with distinct
values) which has been rotated at an unknown pivot.  Given `target`, return its
index or -1 if not in `nums`.  Must run in O(log n).

Example:
    nums = [4,5,6,7,0,1,2], target = 0  →  4
    nums = [4,5,6,7,0,1,2], target = 3  →  -1
    nums = [1],              target = 0  →  -1

Constraints:
    - 1 <= len(nums) <= 5000
    - All values of nums are unique.
    - -10^4 <= nums[i], target <= 10^4

Function signature:
    search(nums: list[int], target: int) -> int
"""


def search(nums: list, target: int) -> int:
    start, end = 0, len(nums) - 1
    while start <= end:
        mid = (start + end) // 2
        if nums[mid] == target:
            return mid

        # 2 3 4 5 1 # mid > left, pivot on the right 
        # 4 5 1 2 3 # mid < left, pivot on the left
        # 3 1, 
        # 0 -> 3, start =3 mid = 3, target = 1
        if nums[start] <= nums[mid]:
            # pivot on the right
            if nums[start] <= target <= nums[mid]:
                end = mid - 1
            else:
                start = mid + 1
        else: # nums[mid] < nums[start]
            # 4 5 1 2 3 # mid < left, pivot on the left
            if nums[mid] <= target <= nums[end]:
                start = mid + 1
            else:
                end = mid - 1
    return -1


        


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: 4")
    print("got:           ", search([4,5,6,7,0,1,2], 0))

    print("Test 2 expected: -1")
    print("got:           ", search([4,5,6,7,0,1,2], 3))

    print("Test 3 expected: -1")
    print("got:           ", search([1], 0))

    print("Test 4 expected: 1")
    print("got:           ", search([3,1], 1))
