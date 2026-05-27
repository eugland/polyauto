"""
LeetCode 31. Next Permutation  (Medium)
========================================

A permutation of an array of integers is an arrangement of its members into a
sequence or linear order.  The next permutation of an array is the next
lexicographically greater permutation.  If no such arrangement exists, the array
must be rearranged as the lowest possible order (sorted ascending).

The replacement must be in-place and use only constant extra memory.

Example:
    [1,2,3] → [1,3,2]
    [3,2,1] → [1,2,3]
    [1,1,5] → [1,5,1]

Constraints:
    - 1 <= len(nums) <= 100
    - 0 <= nums[i] <= 100

Function signature:
    next_permutation(nums: list[int]) -> None  (modify in place)

Hint: find the first decreasing element from the right, swap with the next
      larger element to its right, then reverse the suffix.
"""


def next_permutation(nums: list) -> None:
    n = len(nums)
    i = n - 2
    while i >= 0 and nums[i] >= nums[i + 1]:
        i -= 1

    if i >= 0:
        j = n - 1
        while nums[j] <= nums[i]:
            j -= 1
        nums[i], nums[j] = nums[j], nums[i]

    left, right = i + 1, n - 1
    while left < right:
        nums[left], nums[right] = nums[right], nums[left]
        left += 1
        right -= 1


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    a = [1, 2, 3]; next_permutation(a)
    print("Test 1 expected: [1, 3, 2]  got:", a)

    b = [3, 2, 1]; next_permutation(b)
    print("Test 2 expected: [1, 2, 3]  got:", b)

    c = [1, 1, 5]; next_permutation(c)
    print("Test 3 expected: [1, 5, 1]  got:", c)

    d = [1, 3, 2]; next_permutation(d)
    print("Test 4 expected: [2, 1, 3]  got:", d)
