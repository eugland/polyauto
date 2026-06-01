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
    1 2 5 3 -> 1 5 2 3 -> 1 5 3 2
    3 1 5 3 -> 3 5 1 3 
Constraints:
    - 1 <= len(nums) <= 100
    - 0 <= nums[i] <= 100

Function signature:
    next_permutation(nums: list[int]) -> None  (modify in place)

Hint: find the first decreasing element from the right, swap with the next
      larger element to its right, then reverse the suffix.


go from the back 1, 2, find the first number that is smaller 
3 5 1 2, 0

"""


def next_permutation(nums: list) -> None:
    # find pivot first
    n = len(nums)
    j = n - 2
    # 1, 5, 4, 3
    
    while j >= 0 and nums[j] > nums[j+1]:
        j -= 1
    # j == 0 at 1 now 
    print(j)
    
    if j >=0: 
        i = n - 1
        while i > j and nums[i] < nums[j]:
            i -= 1
        nums[j], nums[i] = nums[i], nums[j]
    
    nums[j+1:] = nums[j+1:][::-1]

    







# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    a = [5, 1, 3, 1, 1]; next_permutation(a)
    print("Test a expected:   got:", a)

    a = [ 5,3,1]; next_permutation(a)
    print("Test b expected:   got:", a)
    
    a = [1, 2, 3]; next_permutation(a)
    print("Test 1 expected: [1, 3, 2]  got:", a)

    b = [3, 2, 1]; next_permutation(b)
    print("Test 2 expected: [1, 2, 3]  got:", b)

    c = [1, 1, 5]; next_permutation(c)
    print("Test 3 expected: [1, 5, 1]  got:", c)

    d = [1, 3, 2]; next_permutation(d)
    print("Test 4 expected: [2, 1, 3]  got:", d)
