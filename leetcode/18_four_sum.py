"""
LeetCode 18. 4Sum  (Medium)
===========================

Given an integer array `nums` and an integer `target`, return all
unique quadruplets [nums[a], nums[b], nums[c], nums[d]] such that:
    - 0 <= a, b, c, d < len(nums)
    - a, b, c, d are all distinct
    - nums[a] + nums[b] + nums[c] + nums[d] == target

You may return the answer in any order.

Constraints:
    - 1 <= len(nums) <= 200
    - -10^9 <= nums[i] <= 10^9
    - -10^9 <= target <= 10^9

Function signature:
    four_sum(nums: list[int], target: int) -> list[list[int]]

Hint: sort first, then use two nested loops + two-pointer inner scan.
      Skip duplicate values to avoid duplicate quadruplets.
"""
from typing import List


def four_sum(nums: List[int], target: int) -> List[List[int]]:
    nums.sort()
    n = len(nums)
    res = []
    for q1 in range(n - 3):
        if q1 > 0 and nums[q1] == nums[q1 - 1]:
            continue
        if nums[q1] + nums[q1+1] + nums[q1+2] + nums[q1+3] > target:
            break
        if nums[q1] + nums[n-3] + nums[n-2] + nums[n-1] < target:
            continue
        for q2 in range(q1 + 1, n - 2):
            if q2 > q1 + 1 and nums[q2] == nums[q2 - 1]:
                continue
            if nums[q1] + nums[q2] + nums[q2+1] + nums[q2+2] > target:
                break
            if nums[q1] + nums[q2] + nums[n-2] + nums[n-1] < target:
                continue
            q3, q4 = q2 + 1, n - 1
            while q3 < q4:
                total = nums[q1] + nums[q2] + nums[q3] + nums[q4]
                if total == target:
                    res.append([nums[q1], nums[q2], nums[q3], nums[q4]])
                    while q3 + 1 < q4 and nums[q3] == nums[q3 + 1]:
                        q3 += 1
                    while q3 < q4 - 1 and nums[q4] == nums[q4 - 1]:
                        q4 -= 1
                    q3 += 1
                    q4 -= 1
                elif total < target:
                    q3 += 1
                else:
                    q4 -= 1
    return res


from collections import defaultdict(set)


def four_sum(nums: List[int], target: int) -> List[List[int]]:
    # so no duplicate indicides, no duplicate values? 
    look = 

# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: [[-2,-1,1,2],[-2,0,0,2],[-1,0,0,1]]")
    # print("got:           ", four_sum([1,0,-1,0,-2,2], 0))

    print("Test 2 expected: [[2,2,2,2]]")
    # print("got:           ", four_sum([2,2,2,2,2], 8))

    print("Test 3 expected: []  (no valid quadruplet)")
    # print("got:           ", four_sum([1,2,3,4], 100))
