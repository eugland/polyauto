"""
LeetCode 23. Merge k Sorted Lists  (Hard)
==========================================

You are given an array of `k` linked-list heads, each sorted in
ascending order. Merge all the linked lists into one sorted linked
list and return its head.

Example:
    Input:  [[1,4,5],[1,3,4],[2,6]]
    Output: 1->1->2->3->4->4->5->6

Constraints:
    - k == len(lists)
    - 0 <= k <= 10^4
    - 0 <= len(lists[i]) <= 500
    - -10^4 <= lists[i][j] <= 10^4
    - lists[i] is sorted in ascending order
    - The total number of nodes across all lists <= 10^4

Function signature:
    merge_k_lists(lists: list[Optional[ListNode]]) -> Optional[ListNode]

Hint: use a min-heap (heapq) — push (val, id, node) for each head,
      then repeatedly pop the minimum and push its successor.
      Alternatively, divide-and-conquer pair-wise merge in O(N log k).
"""
import heapq
from typing import List, Optional


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next
    
    def __lt__(self, other):
        return self.val < other.val


def merge_k_lists(lists: List[Optional[ListNode]]) -> Optional[ListNode]:
    arr = [(x.val, i, x) for i, x in enumerate(lists) if x]
    print(arr)
    heapq.heapify(arr)
    dummy = ListNode(0)
    cur = dummy
    while arr:
        val, i, node = heapq.heappop(arr)
        if node.next:
            heapq.heappush(arr, (node.next.val, i, node.next))
        # print(node.val)
        cur.next = node
        cur = cur.next
        cur.next = None
    return dummy.next


    




# ---------------------------- Helpers ----------------------------
def to_list(node):
    result = []
    while node:
        result.append(node.val)
        node = node.next
    return result

def from_list(vals):
    dummy = ListNode(0)
    cur = dummy
    for v in vals:
        cur.next = ListNode(v)
        cur = cur.next
    return dummy.next


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: [1, 1, 2, 3, 4, 4, 5, 6]")
    heads = [from_list([1,4,5]), from_list([1,3,4]), from_list([2,6])]
    print("got:           ", to_list(merge_k_lists(heads)))

    print("Test 2 expected: []  (empty input)")
    print("got:           ", to_list(merge_k_lists([])))

    print("Test 3 expected: []  (list of one empty list)")
    print("got:           ", to_list(merge_k_lists([None])))
