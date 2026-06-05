"""
LeetCode 21. Merge Two Sorted Lists  (Easy)
============================================

You are given the heads of two sorted linked lists `list1` and `list2`.
Merge the two lists into one sorted list. The list should be made by
splicing together the nodes of the first two lists. Return the head of
the merged linked list.

Example:
    Input:  list1 = 1->2->4,  list2 = 1->3->4
    Output: 1->1->2->3->4->4

Constraints:
    - The number of nodes in each list is in [0, 50].
    - -100 <= Node.val <= 100
    - Both lists are sorted in non-decreasing order.

Function signature:
    merge_two_lists(list1: Optional[ListNode], list2: Optional[ListNode]) -> Optional[ListNode]

Hint: use a dummy head and iteratively attach the smaller node; or
      solve recursively in ~3 lines.
"""
from typing import Optional


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


def merge_two_lists(list1: Optional[ListNode], list2: Optional[ListNode]) -> Optional[ListNode]:
    dummy = ListNode(0)
    cur = dummy
    left = list1
    right = list2
    while left and right: 
        if left.val <= right.val:
            cur.next = left
            cur = cur.next
            left = left.next
        else:
            cur.next = right
            cur = cur.next
            right = right.next
        cur.next = None
    if left:
        cur.next = left
    elif right:
        cur.next = right

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
    print("Test 1 expected: [1, 1, 2, 3, 4, 4]")
    print("got:           ", to_list(merge_two_lists(from_list([1,2,4]), from_list([1,3,4]))))

    print("Test 2 expected: []  (both empty)")
    print("got:           ", to_list(merge_two_lists(None, None)))

    print("Test 3 expected: [0]  (empty + [0])")
    print("got:           ", to_list(merge_two_lists(None, from_list([0]))))
