"""
LeetCode 24. Swap Nodes in Pairs  (Medium)
===========================================

Given a linked list, swap every two adjacent nodes and return its head.
You must solve the problem without modifying the values in the list's
nodes (i.e., only nodes themselves may be changed).

Example:
    Input:  1 -> 2 -> 3 -> 4
    Output: 2 -> 1 -> 4 -> 3

Constraints:
    - The number of nodes in the list is in [0, 100].
    - 0 <= Node.val <= 100

Function signature:
    swap_pairs(head: Optional[ListNode]) -> Optional[ListNode]

Hint: use a dummy head; keep a pointer `prev` and advance by pairs,
      re-wiring next pointers for each pair before moving forward.
      Or solve recursively in a few lines.
"""
from typing import Optional


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


def swap_pairs(head: Optional[ListNode]) -> Optional[ListNode]:
    raise NotImplementedError("Implement swap_pairs")


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
    print("Test 1 expected: [2, 1, 4, 3]")
    # print("got:           ", to_list(swap_pairs(from_list([1,2,3,4]))))

    print("Test 2 expected: []  (empty list)")
    # print("got:           ", to_list(swap_pairs(None)))

    print("Test 3 expected: [1]  (single node, unchanged)")
    # print("got:           ", to_list(swap_pairs(from_list([1]))))

    print("Test 4 expected: [2, 1, 3]  (odd length)")
    # print("got:           ", to_list(swap_pairs(from_list([1,2,3]))))
