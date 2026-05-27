"""
LeetCode 25. Reverse Nodes in k-Group  (Hard)
==============================================

Given the head of a linked list, reverse the nodes of the list k at
a time, and return the modified list.

k is a positive integer and is less than or equal to the length of the
linked list. If the number of nodes is not a multiple of k then the
left-out nodes at the end should remain as is.

You may not alter the values in the list's nodes, only nodes themselves
may be changed.

Example:
    Input:  1->2->3->4->5,  k=2  →  2->1->4->3->5
    Input:  1->2->3->4->5,  k=3  →  3->2->1->4->5

Constraints:
    - The number of nodes in the list is n.
    - 1 <= k <= n <= 5000
    - 0 <= Node.val <= 1000

Function signature:
    reverse_k_group(head: Optional[ListNode], k: int) -> Optional[ListNode]

Hint: check whether k nodes remain; if yes, reverse that group and
      recurse on the tail. A dummy head simplifies pointer surgery.
"""
from typing import Optional


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


def reverse_k_group(head: Optional[ListNode], k: int) -> Optional[ListNode]:
    raise NotImplementedError("Implement reverse_k_group")


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
    print("Test 1 expected: [2, 1, 4, 3, 5]  (k=2)")
    # print("got:           ", to_list(reverse_k_group(from_list([1,2,3,4,5]), 2)))

    print("Test 2 expected: [3, 2, 1, 4, 5]  (k=3)")
    # print("got:           ", to_list(reverse_k_group(from_list([1,2,3,4,5]), 3)))

    print("Test 3 expected: [1, 2, 3]  (k > len, unchanged)")
    # print("got:           ", to_list(reverse_k_group(from_list([1,2,3]), 4)))
