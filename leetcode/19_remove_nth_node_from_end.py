"""
LeetCode 19. Remove Nth Node From End of List  (Medium)
=======================================================

Given the head of a linked list, remove the n-th node from the end of
the list and return its head.

Example:
    Input:  1 -> 2 -> 3 -> 4 -> 5,  n = 2
    Output: 1 -> 2 -> 3 -> 5

Constraints:
    - The number of nodes in the list is sz.
    - 1 <= sz <= 30
    - 0 <= Node.val <= 100
    - 1 <= n <= sz

Follow-up: Can you do it in one pass?

Function signature:
    remove_nth_from_end(head: Optional[ListNode], n: int) -> Optional[ListNode]

Hint: use a two-pointer (fast/slow) approach — advance fast n steps
      ahead, then move both until fast reaches the tail.
"""
from typing import Optional


class ListNode:
    def __init__(self, val=0, next=None):
        self.val = val
        self.next = next


def remove_nth_from_end(head: Optional[ListNode], n: int) -> Optional[ListNode]:
    dummy = ListNode(0)
    dummy.next = head
    slow = dummy
    fast = dummy 
    # if just 1 elelemt
    # d -> 0 -> null
    # s          f , 
    # s = d
    for i in range(n+1):
        fast = fast.next
    while fast: 
        fast = fast.next
        slow = slow.next
    
    if slow:
        if slow.next:
            slow.next = slow.next.next
        else: 
            slow.next = None
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
    print("Test 1 expected: [1, 2, 3, 5]  (remove 2nd from end)")
    # print("got:           ", to_list(remove_nth_from_end(from_list([1,2,3,4,5]), 2)))

    print("Test 2 expected: []  (single-element list, n=1)")
    # print("got:           ", to_list(remove_nth_from_end(from_list([1]), 1)))

    print("Test 3 expected: [1]  (remove head, n=2)")
    # print("got:           ", to_list(remove_nth_from_end(from_list([1,2]), 2)))
