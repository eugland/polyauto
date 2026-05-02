"""
LeetCode 2. Add Two Numbers  (Medium)
=====================================

You are given two non-empty linked lists representing two non-negative
integers. The digits are stored in REVERSE order, and each node contains
a single digit. Add the two numbers and return the sum as a linked list.

You may assume the two numbers do not contain any leading zero, except
the number 0 itself.

Example:
    Input:  l1 = 2 -> 4 -> 3      (represents 342)
            l2 = 5 -> 6 -> 4      (represents 465)
    Output:        7 -> 0 -> 8    (represents 807)

Constraints:
    - The number of nodes in each list is in [1, 100].
    - 0 <= Node.val <= 9
    - It is guaranteed the list represents a number with no leading zeros.

Function signature:
    add_two_numbers(l1: ListNode | None, l2: ListNode | None) -> ListNode | None
"""
from typing import Optional


class ListNode:
    def __init__(self, val: int = 0, next: "Optional[ListNode]" = None):
        self.val = val
        self.next = next


def add_two_numbers(
    l1: Optional[ListNode], l2: Optional[ListNode]
) -> Optional[ListNode]:
    dummy = ListNode(0)
    cur = dummy
    carry = 0
    left = l1
    right = l2

    while left and right:
        cur_num = left.val + right.val + carry
        # print(cur_num)
        cur.next = ListNode(cur_num % 10)
        carry = cur_num // 10
        cur = cur.next
        left = left.next
        right = right.next

    while left:
        cur_num = left.val + carry
        cur.next = ListNode(cur_num % 10)
        carry = cur_num // 10
        cur = cur.next
        left = left.next

    while right:
        cur_num = right.val + carry
        cur.next = ListNode(cur_num % 10)
        carry = cur_num // 10
        cur = cur.next
        right = right.next
    if carry:
        cur.next = ListNode(carry)

    return dummy.next

def add_two_numbers_inplace(
    l1: Optional[ListNode], l2: Optional[ListNode]
) -> Optional[ListNode]:
    carry = 0
    left = l1
    right = l2


    while left and right:
        left = left.next
        right = right.next

    if not left:
        left, right = l2, l1
    else:
        left, right = l1, l2
    if not left:
        return None
    root = left
    prev = root
    while left and right:
        cur_num = left.val + right.val + carry
        left.val = cur_num % 10
        carry = cur_num // 10
        prev = left
        left = left.next
        right = right.next

    while left:
        cur_num = left.val + carry
        carry = cur_num // 10
        left.val = cur_num % 10
        prev = left
        left = left.next

    if carry:
        prev.next = ListNode(carry)
    return root






# --------------------------- Helpers --------------------------------
def from_list(values: list[int]) -> Optional[ListNode]:
    dummy = ListNode()
    cur = dummy
    for v in values:
        cur.next = ListNode(v)
        cur = cur.next
    return dummy.next


def to_list(node: Optional[ListNode]) -> list[int]:
    out: list[int] = []
    while node:
        out.append(node.val)
        node = node.next
    return out


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    # Test 1: 342 + 465 = 807
    print("Test 1 expected: [7, 0, 8]")
    print("got:           ",
          to_list(add_two_numbers_inplace(from_list([2, 4, 3]), from_list([5, 6, 4]))))

    # Test 2: 0 + 0 = 0
    print("Test 2 expected: [0]")
    print("got:           ",
          to_list(add_two_numbers_inplace(from_list([0]), from_list([0]))))

    # Test 3: 9999999 + 9999 = 10009998 (carry propagation)
    print("Test 3 expected: [8, 9, 9, 9, 0, 0, 0, 1]")
    print("got:           ", to_list(add_two_numbers_inplace(
        from_list([9, 9, 9, 9, 9, 9, 9]), from_list([9, 9, 9, 9]))))

    # Test 4: different lengths
    print("Test 4 expected: [9, 9]    (90 + 9 = 99)")
    print("got:           ",
          to_list(add_two_numbers_inplace(from_list([0, 9]), from_list([9]))))
