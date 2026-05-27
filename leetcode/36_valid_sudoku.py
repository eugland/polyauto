"""
LeetCode 36. Valid Sudoku  (Medium)
=====================================

Determine if a 9x9 Sudoku board is valid.  Only the filled cells need to be
validated according to these rules:
  1. Each row must contain digits 1-9 without repetition.
  2. Each column must contain digits 1-9 without repetition.
  3. Each of the nine 3x3 sub-boxes must contain digits 1-9 without repetition.

Note: a valid board is not necessarily solvable.

Example:
    (see LeetCode problem for the board diagrams)

Constraints:
    - board.length == 9, board[i].length == 9
    - board[i][j] is a digit 1-9 or '.'.

Function signature:
    is_valid_sudoku(board: list[list[str]]) -> bool
"""


def is_valid_sudoku(board: list) -> bool:
    rows = [set() for _ in range(9)]
    cols = [set() for _ in range(9)]
    boxes = [set() for _ in range(9)]

    for r in range(9):
        for c in range(9):
            val = board[r][c]
            if val == '.':
                continue
            box_idx = (r // 3) * 3 + (c // 3)
            if val in rows[r] or val in cols[c] or val in boxes[box_idx]:
                return False
            rows[r].add(val)
            cols[c].add(val)
            boxes[box_idx].add(val)

    return True


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    valid_board = [
        ["5","3",".",".","7",".",".",".","."],
        ["6",".",".","1","9","5",".",".","."],
        [".","9","8",".",".",".",".","6","."],
        ["8",".",".",".","6",".",".",".","3"],
        ["4",".",".","8",".","3",".",".","1"],
        ["7",".",".",".","2",".",".",".","6"],
        [".","6",".",".",".",".","2","8","."],
        [".",".",".","4","1","9",".",".","5"],
        [".",".",".",".","8",".",".","7","9"],
    ]
    print("Test 1 expected: True")
    print("got:           ", is_valid_sudoku(valid_board))

    invalid_board = [
        ["8","3",".",".","7",".",".",".","."],
        ["6",".",".","1","9","5",".",".","."],
        [".","9","8",".",".",".",".","6","."],
        ["8",".",".",".","6",".",".",".","3"],
        ["4",".",".","8",".","3",".",".","1"],
        ["7",".",".",".","2",".",".",".","6"],
        [".","6",".",".",".",".","2","8","."],
        [".",".",".","4","1","9",".",".","5"],
        [".",".",".",".","8",".",".","7","9"],
    ]
    print("Test 2 expected: False  (8 repeated in first column)")
    print("got:           ", is_valid_sudoku(invalid_board))
