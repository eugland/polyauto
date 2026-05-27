"""
LeetCode 37. Sudoku Solver  (Hard)
====================================

Write a program to solve a Sudoku puzzle by filling in the empty cells.  The
solution is guaranteed to be unique.  Modify the board in-place.

Rules:
  1. Each row must contain digits 1-9 without repetition.
  2. Each column must contain digits 1-9 without repetition.
  3. Each of the nine 3x3 sub-boxes must contain digits 1-9 without repetition.

Constraints:
    - board.length == 9, board[i].length == 9
    - board[i][j] is a digit or '.'.
    - The input board has exactly one solution.

Function signature:
    solve_sudoku(board: list[list[str]]) -> None  (modify in place)

Hint: backtracking — for each empty cell try digits 1-9, prune with row/col/box sets.
"""


def solve_sudoku(board: list) -> None:
    rows = [set() for _ in range(9)]
    cols = [set() for _ in range(9)]
    boxes = [set() for _ in range(9)]
    empty = []

    for r in range(9):
        for c in range(9):
            val = board[r][c]
            if val == '.':
                empty.append((r, c))
            else:
                box = (r // 3) * 3 + (c // 3)
                rows[r].add(val)
                cols[c].add(val)
                boxes[box].add(val)

    def backtrack(idx: int) -> bool:
        if idx == len(empty):
            return True
        r, c = empty[idx]
        box = (r // 3) * 3 + (c // 3)
        for d in "123456789":
            if d not in rows[r] and d not in cols[c] and d not in boxes[box]:
                board[r][c] = d
                rows[r].add(d)
                cols[c].add(d)
                boxes[box].add(d)
                if backtrack(idx + 1):
                    return True
                board[r][c] = '.'
                rows[r].discard(d)
                cols[c].discard(d)
                boxes[box].discard(d)
        return False

    backtrack(0)


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    board = [
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
    solve_sudoku(board)
    expected = [
        ["5","3","4","6","7","8","9","1","2"],
        ["6","7","2","1","9","5","3","4","8"],
        ["1","9","8","3","4","2","5","6","7"],
        ["8","5","9","7","6","1","4","2","3"],
        ["4","2","6","8","5","3","7","9","1"],
        ["7","1","3","9","2","4","8","5","6"],
        ["9","6","1","5","3","7","2","8","4"],
        ["2","8","7","4","1","9","6","3","5"],
        ["3","4","5","2","8","6","1","7","9"],
    ]
    print("Solved correctly:", board == expected)
    for row in board:
        print(row)
