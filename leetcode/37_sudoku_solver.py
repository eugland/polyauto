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
    pass


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
