"""
LeetCode 3. Longest Substring Without Repeating Characters  (Medium)
====================================================================

Given a string `s`, find the length of the longest SUBSTRING (contiguous)
without repeating characters.

Constraints:
    - 0 <= len(s) <= 5 * 10^4
    - `s` consists of English letters, digits, symbols, and spaces.

Follow-up: Can you do it in a single pass, O(n) time?

Function signature:
    length_of_longest_substring(s: str) -> int
"""


def length_of_longest_substring(s: str) -> int:
    # TODO: your implementation
    pass


def longest_substring_without_repeating(s: str) -> str:
    """Return any longest substring of `s` with no repeating characters.

    Used by the test harness to verify the actual segment your sliding
    window picked, not just its length. If there are multiple equally
    long answers, returning any one of them is fine.
    """
    # TODO: your implementation
    pass


# ---------------------------- Test cases ----------------------------
def _check(s: str, expected_len: int, valid_segments: set[str]) -> None:
    """Assert both the reported length and the picked segment.

    `valid_segments` is the set of acceptable winners (some inputs have
    multiple equally-long answers — any of them is correct, as long as
    it actually appears in `s` and has no repeats).
    """
    got_len = length_of_longest_substring(s)
    got_seg = longest_substring_without_repeating(s)

    assert got_len == expected_len, (
        f"length mismatch for {s!r}: expected {expected_len}, got {got_len}"
    )
    assert len(got_seg) == expected_len, (
        f"segment length mismatch for {s!r}: segment {got_seg!r} "
        f"has length {len(got_seg)}, expected {expected_len}"
    )
    assert got_seg in s, (
        f"segment {got_seg!r} is not a substring of {s!r}"
    )
    assert len(set(got_seg)) == len(got_seg), (
        f"segment {got_seg!r} contains repeating characters"
    )
    assert got_seg in valid_segments, (
        f"segment {got_seg!r} not in accepted answers {valid_segments} for {s!r}"
    )
    print(f"OK  s={s!r:25}  len={got_len}  seg={got_seg!r}")


if __name__ == "__main__":
    # Classic LeetCode examples
    _check("abcabcbb", 3, {"abc"})
    _check("bbbbb",    1, {"b"})
    _check("pwwkew",   3, {"wke", "kew"})    # either tie-break is valid
    _check("",         0, {""})
    _check("abcdeabc", 5, {"abcde"})

    # Single character
    _check("a", 1, {"a"})

    # All unique — whole string is the answer
    _check("abcdef", 6, {"abcdef"})

    # Repeat right at the end
    _check("abcdea", 5, {"abcde", "bcdea"})  # both windows are valid length-5 winners

    # Repeats with mixed symbols / digits / spaces
    _check("a1b2c3a", 6, {"a1b2c3", "1b2c3a"})
    _check("dvdf",    3, {"vdf"})            # tricky: must drop only up to first 'd'
    _check("tmmzuxt", 5, {"mzuxt"})          # tricky: window jumps over earlier 'm'

    # Spaces count as characters — repeated spaces cap the window
    _check("a b c a", 3, {"a b", " b ", "b c", " c ", "c a"})

    # Long run with one repeat in the middle
    _check("anviaj", 5, {"nviaj"})

    print("\nAll assertions passed.")
