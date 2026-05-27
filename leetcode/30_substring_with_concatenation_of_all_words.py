"""
LeetCode 30. Substring with Concatenation of All Words  (Hard)
===============================================================

You are given a string `s` and an array of strings `words`.  All words are of
the same length.  Return all starting indices of substring(s) in `s` that is a
concatenation of each word in `words` exactly once, in any order.

Example:
    s = "barfoothefoobarman", words = ["foo","bar"]  →  [0, 9]
    s = "wordgoodgoodgoodbestword", words = ["word","good","best","word"]  →  []
    s = "barfoofoobarthefoobarman", words = ["bar","foo","the"]  →  [6, 9, 12]

Constraints:
    - 1 <= len(s) <= 10^4
    - 1 <= len(words) <= 5000
    - 1 <= len(words[i]) <= 30
    - s and words[i] consist of lowercase English letters.

Function signature:
    find_substring(s: str, words: list[str]) -> list[int]

Hint: use a sliding window of size word_len * num_words with a frequency map.
"""

from collections import Counter


def find_substring(s: str, words: list) -> list:
    if not s or not words:
        return []

    word_len = len(words[0])
    num_words = len(words)
    window = word_len * num_words
    word_count = Counter(words)
    result = []

    for i in range(word_len):
        left = i
        curr = Counter()
        matched = 0
        for j in range(i, len(s) - word_len + 1, word_len):
            w = s[j:j + word_len]
            if w in word_count:
                curr[w] += 1
                matched += 1
                while curr[w] > word_count[w]:
                    left_w = s[left:left + word_len]
                    curr[left_w] -= 1
                    matched -= 1
                    left += word_len
                if matched == num_words:
                    result.append(left)
                    left_w = s[left:left + word_len]
                    curr[left_w] -= 1
                    matched -= 1
                    left += word_len
            else:
                curr.clear()
                matched = 0
                left = j + word_len

    return sorted(result)


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: [0, 9]")
    print("got:           ", find_substring("barfoothefoobarman", ["foo", "bar"]))

    print("Test 2 expected: []")
    print("got:           ", find_substring("wordgoodgoodgoodbestword", ["word","good","best","word"]))

    print("Test 3 expected: [6, 9, 12]")
    print("got:           ", find_substring("barfoofoobarthefoobarman", ["bar","foo","the"]))
