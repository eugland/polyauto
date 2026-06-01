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
    n = len(s)
    m = len(words[0])
    bench = Counter(words)
    res = []
    for offset in range(m):
        acc = 0
        slow = offset
        fast = offset
        count = Counter()
        while fast < n:
            word = s[fast:fast+m]
            count[word] += 1
            fast += m
            acc += 1
            if acc == len(words):
                # print(count)
                if count == bench:
                    res.append(slow)
                acc -= 1
                word_removal = s[slow:slow+m]
                count[word_removal] -= 1
                slow += m
    return res
        


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    print("Test 1 expected: [0, 9]")
    print("got:           ", find_substring("barfoothefoobarman", ["foo", "bar"]))

    print("Test 2 expected: []")
    print("got:           ", find_substring("wordgoodgoodgoodbestword", ["word","good","best","word"]))

    print("Test 3 expected: [6, 9, 12]")
    print("got:           ", find_substring("barfoofoobarthefoobarman", ["bar","foo","the"]))

    print("Test 4 expected: ")
    print("got:           ", find_substring("lingmindraboofooowingdingbarrwingmonkeypoundcake", ["fooo","barr","wing","ding","wing"]))
