"""Generic exhaustive backtracking search for update orders.

Separated from the forwarding-table semantics so the search strategy itself
(real backtracking, lexicographic minimality, proven-impossible) is testable
with any safety predicate. The state is a bitmask over the ordered candidate
list: bit ``i`` means candidate ``i`` has already been applied.
"""
from __future__ import annotations

from typing import Callable


def lex_smallest_safe_order(
    candidate_count: int,
    is_safe_mask: Callable[[int], bool],
) -> list[int] | None:
    """Return the lexicographically smallest index permutation whose every
    prefix mask satisfies ``is_safe_mask``.

    Candidate indices are tried in ascending order, so the first complete
    branch found is the lexicographically smallest permutation. Returns
    ``None`` only after the full search space is exhausted with no complete
    safe permutation (``proven_impossible``). Dead-end masks are memoized;
    the search never commits to the smallest currently-safe candidate
    without backtracking.
    """
    if candidate_count == 0:
        return []
    full = (1 << candidate_count) - 1
    if not is_safe_mask(0):
        return None

    # memo[mask] = smallest suffix completing mask, or None for a dead end.
    memo: dict[int, list[int] | None] = {full: []}

    def search(mask: int) -> list[int] | None:
        if mask in memo:
            return memo[mask]
        memo[mask] = None  # placeholder also short-cycles revisits
        for i in range(candidate_count):
            bit = 1 << i
            if mask & bit:
                continue
            candidate = mask | bit
            if not is_safe_mask(candidate):
                continue
            suffix = search(candidate)
            if suffix is not None:
                result = [i] + suffix
                memo[mask] = result
                return result
        return None

    return search(0)
