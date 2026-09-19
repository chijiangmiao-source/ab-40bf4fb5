"""Pure planning core: topology validation, safety simulation, backtracking search.

No database or web framework code lives here so the search can be unit tested
in isolation.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from . import search

DELIVER = "DELIVER"
DROP = "DROP"

MIN_SWITCHES = 1
MAX_SWITCHES = 60
MAX_UPDATES = 22


class PlanError(ValueError):
    """Validation failure carrying a stable machine-readable error code."""

    def __init__(self, code: str, message: str, http_status: int = 400,
                 details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.details = details or {}


def validate_topology(payload: Any) -> dict[str, Any]:
    """Validate a create-plan request body and return a normalized topology.

    Normalized form::

        {
          "switches": ["sw1", ...],          # sorted by raw UTF-8 bytes
          "entries":  ["ingress-a", ...],     # sorted by raw UTF-8 bytes
          "old_next": {"sw1": "DELIVER", ...},
          "new_next": {"sw1": "sw2", ...},
        }
    """
    if not isinstance(payload, dict):
        raise PlanError("invalid_body", "request body must be a JSON object", 400)

    switches_raw = payload.get("switches")
    entries_raw = payload.get("ingresses")
    old_raw = payload.get("old_next")
    new_raw = payload.get("new_next")
    if entries_raw is None:
        entries_raw = payload.get("entries")

    if not isinstance(switches_raw, list) or not switches_raw:
        raise PlanError("invalid_switches",
                        "'switches' must be a non-empty array", 400)
    if not isinstance(entries_raw, list) or not entries_raw:
        raise PlanError("invalid_ingresses",
                        "'ingresses' must be a non-empty array", 400)
    if not isinstance(old_raw, dict) or not isinstance(new_raw, dict):
        raise PlanError("invalid_next_hop_table",
                        "'old_next' and 'new_next' must be objects", 400)

    switches: list[str] = []
    seen: set[str] = set()
    for item in switches_raw:
        if not isinstance(item, str) or not item:
            raise PlanError("invalid_switch_id",
                            "every switch id must be a non-empty string", 400)
        if len(item) > 255:
            raise PlanError("invalid_switch_id",
                            "switch ids must be at most 255 UTF-8 characters",
                            400)
        if item in (DELIVER, DROP):
            raise PlanError("reserved_switch_id",
                            f"'{item}' is a reserved next hop", 400)
        if item in seen:
            raise PlanError("duplicate_switch_id",
                            f"duplicate switch id {item!r}", 409,
                            {"switch_id": item})
        seen.add(item)
        switches.append(item)

    n = len(switches)
    if not MIN_SWITCHES <= n <= MAX_SWITCHES:
        raise PlanError("topology_out_of_bounds",
                        f"switch count must be between {MIN_SWITCHES} and "
                        f"{MAX_SWITCHES}", 400, {"count": n})

    switch_set = set(switches)
    entries: list[str] = []
    seen_entries: set[str] = set()
    for item in entries_raw:
        if not isinstance(item, str) or not item:
            raise PlanError("invalid_ingress",
                            "every ingress id must be a non-empty string", 400)
        if item not in switch_set:
            raise PlanError("unknown_ingress",
                            f"ingress {item!r} is not a declared switch", 400,
                            {"ingress": item})
        if item in seen_entries:
            raise PlanError("duplicate_ingress",
                            f"duplicate ingress {item!r}", 400,
                            {"ingress": item})
        seen_entries.add(item)
        entries.append(item)

    def _normalize_table(raw: dict[str, Any], field: str) -> dict[str, str]:
        table: dict[str, str] = {}
        for sw in switches:
            if sw not in raw:
                raise PlanError("missing_next_hop",
                                f"{field} is missing an entry for switch "
                                f"{sw!r}", 400,
                                {"switch_id": sw, "table": field})
            hop = raw[sw]
            if not isinstance(hop, str) or not hop:
                raise PlanError("invalid_next_hop",
                                f"{field}[{sw!r}] must be a non-empty string",
                                400, {"switch_id": sw, "table": field})
            if hop not in switch_set and hop not in (DELIVER, DROP):
                raise PlanError("unknown_next_hop",
                                f"{field}[{sw!r}] points to undeclared switch "
                                f"{hop!r}", 400,
                                {"switch_id": sw, "next_hop": hop,
                                 "table": field})
            table[sw] = hop
        for extra in raw.keys() - switch_set:
            raise PlanError("unknown_switch_reference",
                            f"{field} references undeclared switch {extra!r}",
                            400, {"switch_id": extra, "table": field})
        return table

    old_next = _normalize_table(old_raw, "old_next")
    new_next = _normalize_table(new_raw, "new_next")

    # Raw UTF-8 byte ordering, not Python's Unicode code-point ordering.
    switches.sort(key=lambda s: s.encode("utf-8"))
    entries.sort(key=lambda s: s.encode("utf-8"))

    return {
        "switches": switches,
        "entries": entries,
        "old_next": old_next,
        "new_next": new_next,
    }


def changed_switches(topo: dict[str, Any]) -> list[str]:
    """Switches whose old and new next hops differ, in raw UTF-8 byte order."""
    changed = [s for s in topo["switches"]
               if topo["old_next"][s] != topo["new_next"][s]]
    changed.sort(key=lambda s: s.encode("utf-8"))
    return changed


def _compile(topo: dict[str, Any]) -> dict[str, Any]:
    """Compile string-keyed topology into integer arrays for fast simulation."""
    index = {s: i for i, s in enumerate(topo["switches"])}

    def hop_id(target: str) -> int:
        if target == DELIVER:
            return -1
        if target == DROP:
            return -2
        return index[target]

    old_hops = [hop_id(topo["old_next"][s]) for s in topo["switches"]]
    new_hops = [hop_id(topo["new_next"][s]) for s in topo["switches"]]
    entry_ids = [index[e] for e in topo["entries"]]
    return {"old": old_hops, "new": new_hops, "entries": entry_ids,
            "n": len(topo["switches"])}


def _safe_mask(compiled: dict[str, Any], updated_mask: int) -> bool:
    """Safety of the mixed table for the given bitmask of updated switches.

    From each ingress, follow current next hops. Every walk must reach
    DELIVER within n steps (n = number of switches). DROP, a dangling
    reference, a repeated node (loop) or exceeding the step budget fail.
    """
    n = len(compiled["old"])
    old_hops, new_hops, entries = (compiled["old"], compiled["new"],
                                   compiled["entries"])
    for start in entries:
        seen = 0
        node = start
        steps = 0
        while True:
            if node == -1:  # DELIVER
                break
            if node == -2:  # DROP
                return False
            bit = 1 << node
            if seen & bit:  # repeated node: forwarding loop
                return False
            if steps >= n:  # walked n switches without the egress
                return False
            seen |= bit
            hops = new_hops if (updated_mask & bit) else old_hops
            node = hops[node]
            steps += 1
    return True


def is_safe_state(topo: dict[str, Any], updated: set[str]) -> bool:
    """Public set-based wrapper around the safety simulator."""
    compiled = _compile(topo)
    index = {s: i for i, s in enumerate(topo["switches"])}
    mask = 0
    for sw in updated:
        mask |= 1 << index[sw]
    return _safe_mask(compiled, mask)


def find_lexicographically_minimal_order(topo: dict[str, Any]) -> list[str] | None:
    """Exhaustive backtracking search for the raw-UTF-8-byte lexicographically
    smallest complete update permutation whose every prefix yields a safe
    mixed forwarding table.

    Returns ``None`` (proven impossible) only when the full search space is
    exhausted without a complete safe permutation. This never commits to a
    "smallest currently safe" switch without backtracking, and never treats a
    batch of unsafe flips as atomic.
    """
    changed = changed_switches(topo)
    if not changed:
        return []
    compiled = _compile(topo)

    index = {s: i for i, s in enumerate(topo["switches"])}
    changed_bits = [1 << index[s] for s in changed]

    def safe_for_changed_mask(cmask: int) -> bool:
        updated_mask = 0
        for i, bit in enumerate(changed_bits):
            if cmask & (1 << i):
                updated_mask |= bit
        return _safe_mask(compiled, updated_mask)

    indices = search.lex_smallest_safe_order(
        len(changed), safe_for_changed_mask)
    if indices is None:
        return None
    return [changed[i] for i in indices]


def canonical_input(topo: dict[str, Any]) -> bytes:
    """Canonical JSON encoding of the normalized planning input."""
    canonical = {
        "switches": topo["switches"],
        "ingresses": topo["entries"],
        "old_next": {s: topo["old_next"][s] for s in topo["switches"]},
        "new_next": {s: topo["new_next"][s] for s in topo["switches"]},
    }
    return json.dumps(
        canonical, ensure_ascii=False, sort_keys=False,
        separators=(",", ":")).encode("utf-8")


def compute_plan_digest(canonical: bytes, order: list[str]) -> str:
    """Stable digest over normalized topology, ingresses and permutation.

    SHA-256 of the canonical input followed by a length-prefixed canonical
    encoding of the update permutation.
    """
    order_json = json.dumps(
        order, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    h = hashlib.sha256()
    h.update(b"switch-migration-plan/v1\n")
    h.update(str(len(canonical)).encode("ascii"))
    h.update(b":")
    h.update(canonical)
    h.update(b"\n")
    h.update(str(len(order_json)).encode("ascii"))
    h.update(b":")
    h.update(order_json)
    return h.hexdigest()
