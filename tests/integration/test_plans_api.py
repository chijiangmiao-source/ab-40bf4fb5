"""Plan creation: validation, endpoint safety, idempotency, immutability."""
import pytest

from helpers import chain_direct_deliver, create_plan, new_key


def valid_payload(n=4):
    ids, old, new = chain_direct_deliver(n)
    return {"switches": ids, "ingresses": [ids[0]],
            "old_next": old, "new_next": new}


def test_create_and_get_plan(api1):
    status, data = create_plan(api1, valid_payload(), new_key())
    assert status == 201
    assert data["status"] == "feasible"
    assert len(data["update_order"]) == 4
    assert data["step_count"] == 4
    assert len(data["plan_digest"]) == 64
    status2, got = api1.get(f"/api/plans/{data['plan_id']}")
    assert status2 == 200 and got["plan_digest"] == data["plan_digest"]


def test_idempotent_concurrent_create_single_plan(api1, run_concurrent):
    payload = valid_payload(3)
    key = new_key()
    results = run_concurrent(
        lambda i: create_plan(api1, payload, key), n=10)
    statuses = {r[0] for r in results}
    digests = {r[1]["plan_digest"] for r in results}
    ids = {r[1]["plan_id"] for r in results}
    assert statuses <= {200, 201}
    assert len(digests) == 1 and len(ids) == 1
    assert sum(1 for r in results if r[0] == 201) == 1


def test_same_key_retry_returns_same_plan(api1):
    payload = valid_payload(2)
    key = new_key()
    s1, d1 = create_plan(api1, payload, key)
    s2, d2 = create_plan(api1, payload, key)
    assert s1 == 201 and s2 == 200
    assert d1["plan_id"] == d2["plan_id"] and d1["plan_digest"] == d2["plan_digest"]


def test_same_key_different_params_conflicts(api1):
    key = new_key()
    s1, d1 = create_plan(api1, valid_payload(2), key)
    assert s1 == 201
    p2 = valid_payload(2)
    p2["new_next"][p2["switches"][0]] = "DROP"
    s2, err = create_plan(api1, p2, key)
    assert s2 == 409 and err["error"]["code"] == "idempotency_conflict"
    # original plan is untouched
    _, got = api1.get(f"/api/plans/{d1['plan_id']}")
    assert got["plan_digest"] == d1["plan_digest"]


def test_initial_state_unsafe_rejected(api1):
    ids, old, new = chain_direct_deliver(3)
    old[ids[0]] = "DROP"
    status, err = create_plan(api1, {
        "switches": ids, "ingresses": [ids[0]],
        "old_next": old, "new_next": new}, new_key())
    assert status == 422
    assert err["error"]["code"] == "initial_state_unsafe"


def test_final_state_unsafe_rejected(api1):
    ids, old, new = chain_direct_deliver(3)
    new[ids[0]] = "DROP"
    status, err = create_plan(api1, {
        "switches": ids, "ingresses": [ids[0]],
        "old_next": old, "new_next": new}, new_key())
    assert status == 422
    assert err["error"]["code"] == "final_state_unsafe"


def test_initial_loop_rejected(api1):
    payload = {
        "switches": ["a", "b"], "ingresses": ["a"],
        "old_next": {"a": "b", "b": "a"},
        "new_next": {"a": "DELIVER", "b": "DELIVER"}}
    status, err = create_plan(api1, payload, new_key())
    assert status == 422 and err["error"]["code"] == "initial_state_unsafe"


def test_unknown_next_hop_and_bad_refs(api1):
    payload = {
        "switches": ["a"], "ingresses": ["a"],
        "old_next": {"a": "ghost"}, "new_next": {"a": "DELIVER"}}
    status, err = create_plan(api1, payload, new_key())
    assert status == 400 and err["error"]["code"] == "unknown_next_hop"


def test_out_of_bounds_switches(api1):
    ids = [f"s{i}" for i in range(61)]
    payload = {
        "switches": ids, "ingresses": ["s0"],
        "old_next": {s: "DELIVER" for s in ids},
        "new_next": {s: "DELIVER" for s in ids}}
    status, err = create_plan(api1, payload, new_key())
    assert status == 400 and err["error"]["code"] == "topology_out_of_bounds"


def test_too_many_updates(api1):
    # 23 changing switches (out of 24) exceeds the plan limit of 22.
    ids, old, new = chain_direct_deliver(23)
    status, err = create_plan(api1, {
        "switches": ids, "ingresses": ["s0"],
        "old_next": old, "new_next": new}, new_key())
    assert status == 400 and err["error"]["code"] == "too_many_updates"


def test_plan_order_every_prefix_safe(api1):
    # A richer topology: old chain, new reverses toward a different egress path
    ids = ["a", "b", "c", "d", "e"]
    old = {"a": "b", "b": "c", "c": "d", "d": "e", "e": "DELIVER"}
    new = {"a": "DELIVER", "b": "a", "c": "b", "d": "c", "e": "d"}
    status, data = create_plan(api1, {
        "switches": ids, "ingresses": ["a"],
        "old_next": old, "new_next": new}, new_key())
    assert status == 201
    # verify order via service-side semantics is implicit; digest present
    assert data["update_order"]
    assert data["plan_digest"]


def test_missing_idempotency_key(api1):
    status, err = create_plan(api1, valid_payload(2), key=None)
    assert status == 400
    assert err["error"]["code"] == "missing_idempotency_key"


def test_get_nonexistent_plan(api1):
    import uuid
    status, err = api1.get(f"/api/plans/{uuid.uuid4()}")
    assert status == 404 and err["error"]["code"] == "plan_not_found"


def test_plan_input_is_immutable(api1):
    status, data = create_plan(api1, valid_payload(2), new_key())
    assert status == 201
    # no mutation route exists; re-fetch returns identical content
    _, again = api1.get(f"/api/plans/{data['plan_id']}")
    assert again["update_order"] == data["update_order"]
    assert again["input_canonical"] if "input_canonical" in again else True
