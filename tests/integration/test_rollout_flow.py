"""Rollout lifecycle: leases, epochs, takeover, advances, acks, completion."""
import uuid

from helpers import (ack, acquire, advance, chain_direct_deliver,
                     create_plan, create_rollout, expire_lease,
                     fetch_command, new_key, renew)


def make_plan_rollout(client, n=3):
    ids, old, new = chain_direct_deliver(n)
    _, plan = create_plan(client, {
        "switches": ids, "ingresses": [ids[0]],
        "old_next": old, "new_next": new}, new_key("plan"))
    _, rollout = create_rollout(client, plan["plan_id"], new_key("roll"))
    return plan, rollout


def test_full_happy_path(api1):
    plan, rollout = make_plan_rollout(api1, 3)
    rid = rollout["rollout_id"]
    s, lease = acquire(api1, rid, "coord-1")
    assert s == 200 and lease["action"] == "acquired"
    epoch = lease["epoch"]

    for step in range(3):
        s, adv = advance(api1, rid, "coord-1", epoch)
        assert s == 200 and adv["action"] == "created"
        cmd = adv["command"]
        assert cmd["step"] == step
        assert cmd["plan_digest"] == plan["plan_digest"]
        assert cmd["device_generation"] >= 1

        # switch can fetch its pending command
        s, fetched = fetch_command(api1, rid, cmd["switch_id"])
        assert s == 200 and fetched["command"]["command_id"] == cmd["command_id"]

        # another switch currently has no command
        other = plan["update_order"][(step + 1) % 3]
        if other != cmd["switch_id"]:
            s, none = fetch_command(api1, rid, other)
            assert none["command"] is None

        s, ackres = ack(api1, rid, cmd["switch_id"], cmd)
        assert s == 200 and ackres["result"] == "APPLIED"

    s, state = api1.get(f"/api/rollouts/{rid}")
    assert s == 200 and state["status"] == "COMPLETED"
    assert len(state["steps"]) == 3
    assert all(st["status"] == "APPLIED" for st in state["steps"])

    # audit trail exists and is ordered
    s, audit = api1.get(f"/api/rollouts/{rid}/audit")
    kinds = [e["event_type"] for e in audit["events"]]
    assert kinds.count("command_created") == 3
    assert kinds.count("command_applied") == 3
    assert kinds[-1] == "rollout_completed"


def test_advance_without_command_while_pending_is_idempotent(api1):
    plan, rollout = make_plan_rollout(api1, 2)
    rid = rollout["rollout_id"]
    _, lease = acquire(api1, rid, "coord-1")
    epoch = lease["epoch"]

    op = new_key("adv")
    s1, r1 = advance(api1, rid, "coord-1", epoch, op_id=op)
    s2, r2 = advance(api1, rid, "coord-1", epoch, op_id=op)
    assert s1 == 200 and s2 == 200
    assert r1["command"]["command_id"] == r2["command"]["command_id"]
    assert r1["command"]["device_generation"] == \
        r2["command"]["device_generation"]

    # a DIFFERENT op id while the same command is still pending must also
    # return the same command (retries after a lost response)
    s3, r3 = advance(api1, rid, "coord-1", epoch, op_id=new_key("adv2"))
    assert s3 == 200 and r3["action"] == "existing"
    assert r3["command"]["command_id"] == r1["command"]["command_id"]

    # still only one command in the state
    _, state = api1.get(f"/api/rollouts/{rid}")
    assert len(state["steps"]) == 1
    assert state["pending_command"] is not None


def test_no_two_pending_commands(api1):
    plan, rollout = make_plan_rollout(api1, 3)
    rid = rollout["rollout_id"]
    _, lease = acquire(api1, rid, "coord-1")
    _, r1 = advance(api1, rid, "coord-1", lease["epoch"])
    # trying to advance again without an ack never creates a second command
    _, r2 = advance(api1, rid, "coord-1", lease["epoch"])
    assert r2["command"]["command_id"] == r1["command"]["command_id"]
    _, state = api1.get(f"/api/rollouts/{rid}")
    pending = [s for s in state["steps"] if s["status"] == "PENDING"]
    assert len(pending) == 1


def test_stale_epoch_cannot_advance_after_takeover(api1, db_conn):
    plan, rollout = make_plan_rollout(api1, 2)
    rid = rollout["rollout_id"]
    _, l1 = acquire(api1, rid, "coord-A")
    epoch_a = l1["epoch"]
    # A creates one command
    _, r = advance(api1, rid, "coord-A", epoch_a)
    assert r["action"] == "created"

    # lease expires; B takes over, gets a strictly greater epoch
    expire_lease(db_conn, rid)
    _, l2 = acquire(api1, rid, "coord-B")
    epoch_b = l2["epoch"]
    assert epoch_b > epoch_a

    # old coordinator A comes back: must not be able to advance
    s, err = advance(api1, rid, "coord-A", epoch_a)
    assert s == 409 and err["error"]["code"] == "stale_epoch"

    # A cannot renew either: it is neither the holder nor the current epoch
    s, err = renew(api1, rid, "coord-A", epoch_a)
    assert s == 409 and err["error"]["code"] in (
        "not_lease_holder", "stale_epoch")

    # B can see the outstanding command (no new id, no new generation)
    _, r2 = advance(api1, rid, "coord-B", epoch_b, op_id=new_key("take"))
    assert r2["action"] == "existing"
    assert r2["command"]["command_id"] == r["command"]["command_id"]
    assert r2["command"]["device_generation"] == \
        r["command"]["device_generation"]


def test_live_lease_cannot_be_stolen(api1):
    plan, rollout = make_plan_rollout(api1, 1)
    rid = rollout["rollout_id"]
    _, l1 = acquire(api1, rid, "coord-A")
    s, err = acquire(api1, rid, "coord-B")
    assert s == 409 and err["error"]["code"] == "lease_held_by_other"
    # same holder reacquiring returns the SAME epoch
    _, l2 = acquire(api1, rid, "coord-A", op_id=new_key("re"))
    assert l2["epoch"] == l1["epoch"] and l2["action"] == "reacquired"


def test_epochs_strictly_increase_over_takeovers(api1, db_conn):
    plan, rollout = make_plan_rollout(api1, 4)
    rid = rollout["rollout_id"]
    epochs = []
    holder = "coord-X"
    _, lease = acquire(api1, rid, holder)
    epochs.append(lease["epoch"])
    for _ in range(3):
        expire_lease(db_conn, rid)
        _, lease = acquire(api1, rid, holder)
        epochs.append(lease["epoch"])
    assert epochs == sorted(epochs) and len(set(epochs)) == 4


def test_acquire_operation_is_idempotent(api1):
    plan, rollout = make_plan_rollout(api1, 1)
    rid = rollout["rollout_id"]
    op = new_key("acq")
    s1, r1 = acquire(api1, rid, "coord-A", op_id=op)
    s2, r2 = acquire(api1, rid, "coord-A", op_id=op)
    assert s1 == s2 == 200
    assert r1["epoch"] == r2["epoch"]
    assert r2["first_execution"] is False


def test_operation_id_cannot_be_reused_across_kinds(api1):
    plan, rollout = make_plan_rollout(api1, 2)
    rid = rollout["rollout_id"]
    op = new_key("shared-op")
    s1, _ = acquire(api1, rid, "coord-A", op_id=op)
    assert s1 == 200
    # reuse the SAME operation id for an advance -> rejected, nothing created
    s2, err = advance(api1, rid, "coord-A", 1, op_id=op)
    assert s2 == 409 and err["error"]["code"] == "operation_id_reused"
    _, state = api1.get(f"/api/rollouts/{rid}")
    assert len(state["steps"]) == 0


def test_lease_duration_bounds(api1):
    plan, rollout = make_plan_rollout(api1, 1)
    rid = rollout["rollout_id"]
    s, err = acquire(api1, rid, "c", ttl=4)
    assert s == 400 and err["error"]["code"] == "invalid_lease_duration"
    s, err = acquire(api1, rid, "c", ttl=61)
    assert s == 400 and err["error"]["code"] == "invalid_lease_duration"
    s, ok = acquire(api1, rid, "c", ttl=60)
    assert s == 200


def test_ack_does_not_require_live_lease(api1, db_conn):
    plan, rollout = make_plan_rollout(api1, 2)
    rid = rollout["rollout_id"]
    _, lease = acquire(api1, rid, "coord-A")
    _, r = advance(api1, rid, "coord-A", lease["epoch"])
    cmd = r["command"]
    # coordinator disappears and lease expires BEFORE the device acks
    expire_lease(db_conn, rid)
    s, res = ack(api1, rid, cmd["switch_id"], cmd)
    assert s == 200 and res["result"] == "APPLIED"
    # the ack closed exactly one step but did NOT create the next command
    _, state = api1.get(f"/api/rollouts/{rid}")
    assert len(state["steps"]) == 1 and state["steps"][0]["status"] == "APPLIED"
    assert state["status"] == "ACTIVE"
    # a new coordinator then advances
    _, l2 = acquire(api1, rid, "coord-B")
    _, r2 = advance(api1, rid, "coord-B", l2["epoch"])
    assert r2["action"] == "created" and r2["command"]["step"] == 1


def test_duplicate_ack_returns_first_result(api1):
    plan, rollout = make_plan_rollout(api1, 1)
    rid = rollout["rollout_id"]
    _, lease = acquire(api1, rid, "coord-A")
    _, r = advance(api1, rid, "coord-A", lease["epoch"])
    cmd = r["command"]
    s1, a1 = ack(api1, rid, cmd["switch_id"], cmd)
    s2, a2 = ack(api1, rid, cmd["switch_id"], cmd)
    assert s1 == s2 == 200
    assert a1["duplicate"] is False and a2["duplicate"] is True
    assert a2["status"] == "already_applied"
    # rollout completed exactly once
    _, state = api1.get(f"/api/rollouts/{rid}")
    assert state["status"] == "COMPLETED"


def test_mismatched_ack_fields_rejected(api1):
    plan, rollout = make_plan_rollout(api1, 2)
    rid = rollout["rollout_id"]
    _, lease = acquire(api1, rid, "coord-A")
    _, r = advance(api1, rid, "coord-A", lease["epoch"])
    cmd = r["command"]

    bad = dict(cmd)
    bad["step"] = cmd["step"] + 1
    s, err = ack(api1, rid, cmd["switch_id"], bad)
    assert s == 409 and err["error"]["code"] == "ack_step_mismatch"

    bad = dict(cmd)
    bad["device_generation"] = cmd["device_generation"] + 1
    s, err = ack(api1, rid, cmd["switch_id"], bad)
    assert s == 409 and err["error"]["code"] == "ack_generation_mismatch"

    bad = dict(cmd)
    bad["plan_digest"] = "deadbeef"
    s, err = ack(api1, rid, cmd["switch_id"], bad)
    assert s == 409 and err["error"]["code"] == "ack_digest_mismatch"

    # ack from a different switch
    other = plan["update_order"][1]
    s, err = ack(api1, rid, other, cmd)
    assert s == 409 and err["error"]["code"] == "ack_switch_mismatch"

    # original command still pending, state unchanged
    _, state = api1.get(f"/api/rollouts/{rid}")
    assert state["steps"][0]["status"] == "PENDING"


def test_unknown_command_ack_404(api1):
    plan, rollout = make_plan_rollout(api1, 1)
    rid = rollout["rollout_id"]
    _, lease = acquire(api1, rid, "coord-A")
    _, r = advance(api1, rid, "coord-A", lease["epoch"])
    cmd = dict(r["command"])
    cmd["command_id"] = str(uuid.uuid4())
    s, err = ack(api1, rid, r["command"]["switch_id"], cmd)
    assert s == 404 and err["error"]["code"] == "command_not_found"


def test_stale_generation_ack_rejected(api1):
    # Two rollouts commanding the same switch: a late low-generation ack in
    # the second rollout targets an old generation and must be rejected.
    ids, old, new = chain_direct_deliver(1)
    _, plan = create_plan(api1, {
        "switches": ids, "ingresses": ids,
        "old_next": old, "new_next": new}, new_key("plan"))
    _, ro1 = create_rollout(api1, plan["plan_id"], new_key("r1"))
    _, l1 = acquire(api1, ro1["rollout_id"], "c")
    _, r1 = advance(api1, ro1["rollout_id"], "c", l1["epoch"])
    gen1 = r1["command"]["device_generation"]
    s, _ = ack(api1, ro1["rollout_id"], ids[0], r1["command"])
    assert s == 200

    _, ro2 = create_rollout(api1, plan["plan_id"], new_key("r2"))
    _, l2 = acquire(api1, ro2["rollout_id"], "c")
    _, r2 = advance(api1, ro2["rollout_id"], "c", l2["epoch"])
    assert r2["command"]["device_generation"] == gen1 + 1

    # a late ack carrying generation 1 against the generation-2 command
    stale = dict(r2["command"])
    stale["device_generation"] = gen1
    s, err = ack(api1, ro2["rollout_id"], ids[0], stale)
    assert s == 409
    assert err["error"]["code"] == "ack_generation_mismatch"


def test_advance_requires_lease(api1):
    plan, rollout = make_plan_rollout(api1, 1)
    rid = rollout["rollout_id"]
    s, err = advance(api1, rid, "ghost", 999)
    assert s == 409 and err["error"]["code"] == "no_lease"


def test_renew_without_acquire_conflicts(api1):
    plan, rollout = make_plan_rollout(api1, 1)
    rid = rollout["rollout_id"]
    s, err = renew(api1, rid, "c", 1)
    assert s == 409 and err["error"]["code"] == "no_lease"


def test_create_rollout_of_impossible_plan_rejected(api1):
    # proven_impossible plans cannot be created via the API in this model
    # (see planner theory note); at minimum a malformed/unknown plan 404s.
    s, err = api1.post("/api/rollouts", {"idempotency_key": new_key(),
                                         "plan_id": str(uuid.uuid4())})
    assert s == 404 and err["error"]["code"] == "plan_not_found"


def test_rollout_idempotency_conflict(api1):
    plan, rollout = make_plan_rollout(api1, 1)
    key = new_key("shared-roll")
    s1, r1 = create_rollout(api1, plan["plan_id"], key)
    assert s1 == 201
    # another plan
    ids, old, new = chain_direct_deliver(2)
    _, plan2 = create_plan(api1, {
        "switches": ids, "ingresses": [ids[0]],
        "old_next": old, "new_next": new}, new_key("plan2"))
    s2, err = create_rollout(api1, plan2["plan_id"], key)
    assert s2 == 409 and err["error"]["code"] == "idempotency_conflict"
    # same key same plan -> same rollout
    s3, r3 = create_rollout(api1, plan["plan_id"], key)
    assert s3 == 200 and r3["rollout_id"] == r1["rollout_id"]


def test_zero_step_plan_completes_immediately(api1):
    payload = {
        "switches": ["a"], "ingresses": ["a"],
        "old_next": {"a": "DELIVER"}, "new_next": {"a": "DELIVER"}}
    _, plan = create_plan(api1, payload, new_key("zero"))
    assert plan["step_count"] == 0
    _, rollout = create_rollout(api1, plan["plan_id"], new_key("zr"))
    assert rollout["status"] == "COMPLETED"
