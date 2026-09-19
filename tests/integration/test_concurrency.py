"""Concurrency across TWO independent API instances sharing one database.

In Docker ``verify`` mode api1/api2 are separate containers; locally they are
two Flask clients over the same PostgreSQL database.
"""
import threading

from helpers import (ack, acquire, advance, chain_direct_deliver,
                     create_plan, create_rollout, expire_lease,
                     fetch_command, new_key)


def make_plan_rollout(client, n=6):
    ids, old, new = chain_direct_deliver(n)
    _, plan = create_plan(client, {
        "switches": ids, "ingresses": [ids[0]],
        "old_next": old, "new_next": new}, new_key("plan"))
    _, rollout = create_rollout(client, plan["plan_id"], new_key("roll"))
    return plan, rollout


def test_parallel_advance_only_one_command(api1, api2, run_concurrent):
    plan, rollout = make_plan_rollout(api1, 4)
    rid = rollout["rollout_id"]
    _, lease = acquire(api1, rid, "coord-1")
    epoch = lease["epoch"]

    results = run_concurrent(
        lambda i: advance(api1 if i % 2 else api2, rid, "coord-1", epoch),
        n=10)
    command_ids = {r[1]["command"]["command_id"] for r in results}
    generations = {r[1]["command"]["device_generation"] for r in results}
    assert len(command_ids) == 1
    assert len(generations) == 1
    _, state = api1.get(f"/api/rollouts/{rid}")
    assert len(state["steps"]) == 1


def test_two_coordinators_cannot_both_hold_live_lease(api1, api2):
    plan, rollout = make_plan_rollout(api1, 2)
    rid = rollout["rollout_id"]
    s1, l1 = acquire(api1, rid, "coord-1")
    assert s1 == 200
    s2, err = acquire(api2, rid, "coord-2")
    assert s2 == 409 and err["error"]["code"] == "lease_held_by_other"
    # both instances agree on the single live epoch
    _, state = api2.get(f"/api/rollouts/{rid}")
    assert state["coordinator"]["epoch"] == l1["epoch"]


def test_takeover_via_second_instance_then_old_fails(api1, api2, db_conn):
    plan, rollout = make_plan_rollout(api1, 3)
    rid = rollout["rollout_id"]
    _, l1 = acquire(api1, rid, "coord-1")
    _, r1 = advance(api1, rid, "coord-1", l1["epoch"])

    expire_lease(db_conn, rid)
    s2, l2 = acquire(api2, rid, "coord-2")
    assert s2 == 200 and l2["epoch"] > l1["epoch"]

    # stale coordinator on api1 cannot create anything
    s, err = advance(api1, rid, "coord-1", l1["epoch"])
    assert s == 409 and err["error"]["code"] == "stale_epoch"

    # new coordinator sees the original command, not a new one
    _, r2 = advance(api2, rid, "coord-2", l2["epoch"])
    assert r2["action"] == "existing"
    assert r2["command"]["command_id"] == r1["command"]["command_id"]


def test_full_migration_alternating_instances(api1, api2):
    plan, rollout = make_plan_rollout(api1, 5)
    rid = rollout["rollout_id"]
    _, lease = acquire(api1, rid, "coord-main")
    epoch = lease["epoch"]

    for step in range(5):
        client = api1 if step % 2 == 0 else api2
        _, adv = advance(client, rid, "coord-main", epoch)
        assert adv["command"]["step"] == step
        cmd = adv["command"]
        # device may pull from either instance
        _, fetched = fetch_command(api2, rid, cmd["switch_id"])
        assert fetched["command"]["command_id"] == cmd["command_id"]
        # and ack through the other instance
        s, _ = ack(api1, rid, cmd["switch_id"], cmd)
        assert s == 200

    _, state = api2.get(f"/api/rollouts/{rid}")
    assert state["status"] == "COMPLETED"


def test_concurrent_ack_submissions_collapse(api1, api2, run_concurrent):
    plan, rollout = make_plan_rollout(api1, 1)
    rid = rollout["rollout_id"]
    _, lease = acquire(api1, rid, "coord-1")
    _, r = advance(api1, rid, "coord-1", lease["epoch"])
    cmd = r["command"]

    results = run_concurrent(
        lambda i: ack(api1 if i % 2 else api2, rid, cmd["switch_id"], cmd),
        n=8)
    accepted = [r for r in results if r[1].get("duplicate") is False]
    duplicates = [r for r in results if r[1].get("duplicate") is True]
    assert len(accepted) == 1 and len(duplicates) == 7
    _, state = api1.get(f"/api/rollouts/{rid}")
    assert state["status"] == "COMPLETED"


def test_concurrent_plan_creation_across_instances(api1, api2, run_concurrent):
    ids, old, new = chain_direct_deliver(3)
    payload = {"switches": ids, "ingresses": [ids[0]],
               "old_next": old, "new_next": new}
    key = new_key("cross-plan")
    results = run_concurrent(
        lambda i: (api1 if i % 2 else api2).post(
            "/api/plans", {**payload, "idempotency_key": key}), n=8)
    plan_ids = {r[1]["plan_id"] for r in results}
    assert len(plan_ids) == 1
    created = [r for r in results if r[0] == 201]
    assert len(created) == 1


def test_restart_recovery_from_database_only(api1, api2, db_conn):
    """A brand-new app object/container must reconstruct everything from the
    database; in local mode that is a fresh Flask client, in Docker verify
    mode the second container plays that role."""
    plan, rollout = make_plan_rollout(api1, 3)
    rid = rollout["rollout_id"]
    _, lease = acquire(api1, rid, "coord-1")
    _, r1 = advance(api1, rid, "coord-1", lease["epoch"])
    ack(api1, rid, r1["command"]["switch_id"], r1["command"])
    _, r2 = advance(api1, rid, "coord-1", lease["epoch"])

    # A "fresh process" — new Flask app/client with no shared memory locally;
    # in Docker verify mode the second container plays that role.
    import os
    if os.environ.get("API1_URL"):
        client = api2
    else:
        from app.api import create_app
        fresh = create_app()
        fresh.testing = True
        client = type(api1)("local://fresh", fresh.test_client())

    _, state = client.get(f"/api/rollouts/{rid}")
    assert state["status"] == "ACTIVE"
    assert state["pending_command"]["command_id"] == \
        r2["command"]["command_id"]
    assert state["pending_command"]["device_generation"] == \
        r2["command"]["device_generation"]
    _, adv = advance(client, rid, "coord-1", lease["epoch"])
    # cannot create a parallel command before ack even from the "new process"
    assert adv["command"]["command_id"] == r2["command"]["command_id"]
