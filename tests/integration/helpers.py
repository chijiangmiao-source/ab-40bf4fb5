"""Helpers shared by integration tests."""
import uuid


def new_key(prefix="idem"):
    return f"{prefix}-{uuid.uuid4()}"


def chain_direct_deliver(n, *, prefix="s"):
    """Exactly ``n`` changing switches plus one unchanged tail switch.

    old: s0->s1->...->s(n-1)->tail->DELIVER
    new: s0->DELIVER, s_i->s(i-1) for i>0, tail->DELIVER (unchanged)

    Both endpoints are safe; exactly s0..s(n-1) differ (n changes, works for
    n >= 1); every update prefix is safe and the lex-minimal order is
    [s0, s1, ..., s(n-1)].
    """
    ids = [f"{prefix}{i}" for i in range(n)]
    tail = f"{prefix}tail"
    switches = ids + [tail]
    old = {}
    for i in range(n):
        old[ids[i]] = ids[i + 1] if i + 1 < n else tail
    old[tail] = "DELIVER"
    new = {ids[0]: "DELIVER"}
    for i in range(1, n):
        new[ids[i]] = ids[i - 1]
    new[tail] = "DELIVER"
    return switches, old, new


def create_plan(client, payload, key=None, headers=None):
    body = dict(payload)
    if key is not None:
        body["idempotency_key"] = key
    status, data = client.post("/api/plans", body, headers=headers)
    return status, data


def create_rollout(client, plan_id, key=None):
    status, data = client.post("/api/rollouts", {
        "idempotency_key": key or new_key("roll"),
        "plan_id": plan_id,
    })
    return status, data


def acquire(client, rollout_id, holder, *, op_id=None, ttl=30):
    return client.post(
        f"/api/rollouts/{rollout_id}/coordinator/acquire",
        {"holder_id": holder, "operation_id": op_id or new_key("op"),
         "duration_seconds": ttl})


def renew(client, rollout_id, holder, epoch, *, op_id=None, ttl=30):
    return client.post(
        f"/api/rollouts/{rollout_id}/coordinator/renew",
        {"holder_id": holder, "epoch": epoch,
         "operation_id": op_id or new_key("op"), "duration_seconds": ttl})


def advance(client, rollout_id, holder, epoch, *, op_id=None):
    return client.post(f"/api/rollouts/{rollout_id}/advance",
                       {"holder_id": holder, "epoch": epoch,
                        "operation_id": op_id or new_key("adv")})


def fetch_command(client, rollout_id, switch_id):
    return client.get(
        f"/api/rollouts/{rollout_id}/devices/{switch_id}/command")


def ack(client, rollout_id, switch_id, command):
    return client.post(
        f"/api/rollouts/{rollout_id}/devices/{switch_id}/ack",
        {"command_id": command["command_id"], "step": command["step"],
         "plan_digest": command["plan_digest"],
         "device_generation": command["device_generation"],
         "result": "APPLIED"})


def expire_lease(db_conn, rollout_id):
    """Force the lease to be expired using the database clock."""
    db_conn.execute(
        "UPDATE coordinator_leases SET lease_expires_at = now() - interval "
        "'10 second' WHERE rollout_id = %s", (uuid.UUID(rollout_id),))
    db_conn.commit()
