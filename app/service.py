"""Business logic. Every concurrency decision is a database transaction;
multiple API processes sharing one PostgreSQL database stay correct without
any in-process locks, in-memory queues or local clocks.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from . import planner
from .db import connect

MIN_LEASE_SECONDS = 5
MAX_LEASE_SECONDS = 60
DEFAULT_LEASE_SECONDS = 15


class ServiceError(Exception):
    def __init__(self, code: str, message: str, http_status: int = 400,
                 details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.details = details or {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fingerprint(canonical: bytes) -> str:
    return hashlib.sha256(canonical).hexdigest()


def _require_idempotency_key(body: dict[str, Any], headers) -> str:
    key = body.get("idempotency_key")
    if key is None and headers is not None:
        key = headers.get("Idempotency-Key")
    if not isinstance(key, str) or not key:
        raise ServiceError("missing_idempotency_key",
                           "an idempotency key is required (body field "
                           "'idempotency_key' or 'Idempotency-Key' header)",
                           400)
    if len(key) > 200:
        raise ServiceError("invalid_idempotency_key",
                           "idempotency key must be at most 200 characters",
                           400)
    return key


def _require_operation_id(body: dict[str, Any], headers) -> str:
    op_id = body.get("operation_id")
    if op_id is None and headers is not None:
        op_id = headers.get("Idempotency-Key")
    if not isinstance(op_id, str) or not op_id:
        raise ServiceError("missing_operation_id",
                           "an idempotent operation id is required (body "
                           "field 'operation_id' or 'Idempotency-Key' header)",
                           400)
    if len(op_id) > 200:
        raise ServiceError("invalid_operation_id",
                           "operation id must be at most 200 characters", 400)
    return op_id


def _lease_duration(body: dict[str, Any]) -> int:
    raw = body.get("duration_seconds", DEFAULT_LEASE_SECONDS)
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ServiceError("invalid_lease_duration",
                           "'duration_seconds' must be an integer", 400)
    if not MIN_LEASE_SECONDS <= raw <= MAX_LEASE_SECONDS:
        raise ServiceError("invalid_lease_duration",
                           f"'duration_seconds' must be between "
                           f"{MIN_LEASE_SECONDS} and {MAX_LEASE_SECONDS}", 400,
                           {"min": MIN_LEASE_SECONDS, "max": MAX_LEASE_SECONDS})
    return raw


def _audit(conn, rollout_id, event_type: str, epoch: int | None,
           payload: dict[str, Any]) -> None:
    conn.execute(
        "INSERT INTO audit_events (rollout_id, event_type, epoch, payload) "
        "VALUES (%s, %s, %s, %s)",
        (rollout_id, event_type, epoch, Jsonb(payload)))


def _command_json(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "command_id": str(row["id"]),
        "rollout_id": str(row["rollout_id"]),
        "step": row["step"],
        "switch_id": row["switch_id"],
        "plan_digest": row["plan_digest"],
        "device_generation": row["device_generation"],
        "status": row["status"],
        "created_at": row["created_at"].isoformat(),
        "applied_at": row["applied_at"].isoformat()
        if row.get("applied_at") else None,
    }


# ---------------------------------------------------------------------------
# Plans
# ---------------------------------------------------------------------------

def create_plan(body: Any, headers=None) -> dict[str, Any]:
    topo = planner.validate_topology(body if isinstance(body, dict) else None)
    idem_key = _require_idempotency_key(body, headers)

    changed = planner.changed_switches(topo)
    if len(changed) > planner.MAX_UPDATES:
        raise ServiceError(
            "too_many_updates",
            f"at most {planner.MAX_UPDATES} switches may differ between old "
            f"and new next hops (found {len(changed)})", 400,
            {"max": planner.MAX_UPDATES, "count": len(changed)})

    canonical = planner.canonical_input(topo)
    fingerprint = _fingerprint(canonical)

    # Detect key reuse with different parameters BEFORE running potentially
    # expensive endpoint checks/search: the conflict is stable regardless of
    # whether the new parameters themselves validate.
    probe_conn = connect()
    try:
        probe = probe_conn.execute(
            "SELECT id, input_fingerprint FROM plans "
            "WHERE idempotency_key = %s", (idem_key,)).fetchone()
    finally:
        probe_conn.close()
    if probe is not None and probe["input_fingerprint"] != fingerprint:
        raise ServiceError(
            "idempotency_conflict",
            "idempotency key was already used with different parameters", 409,
            {"idempotency_key": idem_key,
             "existing_plan_id": str(probe["id"])})

    compiled = planner._compile(topo)
    changed_mask = 0
    index = {s: i for i, s in enumerate(topo["switches"])}
    for sw in changed:
        changed_mask |= 1 << index[sw]

    if not planner._safe_mask(compiled, 0):
        raise ServiceError("initial_state_unsafe",
                           "the initial forwarding table does not deliver "
                           "every ingress to DELIVER within the step budget",
                           422)
    if not planner._safe_mask(compiled, changed_mask):
        raise ServiceError("final_state_unsafe",
                           "the final forwarding table does not deliver "
                           "every ingress to DELIVER within the step budget",
                           422)

    order = planner.find_lexicographically_minimal_order(topo)

    if order is None:
        status, digest, step_count = "proven_impossible", None, 0
    else:
        status, digest = "feasible", planner.compute_plan_digest(
            canonical, order)
        step_count = len(order)

    plan_id = uuid.uuid4()
    conn = connect()
    try:
        try:
            with conn.transaction():
                existing = conn.execute(
                    "SELECT * FROM plans WHERE idempotency_key = %s FOR UPDATE",
                    (idem_key,)).fetchone()
                if existing is not None:
                    if existing["input_fingerprint"] != fingerprint:
                        raise ServiceError(
                            "idempotency_conflict",
                            "idempotency key was already used with different "
                            "parameters", 409,
                            {"idempotency_key": idem_key,
                             "existing_plan_id": str(existing["id"])})
                    return _plan_json(existing), False

                conn.execute(
                    "INSERT INTO plans (id, idempotency_key, input_fingerprint, "
                    "input_canonical, switches, ingresses, old_next, new_next, "
                    "changed_switches, step_count, status, update_order, "
                    "plan_digest) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (plan_id, idem_key, fingerprint, Jsonb(json.loads(
                        canonical.decode("utf-8"))),
                     Jsonb(topo["switches"]), Jsonb(topo["entries"]),
                     Jsonb(topo["old_next"]), Jsonb(topo["new_next"]),
                     Jsonb(changed), step_count, status,
                     Jsonb(order) if order is not None else None, digest))
        except psycopg.errors.UniqueViolation:
            # A concurrent first request with the same key committed first.
            with conn.transaction():
                existing = conn.execute(
                    "SELECT * FROM plans WHERE idempotency_key = %s",
                    (idem_key,)).fetchone()
            if existing is None:  # pragma: no cover - extremely narrow race
                raise
            if existing["input_fingerprint"] != fingerprint:
                raise ServiceError(
                    "idempotency_conflict",
                    "idempotency key was already used with different "
                    "parameters", 409,
                    {"idempotency_key": idem_key,
                     "existing_plan_id": str(existing["id"])})
            return _plan_json(existing), False
        row = conn.execute("SELECT * FROM plans WHERE id = %s",
                           (plan_id,)).fetchone()
        return _plan_json(row), True
    finally:
        conn.close()


def _plan_json(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "plan_id": str(row["id"]),
        "idempotency_key": row["idempotency_key"],
        "status": row["status"],
        "switches": row["switches"],
        "ingresses": row["ingresses"],
        "old_next": row["old_next"],
        "new_next": row["new_next"],
        "changed_switches": row["changed_switches"],
        "input_canonical": row["input_canonical"],
        "update_order": row["update_order"],
        "plan_digest": row["plan_digest"],
        "step_count": row["step_count"],
        "created_at": row["created_at"].isoformat(),
    }


def get_plan(plan_id: str) -> dict[str, Any]:
    try:
        pid = uuid.UUID(plan_id)
    except (ValueError, AttributeError):
        raise ServiceError("invalid_plan_id", "malformed plan id", 400)
    conn = connect()
    try:
        row = conn.execute("SELECT * FROM plans WHERE id = %s",
                           (pid,)).fetchone()
    finally:
        conn.close()
    if row is None:
        raise ServiceError("plan_not_found", f"plan {plan_id} not found", 404)
    return _plan_json(row)


# ---------------------------------------------------------------------------
# Rollouts
# ---------------------------------------------------------------------------

def create_rollout(body: Any, headers=None) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise ServiceError("invalid_body", "request body must be a JSON object",
                           400)
    idem_key = _require_idempotency_key(body, headers)
    raw_plan = body.get("plan_id")
    if not isinstance(raw_plan, str):
        raise ServiceError("missing_plan_id", "'plan_id' is required", 400)
    try:
        plan_id = uuid.UUID(raw_plan)
    except ValueError:
        raise ServiceError("invalid_plan_id", "malformed plan id", 400)

    conn = connect()
    try:
        try:
            with conn.transaction():
                plan = conn.execute("SELECT * FROM plans WHERE id = %s FOR UPDATE",
                                    (plan_id,)).fetchone()
                if plan is None:
                    raise ServiceError("plan_not_found",
                                       f"plan {raw_plan} not found", 404)
                if plan["status"] != "feasible":
                    raise ServiceError(
                        "plan_not_executable",
                        "a proven_impossible plan cannot be rolled out", 422,
                        {"plan_id": str(plan_id)})

                existing = conn.execute(
                    "SELECT * FROM rollouts WHERE idempotency_key = %s FOR UPDATE",
                    (idem_key,)).fetchone()
                if existing is not None:
                    if str(existing["plan_id"]) != str(plan_id):
                        raise ServiceError(
                            "idempotency_conflict",
                            "idempotency key was already used with a different "
                            "plan", 409, {"idempotency_key": idem_key})
                    return _rollout_json(conn, existing), False

                total_steps = plan["step_count"]
                rid = uuid.uuid4()
                # A zero-step plan has nothing to execute: it is born COMPLETED.
                initial_status = "COMPLETED" if total_steps == 0 else "PENDING"
                conn.execute(
                    "INSERT INTO rollouts (id, idempotency_key, plan_id, "
                    "plan_digest, total_steps, status) VALUES (%s,%s,%s,%s,%s,%s)",
                    (rid, idem_key, plan_id, plan["plan_digest"], total_steps,
                     initial_status))
                if initial_status == "COMPLETED":
                    _audit(conn, rid, "rollout_completed", None,
                           {"reason": "zero_step_plan"})
                row = conn.execute("SELECT * FROM rollouts WHERE id = %s",
                                   (rid,)).fetchone()
                return _rollout_json(conn, row), True
        except psycopg.errors.UniqueViolation:
            # Concurrent first creation with the same key.
            with conn.transaction():
                existing = conn.execute(
                    "SELECT * FROM rollouts WHERE idempotency_key = %s",
                    (idem_key,)).fetchone()
            if existing is None:  # pragma: no cover
                raise
            if str(existing["plan_id"]) != str(plan_id):
                raise ServiceError(
                    "idempotency_conflict",
                    "idempotency key was already used with a different plan",
                    409, {"idempotency_key": idem_key})
            return _rollout_json(conn, existing), False
    finally:
        conn.close()


def _rollout_json(conn, row: dict[str, Any], *, detailed: bool = False) -> dict[str, Any]:
    data = {
        "rollout_id": str(row["id"]),
        "plan_id": str(row["plan_id"]),
        "plan_digest": row["plan_digest"],
        "total_steps": row["total_steps"],
        "status": row["status"],
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat(),
    }
    if detailed:
        lease = conn.execute(
            "SELECT epoch, holder_id, lease_expires_at, updated_at "
            "FROM coordinator_leases WHERE rollout_id = %s",
            (row["id"],)).fetchone()
        data["coordinator"] = None if lease is None else {
            "epoch": lease["epoch"],
            "holder_id": lease["holder_id"],
            "lease_expires_at": lease["lease_expires_at"].isoformat(),
        }
        steps = conn.execute(
            "SELECT * FROM device_commands WHERE rollout_id = %s "
            "ORDER BY step", (row["id"],)).fetchall()
        data["steps"] = [_command_json(s) for s in steps]
        data["pending_command"] = next(
            (_command_json(s) for s in steps if s["status"] == "PENDING"),
            None)
    return data


def _get_rollout_for_update(conn, rollout_id: str):
    try:
        rid = uuid.UUID(rollout_id)
    except (ValueError, AttributeError):
        raise ServiceError("invalid_rollout_id", "malformed rollout id", 400)
    row = conn.execute(
        "SELECT * FROM rollouts WHERE id = %s FOR UPDATE",
        (rid,)).fetchone()
    if row is None:
        raise ServiceError("rollout_not_found",
                           f"rollout {rollout_id} not found", 404)
    return rid, row


def _get_rollout(conn, rollout_id: str):
    try:
        rid = uuid.UUID(rollout_id)
    except (ValueError, AttributeError):
        raise ServiceError("invalid_rollout_id", "malformed rollout id", 400)
    row = conn.execute("SELECT * FROM rollouts WHERE id = %s",
                       (rid,)).fetchone()
    if row is None:
        raise ServiceError("rollout_not_found",
                           f"rollout {rollout_id} not found", 404)
    return rid, row


# ---------------------------------------------------------------------------
# Coordinator lease: acquire / take over / renew
# ---------------------------------------------------------------------------

def _lease_response(row: dict[str, Any], first: bool, action: str) -> dict[str, Any]:
    return {
        "rollout_id": str(row["rollout_id"]),
        "epoch": row["epoch"],
        "holder_id": row["holder_id"],
        "lease_expires_at": row["lease_expires_at"].isoformat(),
        "action": action,          # acquired | renewed | reacquired
        "first_execution": first,
    }


def acquire_lease(rollout_id: str, body: Any, headers=None) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise ServiceError("invalid_body", "request body must be a JSON object",
                           400)
    operation_id = _require_operation_id(body, headers)
    holder = body.get("holder_id")
    if not isinstance(holder, str) or not holder:
        raise ServiceError("missing_holder_id",
                           "'holder_id' is required", 400)
    ttl = _lease_duration(body)

    conn = connect()
    try:
        with conn.transaction():
            rid, rollout = _get_rollout_for_update(conn, rollout_id)
            dup = _find_op(conn, rid, operation_id)
            if dup is not None:
                return _replay_or_conflict(dup, "acquire")

            lease = conn.execute(
                "SELECT * FROM coordinator_leases WHERE rollout_id = %s",
                (rid,)).fetchone()
            now = conn.execute("SELECT now() AS t").fetchone()["t"]

            if lease is not None and lease["lease_expires_at"] > now \
                    and lease["holder_id"] != holder:
                raise ServiceError(
                    "lease_held_by_other",
                    "a live coordinator lease belongs to another holder", 409,
                    {"epoch": lease["epoch"],
                     "lease_expires_at": lease["lease_expires_at"].isoformat()})

            if lease is not None and lease["lease_expires_at"] > now:
                # Same live holder: keep the same epoch but extend the lease
                # to the requested TTL (a lost-response retry or a redundant
                # reacquire must not allocate a new epoch nor shorten it).
                expires = conn.execute(
                    "SELECT now() + (%s * interval '1 second') AS t",
                    (ttl,)).fetchone()["t"]
                conn.execute(
                    "UPDATE coordinator_leases SET lease_expires_at = %s, "
                    "updated_at = now() WHERE rollout_id = %s",
                    (expires, rid))
                action = "reacquired"
                lease_row = {"rollout_id": rid, "epoch": lease["epoch"],
                             "holder_id": holder,
                             "lease_expires_at": expires}
            else:
                # Fresh acquisition or takeover: allocate a brand-new epoch.
                epoch = conn.execute(
                    "SELECT nextval('coordinator_epoch_seq') AS e"
                ).fetchone()["e"]
                expires = conn.execute(
                    "SELECT now() + (%s * interval '1 second') AS t",
                    (ttl,)).fetchone()["t"]
                conn.execute(
                    "INSERT INTO coordinator_leases (rollout_id, epoch, "
                    "holder_id, lease_expires_at, updated_at) "
                    "VALUES (%s,%s,%s,%s,now()) "
                    "ON CONFLICT (rollout_id) DO UPDATE SET "
                    "epoch = EXCLUDED.epoch, holder_id = EXCLUDED.holder_id, "
                    "lease_expires_at = EXCLUDED.lease_expires_at, "
                    "updated_at = now()",
                    (rid, epoch, holder, expires))
                lease_row = {"rollout_id": rid, "epoch": epoch,
                             "holder_id": holder,
                             "lease_expires_at": expires}
                action = "acquired"
                _audit(conn, rid,
                        "lease_acquired" if lease is None else "lease_taken_over",
                        epoch, {"holder_id": holder,
                                "previous_epoch": lease["epoch"] if lease
                                else None})

            result = _lease_response(lease_row, True, action)
            _record_op(conn, rid, operation_id, "acquire",
                       lease_row["epoch"], holder, result)
            return result, True
    finally:
        conn.close()


def renew_lease(rollout_id: str, body: Any, headers=None) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise ServiceError("invalid_body", "request body must be a JSON object",
                           400)
    operation_id = _require_operation_id(body, headers)
    holder = body.get("holder_id")
    if not isinstance(holder, str) or not holder:
        raise ServiceError("missing_holder_id", "'holder_id' is required", 400)
    ttl = _lease_duration(body)

    conn = connect()
    try:
        with conn.transaction():
            rid, _ = _get_rollout_for_update(conn, rollout_id)
            dup = _find_op(conn, rid, operation_id)
            if dup is not None:
                return _replay_or_conflict(dup, "renew")

            lease = conn.execute(
                "SELECT * FROM coordinator_leases WHERE rollout_id = %s",
                (rid,)).fetchone()
            now = conn.execute("SELECT now() AS t").fetchone()["t"]
            if lease is None:
                raise ServiceError("no_lease",
                                   "this rollout has no coordinator lease; "
                                   "acquire one first", 409)
            if lease["holder_id"] != holder:
                raise ServiceError("not_lease_holder",
                                   "lease belongs to another holder", 409,
                                   {"epoch": lease["epoch"]})
            if lease["lease_expires_at"] <= now:
                raise ServiceError(
                    "lease_expired",
                    "the lease has expired; acquire a new lease (takeover) "
                    "to obtain a new epoch", 410, {"epoch": lease["epoch"]})

            epoch = body.get("epoch")
            if not isinstance(epoch, int) or isinstance(epoch, bool):
                raise ServiceError("missing_epoch",
                                   "'epoch' (integer) is required", 400)
            if epoch != lease["epoch"]:
                raise ServiceError("stale_epoch",
                                   "the supplied epoch is not the current "
                                   "epoch", 409,
                                   {"supplied_epoch": epoch,
                                    "current_epoch": lease["epoch"]})

            expires = conn.execute(
                "SELECT now() + (%s * interval '1 second') AS t",
                (ttl,)).fetchone()["t"]
            conn.execute(
                "UPDATE coordinator_leases SET lease_expires_at = %s, "
                "updated_at = now() WHERE rollout_id = %s",
                (expires, rid))
            lease_row = {"rollout_id": rid, "epoch": lease["epoch"],
                         "holder_id": holder, "lease_expires_at": expires}
            _audit(conn, rid, "lease_renewed", lease["epoch"],
                   {"holder_id": holder,
                    "lease_expires_at": expires.isoformat()})
            result = _lease_response(lease_row, True, "renewed")
            _record_op(conn, rid, operation_id, "renew", lease["epoch"],
                       holder, result)
            return result, True
    finally:
        conn.close()


def _find_op(conn, rid, operation_id):
    return conn.execute(
        "SELECT * FROM coordinator_ops WHERE rollout_id = %s "
        "AND operation_id = %s", (rid, operation_id)).fetchone()


def _replay_or_conflict(dup, expected_kind: str):
    if dup["op_kind"] != expected_kind:
        raise ServiceError(
            "operation_id_reused",
            "this operation id was already used for a different operation",
            409, {"operation_id": dup["operation_id"],
                  "original_kind": dup["op_kind"],
                  "attempted_kind": expected_kind})
    return {**dup["result"], "first_execution": False}, False


def _record_op(conn, rid, operation_id, kind, epoch, holder, result) -> None:
    conn.execute(
        "INSERT INTO coordinator_ops (rollout_id, operation_id, op_kind, "
        "epoch, holder_id, result) VALUES (%s,%s,%s,%s,%s,%s)",
        (rid, operation_id, kind, epoch, holder, Jsonb(result)))


def _authorize_coordinator(conn, rid, body):
    """Validate epoch + holder against a live lease. Returns (lease, now)."""
    epoch = body.get("epoch")
    if not isinstance(epoch, int) or isinstance(epoch, bool):
        raise ServiceError("missing_epoch", "'epoch' (integer) is required",
                           400)
    holder = body.get("holder_id")
    if not isinstance(holder, str) or not holder:
        raise ServiceError("missing_holder_id", "'holder_id' is required", 400)
    lease = conn.execute(
        "SELECT * FROM coordinator_leases WHERE rollout_id = %s",
        (rid,)).fetchone()
    now = conn.execute("SELECT now() AS t").fetchone()["t"]
    if lease is None:
        raise ServiceError("no_lease",
                           "this rollout has no coordinator lease", 409)
    if lease["epoch"] != epoch or lease["holder_id"] != holder:
        raise ServiceError("stale_epoch",
                           "the supplied epoch/holder is not the current "
                           "coordinator", 409,
                           {"supplied_epoch": epoch,
                            "current_epoch": lease["epoch"]})
    if lease["lease_expires_at"] <= now:
        raise ServiceError("lease_expired",
                           "the lease has expired; acquire a new lease", 410,
                           {"epoch": epoch})
    return lease, now


# ---------------------------------------------------------------------------
# Advancement
# ---------------------------------------------------------------------------

def advance(rollout_id: str, body: Any, headers=None) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise ServiceError("invalid_body", "request body must be a JSON object",
                           400)
    operation_id = _require_operation_id(body, headers)

    conn = connect()
    try:
        with conn.transaction():
            rid, rollout = _get_rollout_for_update(conn, rollout_id)
            dup = _find_op(conn, rid, operation_id)
            if dup is not None:
                return _replay_or_conflict(dup, "advance")

            _authorize_coordinator(conn, rid, body)
            epoch = body["epoch"]

            if rollout["status"] == "COMPLETED":
                raise ServiceError("rollout_completed",
                                   "the rollout is already COMPLETED", 409)

            last = conn.execute(
                "SELECT * FROM device_commands WHERE rollout_id = %s "
                "ORDER BY step DESC LIMIT 1 FOR UPDATE",
                (rid,)).fetchone()

            if last is not None and last["status"] == "PENDING":
                # One outstanding command exists for the current step; a lost
                # response / restart / takeover must return that exact
                # command: same id, same generation, no extra increment.
                result = {"action": "existing", "command": _command_json(last),
                          "first_execution": True}
                _record_op(conn, rid, operation_id, "advance", epoch,
                           body["holder_id"], result)
                return result, True

            if last is not None and last["step"] >= rollout["total_steps"] - 1:
                raise ServiceError("rollout_completed",
                                   "all steps have been commanded", 409)

            next_step = 0 if last is None else last["step"] + 1
            plan = conn.execute("SELECT update_order FROM plans WHERE id = %s",
                                (rollout["plan_id"],)).fetchone()
            switch_id = plan["update_order"][next_step]

            # Strictly increasing per-device generation.
            gen_row = conn.execute(
                "INSERT INTO device_generations (switch_id, "
                "current_generation) VALUES (%s,1) "
                "ON CONFLICT (switch_id) DO UPDATE SET "
                "current_generation = device_generations.current_generation + 1 "
                "RETURNING current_generation",
                (switch_id,)).fetchone()
            generation = gen_row["current_generation"]

            command_id = uuid.uuid4()
            conn.execute(
                "INSERT INTO device_commands (id, rollout_id, step, switch_id, "
                "plan_digest, device_generation, status) "
                "VALUES (%s,%s,%s,%s,%s,%s,'PENDING')",
                (command_id, rid, next_step, switch_id,
                 rollout["plan_digest"], generation))
            conn.execute(
                "UPDATE rollouts SET status = 'ACTIVE', updated_at = now() "
                "WHERE id = %s AND status = 'PENDING'", (rid,))
            _audit(conn, rid, "command_created", epoch, {
                "command_id": str(command_id), "step": next_step,
                "switch_id": switch_id, "device_generation": generation})

            cmd = conn.execute(
                "SELECT * FROM device_commands WHERE id = %s",
                (command_id,)).fetchone()
            result = {"action": "created", "command": _command_json(cmd),
                      "first_execution": True}
            _record_op(conn, rid, operation_id, "advance", epoch,
                       body["holder_id"], result)
            return result, True
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Device side: fetch pending command, acknowledge
# ---------------------------------------------------------------------------

def get_pending_command(rollout_id: str, switch_id: str) -> dict[str, Any]:
    if not isinstance(switch_id, str) or not switch_id:
        raise ServiceError("invalid_switch_id", "'switch_id' is required", 400)
    conn = connect()
    try:
        rid, rollout = _get_rollout(conn, rollout_id)
        plan = conn.execute(
            "SELECT switches FROM plans WHERE id = %s",
            (rollout["plan_id"],)).fetchone()
        if switch_id not in plan["switches"]:
            raise ServiceError("switch_not_in_plan",
                               f"switch {switch_id!r} is not part of this "
                               "plan", 404, {"switch_id": switch_id})
        row = conn.execute(
            "SELECT * FROM device_commands WHERE rollout_id = %s "
            "AND status = 'PENDING' ORDER BY step LIMIT 1",
            (rid,)).fetchone()
        if row is None or row["switch_id"] != switch_id:
            return {"rollout_id": str(rid), "switch_id": switch_id,
                    "command": None}
        return {"rollout_id": str(rid), "switch_id": switch_id,
                "command": _command_json(row)}
    finally:
        conn.close()


def submit_acknowledgement(rollout_id: str, body: Any, headers=None) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise ServiceError("invalid_body", "request body must be a JSON object",
                           400)
    required = ("switch_id", "command_id", "step", "plan_digest",
                "device_generation", "result")
    for field in required:
        if field not in body:
            raise ServiceError("missing_field",
                               f"field {field!r} is required", 400,
                               {"field": field})
    if body["result"] != "APPLIED":
        raise ServiceError("invalid_ack_result",
                           "only 'APPLIED' acknowledgements are accepted",
                           422, {"received": body["result"]})
    switch_id = body["switch_id"]
    if not isinstance(switch_id, str) or not switch_id:
        raise ServiceError("invalid_switch_id", "'switch_id' must be a string",
                           400)
    if not isinstance(body["step"], int) or isinstance(body["step"], bool) \
            or body["step"] < 0:
        raise ServiceError("invalid_step", "'step' must be a non-negative int",
                           400)
    if not isinstance(body["device_generation"], int) \
            or isinstance(body["device_generation"], bool) \
            or body["device_generation"] < 1:
        raise ServiceError("invalid_device_generation",
                           "'device_generation' must be a positive int", 400)
    if not isinstance(body["plan_digest"], str) or not body["plan_digest"]:
        raise ServiceError("invalid_plan_digest",
                           "'plan_digest' must be a non-empty string", 400)
    try:
        command_uuid = uuid.UUID(str(body["command_id"]))
    except (ValueError, AttributeError):
        raise ServiceError("invalid_command_id", "malformed command_id", 400)

    conn = connect()
    try:
        with conn.transaction():
            rid, rollout = _get_rollout_for_update(conn, rollout_id)

            command = conn.execute(
                "SELECT * FROM device_commands WHERE id = %s FOR UPDATE",
                (command_uuid,)).fetchone()
            if command is None or command["rollout_id"] != rid:
                raise ServiceError("command_not_found",
                                   "no such command for this rollout", 404,
                                   {"command_id": str(command_uuid)})

            def _mismatch(code, message, **details):
                raise ServiceError(code, message, 409, details)

            if switch_id != command["switch_id"]:
                _mismatch("ack_switch_mismatch",
                          "acknowledgement switch_id does not match the "
                          "command",
                          submitted=switch_id,
                          expected=command["switch_id"])
            if body["step"] != command["step"]:
                _mismatch("ack_step_mismatch",
                          "acknowledgement step does not match the command",
                          submitted=body["step"], expected=command["step"])
            if body["plan_digest"] != command["plan_digest"]:
                _mismatch("ack_digest_mismatch",
                          "acknowledgement plan_digest does not match the "
                          "command",
                          submitted=body["plan_digest"],
                          expected=command["plan_digest"])
            if body["device_generation"] != command["device_generation"]:
                _mismatch("ack_generation_mismatch",
                          "acknowledgement device_generation does not match "
                          "the command",
                          submitted=body["device_generation"],
                          expected=command["device_generation"])

            gen = conn.execute(
                "SELECT current_generation FROM device_generations "
                "WHERE switch_id = %s",
                (switch_id,)).fetchone()
            if gen is not None and \
                    body["device_generation"] < gen["current_generation"]:
                raise ServiceError(
                    "ack_stale_generation",
                    "acknowledgement targets a generation older than the "
                    "device's accepted generation", 409,
                    {"submitted_generation": body["device_generation"],
                     "current_generation": gen["current_generation"]})

            prior = conn.execute(
                "SELECT * FROM acknowledgements WHERE command_id = %s",
                (command_uuid,)).fetchone()
            if prior is not None:
                # Identical ack repeated: collapse to the first result.
                return {
                    "rollout_id": str(rid),
                    "command_id": str(command_uuid),
                    "step": command["step"],
                    "switch_id": switch_id,
                    "result": "APPLIED",
                    "status": "already_applied",
                    "duplicate": True,
                    "first_acknowledged_at":
                        prior["first_created_at"].isoformat(),
                }, False

            conn.execute(
                "UPDATE device_commands SET status = 'APPLIED', "
                "applied_at = now() WHERE id = %s", (command_uuid,))
            conn.execute(
                "INSERT INTO acknowledgements (rollout_id, command_id, "
                "switch_id, step, plan_digest, device_generation, result) "
                "VALUES (%s,%s,%s,%s,%s,%s,'APPLIED')",
                (rid, command_uuid, switch_id, command["step"],
                 command["plan_digest"], command["device_generation"]))
            _audit(conn, rid, "command_applied", None, {
                "command_id": str(command_uuid), "step": command["step"],
                "switch_id": switch_id,
                "device_generation": command["device_generation"]})

            completed = False
            if command["step"] == rollout["total_steps"] - 1:
                conn.execute(
                    "UPDATE rollouts SET status = 'COMPLETED', "
                    "updated_at = now() WHERE id = %s", (rid,))
                _audit(conn, rid, "rollout_completed", None, {
                    "final_step": command["step"]})
                completed = True

            return {
                "rollout_id": str(rid),
                "command_id": str(command_uuid),
                "step": command["step"],
                "switch_id": switch_id,
                "result": "APPLIED",
                "status": "accepted",
                "duplicate": False,
                "rollout_completed": completed,
            }, True
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def get_rollout(rollout_id: str) -> dict[str, Any]:
    conn = connect()
    try:
        _, row = _get_rollout(conn, rollout_id)
        return _rollout_json(conn, row, detailed=True)
    finally:
        conn.close()


def get_audit_trail(rollout_id: str) -> dict[str, Any]:
    conn = connect()
    try:
        rid, _ = _get_rollout(conn, rollout_id)
        rows = conn.execute(
            "SELECT id, event_type, epoch, payload, created_at "
            "FROM audit_events WHERE rollout_id = %s ORDER BY id",
            (rid,)).fetchall()
        return {
            "rollout_id": str(rid),
            "events": [{
                "seq": r["id"],
                "event_type": r["event_type"],
                "epoch": r["epoch"],
                "payload": r["payload"],
                "created_at": r["created_at"].isoformat(),
            } for r in rows],
        }
    finally:
        conn.close()
