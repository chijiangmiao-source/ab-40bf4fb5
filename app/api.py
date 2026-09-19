"""Structured JSON HTTP API."""
from __future__ import annotations

import flask
import psycopg

from . import service
from .planner import PlanError
from .service import ServiceError


def create_app() -> flask.Flask:
    app = flask.Flask(__name__)
    app.url_map.strict_slashes = False

    def error_payload(code: str, message: str, details=None):
        return {
            "error": {
                "code": code,
                "message": message,
                **({"details": details} if details else {}),
            }
        }

    @app.errorhandler(ServiceError)
    def _service_error(exc: ServiceError):
        return flask.jsonify(error_payload(
            exc.code, exc.message, exc.details or None)), exc.http_status

    @app.errorhandler(PlanError)
    def _plan_error(exc: PlanError):
        return flask.jsonify(error_payload(
            exc.code, exc.message, exc.details or None)), exc.http_status

    @app.errorhandler(Exception)
    def _unexpected(exc: Exception):
        # Let Flask/Werkzeug HTTP errors (404/405/...) reach their handlers.
        if isinstance(exc, Exception) and hasattr(exc, "code") and \
                isinstance(getattr(exc, "code", None), int) and exc.code < 500:
            raise exc
        if isinstance(exc, psycopg.Error):
            flask.current_app.logger.exception("database error")
            return flask.jsonify(error_payload(
                "database_error", "a database error occurred")), 500
        flask.current_app.logger.exception("unhandled error")
        return flask.jsonify(error_payload(
            "internal_error", "internal server error")), 500

    def json_body():
        if not flask.request.is_json:
            raise ServiceError(
                "invalid_content_type",
                "requests with a body must use Content-Type: application/json",
                415)
        try:
            data = flask.request.get_json(silent=False)
        except Exception:
            raise ServiceError("invalid_json", "request body is not valid JSON",
                               400)
        return data

    # -- Health -------------------------------------------------------------
    @app.get("/health")
    def health():
        from .db import connect
        try:
            conn = connect()
            conn.execute("SELECT 1")
            conn.close()
        except Exception:
            return flask.jsonify({"status": "unhealthy",
                                  "database": "unreachable"}), 503
        return flask.jsonify({"status": "ok", "database": "ok"})

    # -- Plans --------------------------------------------------------------
    @app.post("/api/plans")
    def create_plan():
        body = json_body()
        result, created = service.create_plan(body, flask.request.headers)
        return flask.jsonify(result), (201 if created else 200)

    @app.get("/api/plans/<plan_id>")
    def get_plan(plan_id):
        return flask.jsonify(service.get_plan(plan_id))

    # -- Rollouts -----------------------------------------------------------
    @app.post("/api/rollouts")
    def create_rollout():
        body = json_body()
        result, created = service.create_rollout(body, flask.request.headers)
        return flask.jsonify(result), (201 if created else 200)

    @app.get("/api/rollouts/<rollout_id>")
    def get_rollout(rollout_id):
        return flask.jsonify(service.get_rollout(rollout_id))

    @app.get("/api/rollouts/<rollout_id>/audit")
    def get_audit(rollout_id):
        return flask.jsonify(service.get_audit_trail(rollout_id))

    # -- Coordinator --------------------------------------------------------
    @app.post("/api/rollouts/<rollout_id>/coordinator/acquire")
    def acquire(rollout_id):
        body = json_body()
        result, first = service.acquire_lease(
            rollout_id, body, flask.request.headers)
        return flask.jsonify(result), (200 if not first else 200)

    @app.post("/api/rollouts/<rollout_id>/coordinator/renew")
    def renew(rollout_id):
        body = json_body()
        result, _ = service.renew_lease(rollout_id, body, flask.request.headers)
        return flask.jsonify(result)

    @app.post("/api/rollouts/<rollout_id>/advance")
    def advance(rollout_id):
        body = json_body()
        result, _ = service.advance(rollout_id, body, flask.request.headers)
        return flask.jsonify(result)

    # -- Devices ------------------------------------------------------------
    @app.get("/api/rollouts/<rollout_id>/devices/<switch_id>/command")
    def get_command(rollout_id, switch_id):
        return flask.jsonify(
            service.get_pending_command(rollout_id, switch_id))

    @app.post("/api/rollouts/<rollout_id>/devices/<switch_id>/ack")
    def submit_ack(rollout_id, switch_id):
        body = json_body()
        # The path switch id and body must agree when the body carries one.
        if "switch_id" in body and body["switch_id"] != switch_id:
            raise ServiceError("ack_switch_mismatch",
                               "path switch_id does not match body switch_id",
                               409, {"path": switch_id,
                                     "body": body["switch_id"]})
        body.setdefault("switch_id", switch_id)
        result, first = service.submit_acknowledgement(
            rollout_id, body, flask.request.headers)
        return flask.jsonify(result), (200 if not first else 200)

    @app.errorhandler(404)
    def _not_found(_):
        return flask.jsonify(error_payload(
            "not_found", "the requested resource or route does not exist")), 404

    @app.errorhandler(405)
    def _method(_):
        return flask.jsonify(error_payload(
            "method_not_allowed", "HTTP method not allowed for this route")), 405

    return app
