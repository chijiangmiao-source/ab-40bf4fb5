"""Shared test fixtures.

Two modes:
* Default (local): talks to PostgreSQL via ``DATABASE_URL`` (a dedicated test
  database), applies migrations, and uses the Flask in-process test client.
* Docker ``verify``: when ``API1_URL`` / ``API2_URL`` are set, tests issue
  real HTTP requests against two independently running API containers that
  share one database.
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
import uuid

import pytest

LOCAL_BASE = "local://flask"


def _http_request(method, url, body=None, headers=None):
    data = None
    req_headers = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode()
        req_headers.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, headers=req_headers,
                                 method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = resp.read().decode()
            return resp.status, json.loads(payload) if payload else {}
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode()
        return exc.code, json.loads(payload) if payload else {}


class ApiClient:
    def __init__(self, base_url, flask_client=None):
        self.base_url = base_url
        self._flask = flask_client

    def request(self, method, path, body=None, headers=None):
        if self._flask is not None:
            fn = {"GET": self._flask.get, "POST": self._flask.post}[method]
            kwargs = {}
            if body is not None:
                kwargs["json"] = body
            if headers:
                kwargs["headers"] = headers
            resp = fn(path, **kwargs)
            payload = resp.get_json()
            return resp.status_code, (payload if payload is not None else {})
        return _http_request(method, self.base_url + path, body, headers)

    def get(self, path, headers=None):
        return self.request("GET", path, None, headers)

    def post(self, path, body, headers=None):
        return self.request("POST", path, body, headers)


def _wait_for_http(base):
    for _ in range(60):
        try:
            status, _ = _http_request("GET", base + "/health")
            if status == 200:
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError(f"API at {base} never became healthy")


@pytest.fixture(scope="session")
def api1():
    url1 = os.environ.get("API1_URL")
    if url1:
        _wait_for_http(url1)
        yield ApiClient(url1)
    else:
        os.environ.setdefault(
            "DATABASE_URL",
            "postgresql://postgres@127.0.0.1:5432/migration_test")
        from app.db import run_migrations, wait_for_database

        wait_for_database()
        run_migrations()
        from app.api import create_app

        app = create_app()
        app.testing = True
        yield ApiClient(LOCAL_BASE, app.test_client())


@pytest.fixture(scope="session")
def api2(api1):
    url2 = os.environ.get("API2_URL")
    if url2:
        _wait_for_http(url2)
        yield ApiClient(url2)
    else:
        # Locally the second "instance" is another app client on the same
        # shared database.
        from app.api import create_app

        app = create_app()
        app.testing = True
        yield ApiClient(LOCAL_BASE, app.test_client())


@pytest.fixture
def run_concurrent():
    def _run(target, n=8):
        return run_threads(target, n)
    return _run


@pytest.fixture
def db_conn():
    """Direct connection to the shared database (available in both modes)."""
    import psycopg
    from psycopg.rows import dict_row

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        dsn = "postgresql://postgres@127.0.0.1:5432/migration_test"
        os.environ["DATABASE_URL"] = dsn
    conn = psycopg.connect(dsn, row_factory=dict_row, autocommit=False)
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


@pytest.fixture
def unique_key():
    return lambda prefix="k": f"{prefix}-{uuid.uuid4()}"


def run_threads(target, n=8):
    errors = []
    results = [None] * n

    def worker(i):
        try:
            results[i] = target(i)
        except Exception as exc:  # pragma: no cover - surfaced via assert
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors
    return results
