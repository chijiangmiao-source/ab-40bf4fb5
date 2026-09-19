"""PostgreSQL connection management and migration runner."""
from __future__ import annotations

import os
import pathlib

import psycopg
from psycopg.rows import dict_row

MIGRATIONS_DIR = pathlib.Path(__file__).resolve().parent.parent / "migrations"


def database_dsn() -> str:
    dsn = os.environ.get("DATABASE_URL") or os.environ.get("DATABASE_DSN")
    if dsn:
        return dsn
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'db')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'migration')} "
        f"user={os.environ.get('POSTGRES_USER', 'migration')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'migration')}"
    )


def connect(dsn: str | None = None, *, autocommit: bool = False):
    conn = psycopg.connect(dsn or database_dsn(), row_factory=dict_row)
    conn.autocommit = autocommit
    return conn


def wait_for_database(dsn: str | None = None, attempts: int = 60,
                      delay: float = 1.0) -> None:
    """Block until PostgreSQL accepts connections (used at container start)."""
    import time

    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            conn = connect(dsn, autocommit=True)
            conn.execute("SELECT 1")
            conn.close()
            return
        except Exception as exc:  # pragma: no cover - startup race only
            last_error = exc
            time.sleep(delay)
    raise RuntimeError(f"database not reachable: {last_error}")


def run_migrations(dsn: str | None = None) -> None:
    conn = connect(dsn, autocommit=True)
    try:
        # Cross-process lock so concurrently starting API containers apply
        # migrations one at a time.
        conn.execute("SELECT pg_advisory_lock(91726354)")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "version INTEGER PRIMARY KEY, "
            "applied_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        applied = {
            row["version"]
            for row in conn.execute("SELECT version FROM schema_migrations")
        }
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            version = int(path.name.split("_", 1)[0])
            if version in applied:
                continue
            sql = path.read_text(encoding="utf-8")
            conn.execute(sql)
            conn.execute(
                "INSERT INTO schema_migrations(version) VALUES (%s) "
                "ON CONFLICT (version) DO NOTHING", (version,))
        conn.execute("SELECT pg_advisory_unlock(91726354)")
    finally:
        conn.close()
