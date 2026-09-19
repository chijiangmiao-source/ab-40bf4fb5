#!/usr/bin/env python3
"""Entry point: wait for PostgreSQL, apply migrations, serve the API."""
from __future__ import annotations

import os

from app.db import run_migrations, wait_for_database
from app.api import create_app


def main() -> None:
    dsn = os.environ.get("DATABASE_URL")
    wait_for_database(dsn)
    run_migrations(dsn)

    port = int(os.environ.get("API_PORT", "8080"))
    workers = int(os.environ.get("GUNICORN_WORKERS", "2"))
    if os.environ.get("USE_FLASK_DEV") == "1":
        app = create_app()
        app.run(host="0.0.0.0", port=port)
        return

    import sys

    from gunicorn.app.wsgiapp import WSGIApplication

    sys.argv = [
        "gunicorn",
        "--bind", f"0.0.0.0:{port}",
        "--worker-class", "gthread",
        "--workers", str(workers),
        "--threads", os.environ.get("GUNICORN_THREADS", "4"),
        "--timeout", "60",
        "--access-logfile", "-",
        "app.wsgi:app",
    ]
    WSGIApplication("%(prog)s [OPTIONS] APP_MODULE").run()


if __name__ == "__main__":
    main()
