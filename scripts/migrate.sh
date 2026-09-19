#!/bin/sh
# One-shot migration service (also usable standalone).
set -e
python - <<'PY'
import os

from app.db import run_migrations, wait_for_database

dsn = os.environ.get("DATABASE_URL")
wait_for_database(dsn)
run_migrations(dsn)
PY
