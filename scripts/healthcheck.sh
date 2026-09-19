#!/bin/sh
# Container-local health check: query the API health endpoint.
PORT="${API_PORT:-8080}"
python - "$PORT" <<'PY'
import json
import sys
import urllib.request

port = sys.argv[1]
try:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=3) as r:
        payload = json.loads(r.read().decode())
    if payload.get("status") == "ok":
        sys.exit(0)
except Exception:
    pass
sys.exit(1)
PY
