#!/usr/bin/env bash
set -euo pipefail

# ---------- Config ----------
MINIO_HEALTH_URL="${MINIO_HEALTH_URL:-http://minio:9000/minio/health/ready}"

# Prefect Server (UI)
PREFECT_HOST="${PREFECT_HOST:-0.0.0.0}"
PREFECT_PORT="${PREFECT_PORT:-4200}"
PREFECT_API_HEALTH="${PREFECT_API_HEALTH:-http://127.0.0.1:${PREFECT_PORT}/api/health}"

# Prefect Worker / Work Pool
WORK_POOL="${WORK_POOL:-default}"
WORKER_NAME="${WORKER_NAME:-app-worker}"
PREFECT_LOGGING_LEVEL="${PREFECT_LOGGING_LEVEL:-INFO}"

trap 'echo "[entrypoint] Signal received, stopping..."; [[ -n "${SERVER_PID:-}" ]] && kill -TERM "$SERVER_PID" 2>/dev/null || true; exit 0' TERM INT

echo "[entrypoint] Waiting for MinIO at ${MINIO_HEALTH_URL}..."
until curl -sSf "$MINIO_HEALTH_URL" >/dev/null; do sleep 1; done
echo "[entrypoint] MinIO ready."

echo "[entrypoint] Creating buckets if needed..."
python - <<'PY'
import os, boto3, botocore
s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"])
for b in (os.environ["BRONZE_BUCKET"], os.environ["SILVER_BUCKET"], os.environ["GOLD_BUCKET"]):
    try:
        s3.create_bucket(Bucket=b)
        print(f"[entrypoint] Bucket OK: {b}")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou","BucketAlreadyExists"):
            raise
print("[entrypoint] Buckets ensured.")
PY

echo "[entrypoint] Starting Prefect Server (UI) on ${PREFECT_HOST}:${PREFECT_PORT}..."
prefect server start --host "${PREFECT_HOST}" --port "${PREFECT_PORT}" &
SERVER_PID=$!
echo "[entrypoint] Prefect Server PID=${SERVER_PID}"

echo "[entrypoint] Waiting for Prefect API health at ${PREFECT_API_HEALTH}..."
until curl -sSf "${PREFECT_API_HEALTH}" >/dev/null || curl -sSf "http://localhost:${PREFECT_PORT}/api/health" >/dev/null; do sleep 1; done
echo "[entrypoint] Prefect Server is healthy."

# Route all Prefect calls to the always-on server (avoid ephemeral server)
export PREFECT_API_URL="http://127.0.0.1:${PREFECT_PORT}/api"
export PREFECT_LOGGING_LEVEL
echo "[entrypoint] PREFECT_API_URL=${PREFECT_API_URL}"

echo "[entrypoint] Ensuring work pool '${WORK_POOL}' (type=process)..."
prefect work-pool create "${WORK_POOL}" -t process >/dev/null 2>&1 || true

echo "[entrypoint] Starting Prefect Worker on pool='${WORK_POOL}' name='${WORKER_NAME}'..."
# Run worker in foreground to keep the container alive
prefect worker start -p "${WORK_POOL}" -n "${WORKER_NAME}"
