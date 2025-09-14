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

# Auto-deploy toggle (1 = deploy at start)
AUTO_DEPLOY="${AUTO_DEPLOY:-1}"

trap 'echo "[entrypoint] Signal received, stopping..."; [[ -n "${SERVER_PID:-}" ]] && kill -TERM "$SERVER_PID" 2>/dev/null || true; exit 0' TERM INT

echo "[entrypoint] Waiting for MinIO at ${MINIO_HEALTH_URL}..."
until curl -sSf "$MINIO_HEALTH_URL" >/dev/null; do sleep 1; done
echo "[entrypoint] MinIO ready."

echo "[entrypoint] Ensuring data-lake bucket..."
python - <<'PY'
import os, boto3, botocore
endpoint = os.environ.get("S3_ENDPOINT_URL")
bucket = os.environ.get("LAKE_BUCKET", "breweries-lake")
s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
)
try:
    s3.create_bucket(Bucket=bucket)
    print(f"[entrypoint] Bucket OK: {bucket}")
except botocore.exceptions.ClientError as e:
    if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou","BucketAlreadyExists"):
        raise
print("[entrypoint] Lake bucket ensured.")
PY

echo "[entrypoint] Starting Prefect Server (UI) on ${PREFECT_HOST}:${PREFECT_PORT}..."
prefect server start --host "${PREFECT_HOST}" --port "${PREFECT_PORT}" &
SERVER_PID=$!
echo "[entrypoint] Prefect Server PID=${SERVER_PID}"

echo "[entrypoint] Waiting for Prefect API health at ${PREFECT_API_HEALTH}..."
until curl -sSf "${PREFECT_API_HEALTH}" >/dev/null || curl -sSf "http://localhost:${PREFECT_PORT}/api/health" >/dev/null; do sleep 1; done
echo "[entrypoint] Prefect Server is healthy."

# Route all Prefect calls to the always-on server (avoid ephemeral servers)
export PREFECT_API_URL="http://127.0.0.1:${PREFECT_PORT}/api"
export PREFECT_LOGGING_LEVEL
echo "[entrypoint] PREFECT_API_URL=${PREFECT_API_URL}"

echo "[entrypoint] Ensuring work pool '${WORK_POOL}' (type=process)..."
prefect work-pool create "${WORK_POOL}" -t process >/dev/null 2>&1 || true

# ---- AUTO DEPLOY (manual runs: no schedules) ----
if [ "${AUTO_DEPLOY}" = "1" ]; then
  if [ -f /app/prefect.yaml ]; then
    echo "[entrypoint] Running 'prefect deploy' from /app/prefect.yaml..."
    # Deterministic: deploy everything defined in prefect.yaml without prompts
    prefect deploy --all --prefect-file /app/prefect.yaml || {
      echo "[entrypoint] 'prefect deploy' failed";
    }
  else
    echo "[entrypoint] No prefect.yaml found; registering deployment programmatically..."
    python - <<'PY'
from prefect import flow
from prefect.client.orchestration import get_client
import asyncio
from app.pipeline import run as pipeline_flow

async def main():
    async with get_client() as client:
        try:
            await client.read_work_pool("default")
        except Exception:
            await client.create_work_pool(name="default", type="process")
    await pipeline_flow.deploy(
        name="breweries-medallion-dev",
        work_pool_name="default",
        cron=None,  # manual runs (no schedule)
        tags=["dev"],
        description="Medallion pipeline for breweries (manual runs)",
    )

asyncio.run(main())
PY
  fi
  echo "[entrypoint] Deployments registered:"
  prefect deployments ls || true
else
  echo "[entrypoint] AUTO_DEPLOY disabled; skipping deploy."
fi

echo "[entrypoint] Starting Prefect Worker on pool='${WORK_POOL}' name='${WORKER_NAME}' (foreground)..."
# Keep the worker in the foreground to keep the container alive
prefect worker start -p "${WORK_POOL}" -n "${WORKER_NAME}"
