#!/usr/bin/env bash
set -euo pipefail
python - <<'PY'
import os, boto3, botocore
endpoint = os.environ.get("S3_ENDPOINT_URL","http://minio:9000")
s3 = boto3.client("s3", endpoint_url=endpoint)
for b in ("lake-bronze","lake-silver","lake-gold"):
    try:
        s3.create_bucket(Bucket=b); print("Bucket OK:", b)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou","BucketAlreadyExists"):
            raise
PY
