import json
from typing import Iterable, Optional
import boto3
from botocore.client import Config
from . import duck  # for path helpers that align with DuckDB S3 config
from app.config import SETTINGS

_session = boto3.session.Session()
_s3 = _session.client(
    "s3",
    endpoint_url=SETTINGS.s3_endpoint,
    region_name=SETTINGS.region,
    aws_access_key_id=SETTINGS.aws_key,
    aws_secret_access_key=SETTINGS.aws_secret,
    config=Config(s3={"addressing_style": "path"}),
)

def bronze_key(ingestion_date: str, page: int) -> str:
    return f"breweries/ingestion_date={ingestion_date}/page={page:04d}.json"

def put_json(bucket: str, key: str, records: Iterable[dict]) -> str:
    body = json.dumps(list(records)).encode("utf-8")
    _s3.put_object(Bucket=bucket, Key=key, Body=body)
    return f"s3://{bucket}/{key}"

def list_keys(bucket: str, prefix: str) -> list[str]:
    paginator = _s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for c in page.get("Contents", []):
            keys.append(c["Key"])
    return keys

def exists(bucket: str, key: str) -> bool:
    try:
        _s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False
