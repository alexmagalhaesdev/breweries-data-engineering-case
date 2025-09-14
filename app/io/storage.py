import json
import uuid
import boto3
from app.config import SETTINGS

s3 = boto3.client(
    "s3",
    endpoint_url=SETTINGS.s3_endpoint,
    aws_access_key_id=SETTINGS.aws_key,
    aws_secret_access_key=SETTINGS.aws_secret,
    region_name=getattr(SETTINGS, "aws_region", "us-east-1"),
)

def bronze_key(ingestion_date: str, page: int) -> str:
    u = uuid.uuid4().hex
    # s3://{bucket}/{bronze_prefix}/breweries/ingestion_date=YYYY-MM-DD/page_{N}_{UUID}.json
    return (
        f"{SETTINGS.bronze_prefix}/breweries/"
        f"ingestion_date={ingestion_date}/page_{page}_{u}.json"
    )

def put_json(key: str, obj) -> str:
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=SETTINGS.lake_bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    return f"s3://{SETTINGS.lake_bucket}/{key}"
