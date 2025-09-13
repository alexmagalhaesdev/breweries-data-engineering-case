import os
import pytest

@pytest.fixture(scope="session", autouse=True)
def _env_defaults():
    os.environ.setdefault("S3_ENDPOINT_URL", "http://minio:9000")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "minio")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minio12345")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ.setdefault("BRONZE_BUCKET", "lake-bronze")
    os.environ.setdefault("SILVER_BUCKET", "lake-silver")
    os.environ.setdefault("GOLD_BUCKET", "lake-gold")
