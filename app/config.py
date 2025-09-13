import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    # Storage
    s3_endpoint: str = os.environ.get("S3_ENDPOINT_URL", "http://minio:9000")
    aws_key: str = os.environ.get("AWS_ACCESS_KEY_ID", "minio")
    aws_secret: str = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio12345")
    region: str = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    bronze_bucket: str = os.environ.get("BRONZE_BUCKET", "lake-bronze")
    silver_bucket: str = os.environ.get("SILVER_BUCKET", "lake-silver")
    gold_bucket: str = os.environ.get("GOLD_BUCKET", "lake-gold")

    # API
    api_url: str = os.environ.get("BREWERIES_API_URL", "https://api.openbrewerydb.org/v1/breweries")
    per_page: int = int(os.environ.get("API_PER_PAGE", "200"))

SETTINGS = Settings()
