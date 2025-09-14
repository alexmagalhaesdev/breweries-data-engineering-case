import os
from pydantic import BaseModel

class Settings(BaseModel):
    s3_endpoint: str = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
    aws_key: str = os.getenv("AWS_ACCESS_KEY_ID", "minio")
    aws_secret: str = os.getenv("AWS_SECRET_ACCESS_KEY", "minio12345")
    aws_region: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    lake_bucket: str = os.getenv("LAKE_BUCKET", "breweries-lake")
    bronze_bucket: str = os.getenv("BRONZE_BUCKET", lake_bucket)
    silver_bucket: str = os.getenv("SILVER_BUCKET", lake_bucket)
    gold_bucket:   str = os.getenv("GOLD_BUCKET",   lake_bucket)

    bronze_prefix: str = os.getenv("LAYER_PREFIX_BRONZE", "bronze-layer")
    silver_prefix: str = os.getenv("LAYER_PREFIX_SILVER", "silver-layer")
    gold_prefix:   str = os.getenv("LAYER_PREFIX_GOLD",   "gold-layer")

    api_url: str = os.getenv("API_URL", "https://api.openbrewerydb.org/v1/breweries")
    per_page: int = int(os.getenv("PER_PAGE", "200"))

SETTINGS = Settings()
