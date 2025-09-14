# tests/conftest.py
import os
import pytest

@pytest.fixture(scope="session", autouse=True)
def _env_defaults():
    # --- MinIO / S3 ---
    os.environ.setdefault("S3_ENDPOINT_URL", "http://minio:9000")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "minio")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minio12345")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

    # --- Lake layout: single bucket + layer prefixes ---
    os.environ.setdefault("LAKE_BUCKET", "breweries-data-lake")
    os.environ.setdefault("LAYER_PREFIX_BRONZE", "bronze-layer")
    os.environ.setdefault("LAYER_PREFIX_SILVER", "silver-layer")
    os.environ.setdefault("LAYER_PREFIX_GOLD", "gold-layer")

    # --- Prefect: run flows locally without any API/server during tests ---
    os.environ.setdefault("PREFECT_LOGGING_TO_API", "false")
    os.environ.setdefault("PREFECT_LOGGING_LEVEL", "WARNING")
    os.environ.setdefault("PREFECT_API_ENABLE_EPHEMERAL_SERVER", "false")
    # Ensure no accidental connection to a real Prefect API during tests
    os.environ.setdefault("PREFECT_API_URL", "")

    os.environ.setdefault("PREFECT_LOGGING__TO_API__ENABLED", "false")
    os.environ.setdefault("PREFECT_SERVER__LOGGING__TO_API__ENABLED", "false")

    os.environ.setdefault("PREFECT_UI_ENABLED", "false")
    os.environ.setdefault("PREFECT_API_SERVICES_ENABLED", "false")
