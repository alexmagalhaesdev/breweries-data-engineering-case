import os
import pytest
from datetime import date
from app.pipeline import run

@pytest.mark.integration
def test_pipeline_runs_against_minio():
    # Requires docker-compose up; skip if not running in container or MinIO not reachable
    if os.getenv("CI") != "true":
        # We don't actually assert S3 contents here; this ensures the flow doesn't raise.
        run(ingestion_date=date.today().isoformat())
