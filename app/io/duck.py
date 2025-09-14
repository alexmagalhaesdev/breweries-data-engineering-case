import os
import duckdb
from urllib.parse import urlparse
from app.config import SETTINGS

def connect(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    if db_path and db_path != ":memory:":
        os.makedirs(os.path.dirname(db_path), exist_ok=True)  # to ensure my data folder 

    con = duckdb.connect(database=(db_path or ":memory:"))

    p = urlparse(SETTINGS.s3_endpoint)
    scheme = (p.scheme or "http").lower()
    host = p.hostname or "minio"
    port = p.port or (443 if scheme == "https" else 9000)
    use_ssl = "true" if scheme == "https" else "false"
    region = getattr(SETTINGS, "aws_region", "us-east-1")

    con.execute(f"SET s3_endpoint='{host}:{port}'")
    con.execute(f"SET s3_use_ssl={use_ssl}")
    con.execute("SET s3_url_style='path'")
    con.execute(f"SET s3_region='{region}'")
    con.execute(f"SET s3_access_key_id='{SETTINGS.aws_key}'")
    con.execute(f"SET s3_secret_access_key='{SETTINGS.aws_secret}'")
    con.execute("SET enable_object_cache=true")
    return con
