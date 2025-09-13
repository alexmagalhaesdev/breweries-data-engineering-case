import duckdb
from app.config import SETTINGS

def connect(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=db_path)
    # Configure DuckDB to reach MinIO/S3
    endpoint = SETTINGS.s3_endpoint.replace("http://", "").replace("https://", "")
    con.execute(f"SET s3_endpoint='{endpoint}'")
    con.execute(f"SET s3_access_key_id='{SETTINGS.aws_key}'")
    con.execute(f"SET s3_secret_access_key='{SETTINGS.aws_secret}'")
    con.execute("SET s3_use_ssl=false")
    con.execute("SET s3_url_style='path'")
    return con
