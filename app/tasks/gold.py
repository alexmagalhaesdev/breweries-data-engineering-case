from prefect import task
from app.config import SETTINGS
from app.io.duck import connect

@task(name="aggregate_gold", retries=2, retry_delay_seconds=5)
def aggregate_gold(ing_date: str) -> str:
    con = connect("/data/warehouse.duckdb")
    silver_glob = f"s3://{SETTINGS.silver_bucket}/breweries/*/*/*.parquet"
    con.execute(f"""
        CREATE OR REPLACE VIEW silver_breweries AS
        SELECT * FROM read_parquet('{silver_glob}')
    """)

    con.execute("""
        CREATE OR REPLACE TABLE gold_breweries_counts AS
        SELECT
          COALESCE(country,'')      AS country,
          COALESCE(state,'')        AS state,
          COALESCE(brewery_type,'') AS brewery_type,
          COUNT(*)                  AS brewery_count
        FROM silver_breweries
        GROUP BY ALL
        ORDER BY country, state, brewery_type
    """)

    gold_dir = f"s3://{SETTINGS.gold_bucket}/breweries_counts/ingestion_date={ing_date}"
    con.execute(f"""
        COPY (SELECT * FROM gold_breweries_counts)
        TO '{gold_dir}'
        (FORMAT PARQUET, PARTITION_BY (country, state), OVERWRITE_OR_IGNORE TRUE)
    """)

    return gold_dir
