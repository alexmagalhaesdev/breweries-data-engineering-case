from prefect import task
from app.config import SETTINGS
from app.io.duck import connect

@task(name="transform_silver", retries=2, retry_delay_seconds=5)
def transform_silver(ing_date: str) -> int:
    con = connect()
    bronze_glob = f"s3://{SETTINGS.bronze_bucket}/breweries/ingestion_date={ing_date}/*.json"

    con.execute(f"""
        CREATE OR REPLACE VIEW v_silver_clean AS
        WITH raw AS (
          SELECT * FROM read_json_auto('{bronze_glob}')
        ),
        cleaned AS (
          SELECT
            CAST(id AS VARCHAR)                                  AS id,
            NULLIF(TRIM(name), '')                               AS name,
            NULLIF(TRIM(brewery_type), '')                       AS brewery_type,
            NULLIF(TRIM(country), '')                            AS country,
            NULLIF(TRIM(state), '')                              AS state,
            NULLIF(TRIM(city), '')                               AS city,
            NULLIF(TRIM(postal_code), '')                        AS postal_code,
            TRY_CAST(NULLIF(TRIM(latitude), '') AS DOUBLE)       AS latitude,
            TRY_CAST(NULLIF(TRIM(longitude), '') AS DOUBLE)      AS longitude
          FROM raw
        ),
        dedup AS (
          SELECT * FROM cleaned
          QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1
        )
        SELECT * FROM dedup
    """)

    out_dir = f"s3://{SETTINGS.silver_bucket}/breweries"
    con.execute(f"""
        COPY (SELECT * FROM v_silver_clean)
        TO '{out_dir}'
        (FORMAT PARQUET, PARTITION_BY (country, state), OVERWRITE_OR_IGNORE TRUE)
    """)

    rows = con.execute("SELECT COUNT(*) FROM v_silver_clean").fetchone()[0]
    return rows
