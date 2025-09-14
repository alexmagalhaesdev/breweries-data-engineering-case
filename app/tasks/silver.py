from prefect import task
from app.config import SETTINGS
from app.io.duck import connect

@task(name="transform_silver", retries=2, retry_delay_seconds=5)
def transform_silver(ing_date: str) -> int:
    con = connect()

    bronze_glob = (
        f"s3://{SETTINGS.lake_bucket}/{SETTINGS.bronze_prefix}/breweries/"
        f"ingestion_date={ing_date}/*.json"
    )

    # Replace the view if it already exists
    con.execute("DROP VIEW IF EXISTS v_silver_clean")

    rel = con.sql(f"""
        WITH raw AS (
          SELECT * FROM read_json_auto('{bronze_glob}')
        ),
        cleaned AS (
          SELECT
            CAST(id AS VARCHAR)                             AS id,
            NULLIF(TRIM(CAST(name AS VARCHAR)), '')         AS name,
            NULLIF(TRIM(CAST(brewery_type AS VARCHAR)), '') AS brewery_type,
            NULLIF(TRIM(CAST(country AS VARCHAR)), '')      AS country,
            COALESCE(
              NULLIF(TRIM(CAST(state AS VARCHAR)), ''),
              NULLIF(TRIM(CAST(state_province AS VARCHAR)), '')
            )                                              AS state,
            NULLIF(TRIM(CAST(city AS VARCHAR)), '')        AS city,
            NULLIF(TRIM(CAST(postal_code AS VARCHAR)), '') AS postal_code,
            TRY_CAST(NULLIF(TRIM(CAST(latitude  AS VARCHAR)), '') AS DOUBLE)  AS latitude,
            TRY_CAST(NULLIF(TRIM(CAST(longitude AS VARCHAR)), '') AS DOUBLE)  AS longitude
          FROM raw
        ),
        dedup AS (
          SELECT *
          FROM cleaned
          QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) = 1
        ),
        enforced AS (
          SELECT *
          FROM dedup
          WHERE id IS NOT NULL
            AND name IS NOT NULL
            AND country IS NOT NULL
            AND state IS NOT NULL
            AND (latitude  IS NULL OR latitude  BETWEEN -90  AND 90)
            AND (longitude IS NULL OR longitude BETWEEN -180 AND 180)
        )
        SELECT * FROM enforced
    """)
    rel.create_view("v_silver_clean")

    out_root = (
        f"s3://{SETTINGS.lake_bucket}/{SETTINGS.silver_prefix}/breweries/"
        f"ingestion_date={ing_date}"
    )

    con.execute(f"""
        COPY (SELECT * FROM v_silver_clean)
        TO '{out_root}'
        (
          FORMAT PARQUET,
          PARTITION_BY (country, state),
          FILENAME_PATTERN '{{uuid}}',
          COMPRESSION 'snappy',
          OVERWRITE_OR_IGNORE TRUE
        )
    """)

    rows = con.sql("SELECT COUNT(*) FROM v_silver_clean").fetchone()[0]
    return rows
