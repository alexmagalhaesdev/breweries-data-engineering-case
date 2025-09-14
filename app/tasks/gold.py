from prefect import task
from app.config import SETTINGS
from app.io.duck import connect


@task(name="aggregate_gold", retries=2, retry_delay_seconds=5)
def aggregate_gold(ing_date: str) -> str:
    con = connect("/data/warehouse.duckdb")

    silver_glob = (
        f"s3://{SETTINGS.silver_bucket}/{SETTINGS.silver_prefix}/"
        f"ingestion_date={ing_date}/*/*/*.parquet"
    )
    base_prefix = f"{SETTINGS.gold_prefix}/breweries_counts/ingestion_date={ing_date}"
    base_dir = f"s3://{SETTINGS.gold_bucket}/{base_prefix}"

    # Ensure clean views
    con.execute("DROP VIEW IF EXISTS silver_breweries")
    con.execute("DROP VIEW IF EXISTS v_gold_counts")

    # Read Silver
    con.sql(f"SELECT * FROM read_parquet('{silver_glob}')").create_view("silver_breweries")

    # Aggregation relation (location + type) -> temp view
    con.sql(
        """
        SELECT
          COALESCE(country,'')      AS country,
          COALESCE(state,'')        AS state,
          COALESCE(brewery_type,'') AS brewery_type,
          COUNT(*)                  AS brewery_count
        FROM silver_breweries
        GROUP BY ALL
        ORDER BY country, state, brewery_type
        """
    ).create_view("v_gold_counts")

    # Final queryable table with history by ingestion_date
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS gold_breweries_counts (
          ingestion_date DATE,
          country        VARCHAR,
          state          VARCHAR,
          brewery_type   VARCHAR,
          brewery_count  BIGINT
        )
        """
    )

    # Idempotency for this ingestion_date
    con.execute(f"DELETE FROM gold_breweries_counts WHERE ingestion_date = DATE '{ing_date}'")

    # Insert current run
    con.execute(
        f"""
        INSERT INTO gold_breweries_counts
        SELECT
          DATE '{ing_date}' AS ingestion_date,
          country,
          state,
          brewery_type,
          brewery_count
        FROM v_gold_counts
        """
    )

    # 1) country, state, type
    con.execute(
        f"""
        COPY (
          SELECT country, state, brewery_type, brewery_count
          FROM gold_breweries_counts
          WHERE ingestion_date = DATE '{ing_date}'
        )
        TO '{base_dir}/by_country_state_type'
        (
          FORMAT PARQUET,
          PARTITION_BY (country, state),
          FILENAME_PATTERN '{{uuid}}',
          COMPRESSION 'snappy',
          OVERWRITE_OR_IGNORE TRUE
        )
        """
    )

    # 2) country, type
    con.execute(
        f"""
        COPY (
          SELECT country, brewery_type, SUM(brewery_count) AS brewery_count
          FROM gold_breweries_counts
          WHERE ingestion_date = DATE '{ing_date}'
          GROUP BY ALL
          ORDER BY country, brewery_type
        )
        TO '{base_dir}/by_country_type'
        (
          FORMAT PARQUET,
          PARTITION_BY (country),
          FILENAME_PATTERN '{{uuid}}',
          COMPRESSION 'snappy',
          OVERWRITE_OR_IGNORE TRUE
        )
        """
    )

    # 3) state, type
    con.execute(
        f"""
        COPY (
          SELECT state, brewery_type, SUM(brewery_count) AS brewery_count
          FROM gold_breweries_counts
          WHERE ingestion_date = DATE '{ing_date}'
          GROUP BY ALL
          ORDER BY state, brewery_type
        )
        TO '{base_dir}/by_state_type'
        (
          FORMAT PARQUET,
          PARTITION_BY (state),
          FILENAME_PATTERN '{{uuid}}',
          COMPRESSION 'snappy',
          OVERWRITE_OR_IGNORE TRUE
        )
        """
    )

    # 4) type (global)
    con.execute(
        f"""
        COPY (
          SELECT brewery_type, SUM(brewery_count) AS brewery_count
          FROM gold_breweries_counts
          WHERE ingestion_date = DATE '{ing_date}'
          GROUP BY ALL
          ORDER BY brewery_type
        )
        TO '{base_dir}/by_type'
        (
          FORMAT PARQUET,
          FILENAME_PATTERN '{{uuid}}',
          COMPRESSION 'snappy',
          OVERWRITE_OR_IGNORE TRUE
        )
        """
    )

    return base_dir
