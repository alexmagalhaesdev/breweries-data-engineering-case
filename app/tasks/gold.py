from typing import Optional, Sequence

import duckdb
from prefect import task

from app.config import SETTINGS
from app.io.duck import connect


# Exports my datas results to the Gold layer (S3/MinIO) as Snappy Parquet
# with UUID filenames and partitions by the given columns.
def _copy_to_gold_layer(
    con: duckdb.DuckDBPyConnection,
    select_sql: str,
    dest_s3_dir: str,
    partition_by: Optional[Sequence[str]] = None,
    compression: str = "snappy",
) -> None:
    part_clause = ""
    if partition_by:
        part_clause = f", PARTITION_BY ({', '.join(partition_by)})"

    con.execute(
        f"""
        COPY ({select_sql})
        TO '{dest_s3_dir}'
        (FORMAT PARQUET,
         FILENAME_PATTERN '{{uuid}}',
         COMPRESSION '{compression}'{part_clause},
         OVERWRITE_OR_IGNORE TRUE)
        """
    )


# Creates or replaces a DuckDB VIEW mirroring the provided SELECT.
def _create_view(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    select_sql: str,
) -> None:
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS {select_sql}")


@task(name="aggregate_gold", retries=2, retry_delay_seconds=5)
def aggregate_gold(ing_date: str) -> str:
    # Local DuckDB file (our small, queryable “mini-warehouse”)
    con = connect("/data/warehouse.duckdb")

    # Paths (single bucket + layer prefixes)
    silver_glob = (
        f"s3://{SETTINGS.lake_bucket}/"
        f"{SETTINGS.silver_prefix}/breweries/ingestion_date={ing_date}/*/*/*.parquet"
    )
    base_dir = (
        f"s3://{SETTINGS.lake_bucket}/"
        f"{SETTINGS.gold_prefix}/breweries_counts/ingestion_date={ing_date}"
    )

    # Clean views from previous runs
    for v in (
        "silver_breweries",
        "v_gold_counts",
        "v_gold_by_country_state_type_run",
        "v_gold_by_country_type_run",
        "v_gold_by_state_type_run",
        "v_gold_by_type_run",
    ):
        con.execute(f"DROP VIEW IF EXISTS {v}")

    # Read Silver and expose as a logical view
    con.sql(f"SELECT * FROM read_parquet('{silver_glob}')").create_view("silver_breweries")

    # Base aggregation (location + type) for this run
    con.execute(
        """
        CREATE OR REPLACE VIEW v_gold_counts AS
        SELECT
          COALESCE(country,'')      AS country,
          COALESCE(state,'')        AS state,
          COALESCE(brewery_type,'') AS brewery_type,
          COUNT(*)                  AS brewery_count
        FROM silver_breweries
        GROUP BY ALL
        ORDER BY country, state, brewery_type
        """
    )

    # History table (append-by-ingestion_date; simple and queryable)
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
    # Idempotency for this run
    con.execute(f"DELETE FROM gold_breweries_counts WHERE ingestion_date = DATE '{ing_date}'")
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

    # ----- Per-run exports (COPY first) and then matching views -----

    # 1) Country, State, Type (no extra aggregation)
    sel_country_state_type = f"""
        SELECT country, state, brewery_type, brewery_count
        FROM gold_breweries_counts
        WHERE ingestion_date = DATE '{ing_date}'
        ORDER BY country, state, brewery_type
    """
    _copy_to_gold_layer(
        con,
        select_sql=sel_country_state_type,
        dest_s3_dir=f"{base_dir}/by_country_state_type",
        partition_by=("country", "state", "brewery_type"),
    )
    _create_view(con, "v_gold_by_country_state_type_run", sel_country_state_type)

    # 2) Country, Type (aggregated)
    sel_country_type = f"""
        SELECT country, brewery_type, SUM(brewery_count) AS brewery_count
        FROM gold_breweries_counts
        WHERE ingestion_date = DATE '{ing_date}'
        GROUP BY ALL
        ORDER BY country, brewery_type
    """
    _copy_to_gold_layer(
        con,
        select_sql=sel_country_type,
        dest_s3_dir=f"{base_dir}/by_country_type",
        partition_by=("country", "brewery_type"),
    )
    _create_view(con, "v_gold_by_country_type_run", sel_country_type)

    # 3) State, Type (aggregated)
    sel_state_type = f"""
        SELECT state, brewery_type, SUM(brewery_count) AS brewery_count
        FROM gold_breweries_counts
        WHERE ingestion_date = DATE '{ing_date}'
        GROUP BY ALL
        ORDER BY state, brewery_type
    """
    _copy_to_gold_layer(
        con,
        select_sql=sel_state_type,
        dest_s3_dir=f"{base_dir}/by_state_type",
        partition_by=("state", "brewery_type"),
    )
    _create_view(con, "v_gold_by_state_type_run", sel_state_type)

    # 4) Type (global, aggregated)
    sel_type = f"""
        SELECT brewery_type, SUM(brewery_count) AS brewery_count
        FROM gold_breweries_counts
        WHERE ingestion_date = DATE '{ing_date}'
        GROUP BY ALL
        ORDER BY brewery_type
    """
    _copy_to_gold_layer(
        con,
        select_sql=sel_type,
        dest_s3_dir=f"{base_dir}/by_type",
        partition_by=("brewery_type",),
    )
    _create_view(con, "v_gold_by_type_run", sel_type)

    return base_dir
