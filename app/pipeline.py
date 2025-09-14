# app/pipeline.py
from datetime import date
from prefect import flow, get_run_logger

from app.utils.logging import setup_logging
from app.tasks.extract import ingest_bronze
from app.tasks.silver import transform_silver
from app.tasks.gold import aggregate_gold

@flow(name="breweries_medallion_pipeline", retries=1)
def run(ingestion_date: str | None = None) -> None:
    setup_logging()
    log = get_run_logger()
    ing_date = ingestion_date or date.today().isoformat()

    # Bronze: single task returns (pages_written, records_written)
    total_pages, total_records = ingest_bronze(ing_date)
    log.info(f"Bronze ingest complete: pages={total_pages}, records={total_records}")

    # Silver
    rows = transform_silver(ing_date)
    log.info(f"Silver rows written: {rows}")

    # Gold
    gold_path = aggregate_gold(ing_date)
    log.info(f"Gold written to: {gold_path}")

if __name__ == "__main__":
    run()
