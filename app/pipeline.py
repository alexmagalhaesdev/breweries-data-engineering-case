from datetime import date
from prefect import flow, get_run_logger
from app.utils.logging import setup_logging
from app.tasks.extract import fetch_first_page_metadata, fetch_page, persist_bronze
from app.tasks.silver import transform_silver
from app.tasks.gold import aggregate_gold

@flow(name="breweries_medallion_pipeline", retries=1)
def run(ingestion_date: str | None = None) -> None:
    setup_logging()
    log = get_run_logger()
    ing_date = ingestion_date or date.today().isoformat()

    first_page, last = fetch_first_page_metadata()
    # Persist page 1
    persist_bronze(first_page, 1, ing_date)

    # Deterministic pagination: 2..last (no while True)
    for p in range(2, last + 1):
        data = fetch_page(p)
        if not data:
            break
        persist_bronze(data, p, ing_date)

    rows = transform_silver(ing_date)
    log.info(f"Silver rows written: {rows}")
    gold_path = aggregate_gold(ing_date)
    log.info(f"Gold written to: {gold_path}")

if __name__ == "__main__":
    run()
