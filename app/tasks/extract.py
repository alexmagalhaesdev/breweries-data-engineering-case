# app/tasks/extract.py
import re
from typing import Optional, List, Dict, Tuple

import requests
from prefect import task, get_run_logger

from app.config import SETTINGS
from app.io.storage import bronze_key, put_json

HEADERS = {
    "User-Agent": "breweries-data-engineering-case/1.0 (+github.com/alexmagalhaesdev)",
    "Accept": "application/json",
}


def _parse_last_page(link_header: Optional[str]) -> Optional[int]:
    if not link_header:
        return None
    parts = [p.strip() for p in link_header.split(",")]
    for p in parts:
        if 'rel="last"' in p:
            m = re.search(r"[?&]page=(\d+)", p)
            if m:
                return int(m.group(1))
    return None


@task(name="ingest_bronze", retries=2, retry_delay_seconds=5)
def ingest_bronze(ing_date: str) -> Tuple[int, int]:
    """
    Single task that fetches ALL pages and writes each page to Bronze as JSON.
    Returns (pages_written, records_written).
    """
    log = get_run_logger()
    per_page = SETTINGS.per_page
    url = SETTINGS.api_url
    max_pages_cap = 10000

    # Page 1 + try to discover 'last' via Link header
    r = requests.get(url, params={"per_page": per_page, "page": 1}, headers=HEADERS, timeout=30)
    r.raise_for_status()
    first_page = r.json()
    last = _parse_last_page(r.headers.get("Link"))
    put_json(bronze_key(ing_date, 1), first_page)

    pages_written = 1
    records_written = len(first_page)
    log.info(f"[bronze] page=1 size={len(first_page)} last={last}")

    if isinstance(last, int) and last >= 2:
        # Deterministic range when 'last' is present
        for p in range(2, min(last, max_pages_cap) + 1):
            r = requests.get(url, params={"per_page": per_page, "page": p}, headers=HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            put_json(bronze_key(ing_date, p), data)
            pages_written += 1
            records_written += len(data)
    else:
        # Fallback: stop when the page size drops below per_page (or empty)
        for p in range(2, max_pages_cap + 1):
            r = requests.get(url, params={"per_page": per_page, "page": p}, headers=HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            put_json(bronze_key(ing_date, p), data)
            pages_written += 1
            records_written += len(data)
            if len(data) < per_page:
                break

    log.info(f"[bronze] done pages={pages_written} records={records_written}")
    return pages_written, records_written

# Re-export for unit tests
parse_last_page = _parse_last_page