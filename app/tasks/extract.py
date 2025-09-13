import re
from typing import Optional, Tuple, List, Dict

import requests
from prefect import task

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


@task(name="fetch_first_page_metadata", retries=2, retry_delay_seconds=5)
def fetch_first_page_metadata() -> Tuple[List[Dict], Optional[int]]:
    r = requests.get(
        SETTINGS.api_url,
        params={"per_page": SETTINGS.per_page, "page": 1},
        headers=HEADERS,
        timeout=30,
    )
    r.raise_for_status()
    last = _parse_last_page(r.headers.get("Link"))
    return r.json(), last


@task(name="fetch_page", retries=2, retry_delay_seconds=5)
def fetch_page(page: int) -> List[Dict]:
    r = requests.get(
        SETTINGS.api_url,
        params={"per_page": SETTINGS.per_page, "page": page},
        headers=HEADERS,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


@task(name="persist_bronze")
def persist_bronze(records: List[Dict], page: int, ing_date: str) -> str:
    key = bronze_key(ing_date, page)
    return put_json(SETTINGS.bronze_bucket, key, records)


# re-export for tests
parse_last_page = _parse_last_page
