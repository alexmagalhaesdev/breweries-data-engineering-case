import subprocess
from pathlib import Path

def run_soda_scan() -> int:
    root = Path(__file__).resolve().parents[2]
    cfg = root / "soda" / "configuration.yml"
    checks = root / "soda" / "checks" / "silver_breweries.yml"
    cmd = ["soda", "scan", "-d", "duckdb", "-c", str(cfg), str(checks)]
    return subprocess.call(cmd)
