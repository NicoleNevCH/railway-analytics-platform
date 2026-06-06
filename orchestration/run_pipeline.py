"""
End-to-end orchestrator (run ON THE HOST).

Chains the pipeline steps once the stack is already up (`docker compose up -d`):

    1. SIMULATE -> fires the chaos simulator against the ingestion API
                   (generates Bronze + sends nulls to the DLQ).
    2. PROCESS  -> runs the Spark job (Bronze -> Silver/Iceberg) via
                   `docker compose exec spark-master spark-submit ...`.
    3. QUERY    -> refreshes the DuckDB view and prints the punctuality KPIs
                   from the consumption API.

Examples:
    python orchestration/run_pipeline.py                  # everything, defaults
    python orchestration/run_pipeline.py --trips 200      # more trips
    python orchestration/run_pipeline.py --skip-simulate  # only reprocess
    python orchestration/run_pipeline.py --only-query     # only query the KPIs
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request


SPARK_SERVICE = "spark-master"
SPARK_JOB_PATH = "/opt/bitnami/spark/jobs/bronze_to_silver.py"
INGESTION_URL = "http://localhost:8000"
CONSUMPTION_URL = "http://localhost:8001"


def compose_cmd() -> list[str]:
    """Detect whether it is `docker compose` (v2) or `docker-compose` (v1)."""
    if shutil.which("docker"):
        return ["docker", "compose"]
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    print("ERROR: neither `docker` nor `docker-compose` found on PATH.", file=sys.stderr)
    sys.exit(2)


def run(cmd: list[str]) -> None:
    print(f"\n$ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def _request_json(url: str, method: str, retries: int, delay: float) -> dict:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, method=method)
            if method == "POST":
                req.data = b""  # empty body just to characterize the POST
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError) as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(delay)
    raise SystemExit(f"Failed to call {url}: {last_err}")


def http_get_json(url: str, retries: int = 1, delay: float = 2.0) -> dict:
    return _request_json(url, "GET", retries, delay)


def http_post_json(url: str, retries: int = 1, delay: float = 2.0) -> dict:
    return _request_json(url, "POST", retries, delay)


def step_simulate(args: argparse.Namespace) -> None:
    print("\n=== [1/3] SIMULATING events (chaos injection) ===")
    cmd = [
        sys.executable, "-m", "simulator.chaos_simulator",
        "--api-url", INGESTION_URL,
        "--trips", str(args.trips),
        "--updates-per-trip", str(args.updates_per_trip),
        "--null-rate", str(args.null_rate),
        "--time-mess-rate", str(args.time_mess_rate),
        "--delayed-rate", str(args.delayed_rate),
        "--seed", str(args.seed),
    ]
    run(cmd)


def step_process(compose: list[str]) -> None:
    print("\n=== [2/3] PROCESSING on Spark (Bronze -> Silver/Iceberg) ===")
    run(compose + ["exec", "-T", SPARK_SERVICE, "spark-submit", SPARK_JOB_PATH])


def step_query() -> None:
    print("\n=== [3/3] QUERYING KPIs (DuckDB over Iceberg) ===")
    # Ensure the DuckDB view sees the most recent metadata.
    http_post_json(f"{CONSUMPTION_URL}/refresh", retries=5, delay=3.0)
    stats = http_get_json(f"{CONSUMPTION_URL}/stats/punctuality", retries=3)
    print(json.dumps(stats, indent=2, ensure_ascii=False))


def main() -> None:
    p = argparse.ArgumentParser(description="End-to-end lakehouse orchestrator.")
    p.add_argument("--trips", type=int, default=120)
    p.add_argument("--updates-per-trip", type=int, default=3)
    p.add_argument("--null-rate", type=float, default=0.12)
    p.add_argument("--time-mess-rate", type=float, default=0.10)
    p.add_argument("--delayed-rate", type=float, default=0.35)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--skip-simulate", action="store_true", help="do not generate new events")
    p.add_argument("--only-query", action="store_true", help="only query the KPIs")
    args = p.parse_args()

    compose = compose_cmd()

    if args.only_query:
        step_query()
        return

    if not args.skip_simulate:
        step_simulate(args)

    step_process(compose)
    step_query()
    print("\nPipeline complete. Explore more at http://localhost:8001/docs")


if __name__ == "__main__":
    main()
