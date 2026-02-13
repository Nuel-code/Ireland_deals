from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path


def run_step(cmd: list[str]) -> int:
    print(f"\n=== RUN: {' '.join(cmd)} ===")
    p = subprocess.run(cmd, text=True)
    return p.returncode


def main() -> int:
    # Daily pipeline must not depend on Overpass unless store files missing
    stores_csv = Path("stores_with_websites.csv")
    stores_dublin_csv = Path("stores_dublin.csv")

    if not stores_csv.exists() or not stores_dublin_csv.exists():
        print("[warn] Store cache files missing. Attempting OSM discovery as fallback...")
        rc = run_step([sys.executable, "src/osm_discover.py"])
        if rc != 0:
            print("[warn] OSM discovery failed. Continuing (pipeline may be empty).")

    rc = run_step([sys.executable, "src/promo_discover.py"])
    if rc != 0:
        print("[warn] promo_discover failed. Continuing to attempt extraction/export.")

    rc = run_step([sys.executable, "src/extract_deals.py"])
    if rc != 0:
        print("[warn] extract_deals failed. Continuing to attempt export.")

    rc = run_step([sys.executable, "src/export_feed.py"])
    if rc != 0:
        print("[warn] export_feed failed.")

    # Never hard-fail the job: publishing commit step will handle "no file changed"
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
