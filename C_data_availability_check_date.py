#!/usr/bin/env python3
"""
C_data_availability_check_date.py

For each sensor under /export/data2/tame-insitu/quantaq/cloud/2025,
prints its latest valid timestamp as soon as it’s determined,
with per‑sensor status messages and robust error handling.
"""

import os
import re
import pandas as pd

BASE_DIR = "/export/data2/tame-insitu/quantaq/cloud"
YEAR     = "2025"
FNAME_RE = re.compile(rf"{YEAR}-.*-MOD(\d+)final\.csv")

def main():
    print("→ Starting data‑availability check")
    # list sensors
    try:
        sensors = sorted(os.listdir(BASE_DIR))
    except Exception as e:
        print(f"ERROR: Cannot list sensors in {BASE_DIR} ({e})")
        return

    for sensor in sensors:
        print(f"Checking sensor {sensor}…", end=" ")
        year_dir = os.path.join(BASE_DIR, sensor, YEAR)
        if not os.path.isdir(year_dir):
            print("no data directory → skipping")
            continue

        # find all final CSVs
        files = [f for f in os.listdir(year_dir) if FNAME_RE.match(f)]
        if not files:
            print("no final CSVs → skipping")
            continue

        last_ts = None
        for fn in sorted(files):
            path = os.path.join(year_dir, fn)
            try:
                df = pd.read_csv(path, usecols=["timestamp"])
            except Exception as e:
                print(f"\n  [!] failed to read {fn} ({e.__class__.__name__}) → skip file")
                continue

            ts = pd.to_datetime(df["timestamp"], errors="coerce").dropna()
            if ts.empty:
                continue

            max_ts = ts.max()
            if last_ts is None or max_ts > last_ts:
                last_ts = max_ts

        if last_ts is not None:
            print(f"last timestamp {last_ts.isoformat()}")
        else:
            print("no valid timestamps found")

    print("→ Done.")

if __name__ == "__main__":
    main()
