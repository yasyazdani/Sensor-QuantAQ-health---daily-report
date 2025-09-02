#!/usr/bin/env python3
"""
generate_daily_stats.py

For each sensor under REMOTE_BASE, compute per-24 h window stats (6 AM–6 AM UTC):
– mean, min, max, std, median for NO, NO2, CO, O3, PM1, PM2.5, PM10, temp, rh
– expected vs. available count (ignore NaNs)
Append new daily summaries to `<sensor>_daily_statics.txt`, creating one per sensor (even if no data).
"""

import os
import glob
import pandas as pd
from datetime import timedelta
import pytz

# ─── CONFIG ───────────────────────────────────────────────────────────────────
# Adjust this date to change where historical processing starts
DATE = "2025-01-01"
REMOTE_BASE = "/export/data2/tame-insitu/quantaq/cloud"
VARS = ["co", "no", "no2", "o3", "pm1", "pm25", "pm10", "temp", "rh"]
WINDOW_START_HOUR = 6  # 6 AM UTC
TZ = pytz.UTC

# ─── PREPARE WINDOWS ───────────────────────────────────────────────────────────
start_dt = pd.to_datetime(DATE).tz_localize(TZ) + pd.Timedelta(hours=WINDOW_START_HOUR)
now_utc = pd.Timestamp.now(tz=TZ)
today_6am = now_utc.normalize() + pd.Timedelta(hours=WINDOW_START_HOUR)
if now_utc < today_6am:
    today_6am -= pd.Timedelta(days=1)

# Generate daily 24h window start times
window_starts = pd.date_range(
    start=start_dt,
    end=today_6am - pd.Timedelta(days=1),
    freq="24h",
    tz=TZ
)

# ─── PROCESS EACH SENSOR ──────────────────────────────────────────────────────
sensors = sorted(os.listdir(REMOTE_BASE))
pattern = os.path.join(REMOTE_BASE, "{sensor}", "*", "{year}-*-MOD{sensor}final.csv")
year = start_dt.year

for sensor in sensors:
    # Prepare a DataFrame for this sensor (possibly empty)
    sensor_pattern = pattern.format(sensor=sensor, year=year)
    file_list = sorted(glob.glob(sensor_pattern))
    dfs = []
    for path in file_list:
        try:
            df = pd.read_csv(path, usecols=VARS + ["timestamp"] )
        except Exception:
            continue
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        # ensure numeric columns
        for v in VARS:
            df[v] = pd.to_numeric(df[v], errors="coerce")
        df = df.dropna(subset=["timestamp"]).set_index("timestamp")
        df.index = df.index.tz_localize(TZ, ambiguous="NaT", nonexistent="NaT")
        dfs.append(df)

    if dfs:
        all_df = pd.concat(dfs).sort_index()
    else:
        # empty DataFrame with correct columns and datetime index
        all_df = pd.DataFrame(columns=VARS)
        all_df.index = pd.DatetimeIndex([], tz=TZ)

    out_file = f"{sensor}_daily_statics.txt"
    # Create file with header if missing
    if not os.path.exists(out_file):
        header = ["date"]
        for v in VARS:
            header += [f"mean_{v}", f"min_{v}", f"max_{v}", f"std_{v}", f"median_{v}"]
        header += ["expected_count", "available_count"]
        with open(out_file, "w") as f:
            f.write(",".join(header) + "\n")
        last_date = start_dt - pd.Timedelta(days=1)
    else:
        existing = pd.read_csv(out_file, parse_dates=["date"] )
        if existing.empty:
            last_date = start_dt - pd.Timedelta(days=1)
        else:
            last_date = existing["date"].max().tz_localize(TZ)

    # Determine which windows to process
    to_do = [w for w in window_starts if w > last_date]
    if not to_do:
        print(f"{sensor}: no new windows to process.")
        continue

    # Append stats for each new window
    with open(out_file, "a") as f:
        for window_start in to_do:
            window_end = window_start + pd.Timedelta(hours=24)
            block = all_df.loc[window_start:window_end]

            row = [window_start.isoformat()]
            # Compute statistics for each variable
            for v in VARS:
                series = block[v].dropna()
                if not series.empty:
                    row += [
                        f"{series.mean():.3f}",
                        f"{series.min():.3f}",
                        f"{series.max():.3f}",
                        f"{series.std():.3f}",
                        f"{series.median():.3f}",
                    ]
                else:
                    row += ["", "", "", "", ""]

            # Expected count based on smallest positive interval
            if len(block.index) >= 2:
                deltas = block.index.to_series().diff().dropna()
                sec = deltas[deltas > pd.Timedelta(0)].min().total_seconds()
                expected = int(round(24 * 3600 / sec))
            else:
                expected = ""
            available = len(block.dropna(how="all", subset=VARS))

            row += [str(expected), str(available)]
            f.write(",".join(row) + "\n")
            print(f"{sensor} {window_start.date()}: appended stats.")
