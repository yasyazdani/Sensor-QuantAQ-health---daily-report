#!/usr/bin/env python3
"""
Sensor Status + Log Checker

This script does two things:

1) Fetches device statuses from the QuantAQ API.
   - Uses either the latest data timestamp (from /v1/data/most-recent/) or the
     device's `last_seen` field.
   - Compares the last seen time with the current local time.
   - If the device was seen within the last ACTIVE_MINUTES, it is shown as "active".
   - Otherwise, it is shown as "inactive for <duration>" (how long since last seen).

2) For devices that are inactive:
   - Reads the log file (LOG_TXT) stored in the same folder as this script.
   - Detects the header row automatically by looking for "Sensor ID".
   - Finds log entries for that sensor within the last LOOKBACK_DAYS.
   - Prints the timestamp and the Notes (if present) from those entries.

Configuration:
- LOG_TXT:     filename of your log file ( the text file must include "Sensor ID").
- CRED_FILE:   JSON file with {"api_key": "..."} for the QuantAQ API.
- ORG_ID, NETWORK_ID: IDs for your QuantAQ organization and network.
- ACTIVE_MINUTES: how many minutes since last data before a device is marked inactive.
- LOOKBACK_DAYS: how many past days of notes to show for inactive devices.

Usage:
    python3 Local-computer.py

Output:
- Prints the current time in your local timezone.
- Lists each device with "active" or "inactive for X:YY:ZZ".
- For inactive devices, shows recent notes from the log (if any).
"""

import os
import sys
import json
import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from requests.auth import HTTPBasicAuth

# ————— CONFIG —————
LOOKBACK_DAYS  = 7
ACTIVE_MINUTES = 10
LOG_TXT        = "Low_cost_sensors_log_qq.txt"
CRED_FILE      = "credentials.json"
BASE_URL       = "https://api.quant-aq.com"
ORG_ID         = 1229
NETWORK_ID     = 31
# ————————————

def load_log_dataframe():
    """Load the TXT log, auto-detecting the header row containing 'Sensor ID'."""
    base = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base, LOG_TXT)
    if not os.path.exists(path):
        print(f"WARNING: '{LOG_TXT}' not found in {base}")
        return pd.DataFrame()

    header_idx = None
    with open(path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if 'Sensor ID' in line:
                header_idx = i
                break
    if header_idx is None:
        print("WARNING: 'Sensor ID' column not found in the log file header.")
        return pd.DataFrame()

    df = pd.read_csv(path, sep='\t', header=header_idx, dtype={"Sensor ID": "Int64"})

    if "Datetime (for formulas)" in df.columns:
        df["Datetime"] = pd.to_datetime(df["Datetime (for formulas)"], errors='coerce')
    elif "Date" in df.columns and "Time" in df.columns:
        df["Datetime"] = pd.to_datetime(
            df["Date"].astype(str) + " " + df["Time"].astype(str),
            errors="coerce"
        )
    else:
        ts_cols = [c for c in df.columns if 'date' in c.lower()]
        if ts_cols:
            df["Datetime"] = pd.to_datetime(df[ts_cols[0]], errors='coerce')
        else:
            print("WARNING: No suitable Date/Time column found in log file.")
            return pd.DataFrame()

    return df.dropna(subset=["Datetime"])

def load_api_key(path=CRED_FILE) -> str:
    if not os.path.exists(path):
        print(f"ERROR: '{CRED_FILE}' not found.")
        sys.exit(1)
    with open(path) as f:
        key = json.load(f).get("api_key")
    if not key:
        print(f"ERROR: '{CRED_FILE}' must contain an api_key field")
        sys.exit(1)
    return key

def make_session(api_key: str) -> requests.Session:
    sess = requests.Session()
    sess.auth = HTTPBasicAuth(api_key, "")
    return sess

def list_devices(session, org_id, network_id, per_page=200):
    resp = session.get(
        f"{BASE_URL}/v1/devices",
        params={"org_id": org_id, "network_id": network_id, "per_page": per_page}
    )
    resp.raise_for_status()
    return resp.json().get("data", [])

def fetch_most_recent(session, network_id, per_page=200):
    resp = session.get(
        f"{BASE_URL}/v1/data/most-recent/",
        params={"network_id": network_id, "per_page": per_page}
    )
    resp.raise_for_status()
    return resp.json().get("data", [])

def parse_iso_utc(ts_str: str) -> datetime:
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1]
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def main():
    log_df = load_log_dataframe()
    cutoff = datetime.now() - timedelta(days=LOOKBACK_DAYS)

    api_key = load_api_key()
    sess    = make_session(api_key)

    devices = list_devices(sess, ORG_ID, NETWORK_ID)
    records = fetch_most_recent(sess, NETWORK_ID)
    local_tz = ZoneInfo("America/Toronto")
    now      = datetime.now(local_tz)
    threshold = timedelta(minutes=ACTIVE_MINUTES)

    # map serial -> most recent data timestamp
    reported = {r["sn"]: parse_iso_utc(r["timestamp"]) for r in records}

    print(f"\nSensor Status \n" + "="*60)
    for d in devices:
        sn = d["sn"]
        ts_utc = reported.get(sn) or (parse_iso_utc(d["last_seen"]) if d.get("last_seen") else None)

        if ts_utc:
            last_local = ts_utc.astimezone(local_tz)
            inactive = now - last_local
            if inactive <= threshold:
                status = "active"
            else:
                inactive_str = str(inactive).split('.')[0]
                status = f"inactive for {inactive_str}"
        else:
            status = "inactive for unknown duration"

        print(f"{sn:12} → {status}")

        # if inactive, show notes from log file
        if not status.startswith("active") and not log_df.empty:
            try:
                sid = int(sn.split("-")[-1])
            except ValueError:
                continue
            recent = (
                log_df[log_df["Sensor ID"] == sid]
                .loc[lambda df: df["Datetime"] >= cutoff]
                .sort_values("Datetime", ascending=False)
            )
            if recent.empty:
                print(f"   ⟳ no entries for {sn} in last {LOOKBACK_DAYS} days")
            else:
                print(f"   ⟳ recent log entries for {sn}:")
                for _, row in recent.iterrows():
                    ts = row["Datetime"].strftime("%Y-%m-%d %H:%M")
                    note = str(row.get("Notes", "")).strip()
                    print(f"      - {ts} — {note}")

if __name__ == "__main__":
    main()
