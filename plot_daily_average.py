#!/usr/bin/env python3
"""
plot_daily_average.py

Reads *_daily_statics.txt in current directory, filters windows by availability >= 0.9,
plots daily mean for each of 9 variables across sensors in a 3x3 subplot layout.
Uses consistent colors per sensor, with legend only on PM1 subplot.
Units: ppb for gases, °C for temp, % for rh.
Saves figure as daily_average_time_series.png.
"""
import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import sys

# ─── CONFIG ───────────────────────────────────────────────────────────────────
VARS = ["co", "no", "no2", "o3", "pm1", "pm25", "pm10", "temp", "rh"]
YLABELS = {**{v: 'ppb' for v in VARS[:-2]}, 'temp': '°C', 'rh': '%'}
FILES_PATTERN = "*_daily_statics.txt"
OUTPUT_FILE = "daily_average_time_series.png"
MIN_RATIO = 0.9

# ─── LOAD & PROCESS DATA ───────────────────────────────────────────────────────
sensor_dfs = {}
for filepath in glob.glob(FILES_PATTERN):
    sensor = filepath.replace("_daily_statics.txt", "")
    try:
        df = pd.read_csv(filepath, parse_dates=["date"] )
    except Exception as e:
        print(f"Error reading {filepath}: {e}", file=sys.stderr)
        continue
    # compute availability ratio
    df["expected_count"] = pd.to_numeric(df["expected_count"], errors="coerce")
    df["available_count"] = pd.to_numeric(df["available_count"], errors="coerce")
    df["ratio"] = df["available_count"] / df["expected_count"]
    # filter
    df = df[df["ratio"] >= MIN_RATIO].copy()
    # convert mean columns
    for v in VARS:
        df[f"mean_{v}"] = pd.to_numeric(df.get(f"mean_{v}"), errors="coerce")
    sensor_dfs[sensor] = df

if not sensor_dfs:
    print("No data available after filtering.")
    sys.exit(1)

# determine global date range from unfiltered files
all_dates = []
for df in sensor_dfs.values():
    all_dates.extend(df['date'])
start_date = min(all_dates).date()
end_date = max(all_dates).date()

# consistent color mapping
sensors = sorted(sensor_dfs.keys())
colors = plt.cm.tab20(range(len(sensors)))
color_map = {sensor: colors[i] for i, sensor in enumerate(sensors)}

# ─── PLOTTING ─────────────────────────────────────────────────────────────────
fig, axes = plt.subplots(3, 3, figsize=(15, 12), sharex=True)
axes = axes.flatten()
for ax, var in zip(axes, VARS):
    for sensor, df in sensor_dfs.items():
        ax.plot(df['date'], df[f'mean_{var}'], label=sensor, color=color_map[sensor])
    ax.set_title(var.upper())
    ax.set_ylabel(YLABELS[var])
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True)
    # add legend only on PM1 subplot
    if var == 'pm1':
        ax.legend(loc='upper right', fontsize='small', ncol=2)

# hide any unused axes
for ax in axes[len(VARS):]:
    ax.axis('off')

fig.suptitle(f"Daily average from {start_date} to {end_date}", fontsize=16)
fig.tight_layout(rect=[0, 0, 1, 0.95])

plt.savefig(OUTPUT_FILE, dpi=150)
print(f"Saved plot to {OUTPUT_FILE}")
