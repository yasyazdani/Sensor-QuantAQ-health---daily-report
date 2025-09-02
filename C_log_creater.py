"""
Sensor Log Creator — How to use

1) Run this script on your computer, Carbon, or any system:
   - The log is appended to OUTPUT_FILE ("quantaq_log.txt") in the same folder
     where the script is located (unless you change OUTPUT_FILE).

2) For each sensor entry you will be asked for 5 fields:
       Date, Time, Sensor ID, Location, Notes
   - After each input, the program shows what you typed and asks:
         You entered '...'. Keep? (Y/n):
   - Press Enter or type 'Y' to accept.
   - Type 'n' to re-enter that field.

3) After all 5 fields are confirmed, the program asks:
       Are you done? (Y/n):
   - If you press Enter or type 'Y', the program saves all entries collected so far
     into the log file and prints the full file path.
   - If you type 'n', the program starts a new input cycle for another sensor.

Press Ctrl+C at any time to cancel without saving.
"""





#!/usr/bin/env python3
import os, sys
from datetime import datetime

COLUMNS = ["Date","Time","Sensor ID","Location","Notes"]
OUTPUT_FILE = "quantaq_log.txt"

def prompt_with_confirmation(label: str, allow_blank: bool = True) -> str:
    while True:
        val = input(f"{label}: ").strip()
        if val == "" and not allow_blank:
            print("This field cannot be blank. Please enter a value.")
            continue
        keep = input(f"You entered '{val}'. Keep? (Y/n): ").strip().lower()
        if keep in ("", "y", "yes"):
            return val
        print("Okay, let's try again.")

def collect_one_entry() -> dict:
    print("\nEnter values for each field (Ctrl+C to abort):\n")
    entry = {}
    for col in COLUMNS:
        entry[col] = prompt_with_confirmation(col, allow_blank=True)
    return entry

def save_entries(entries):
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        for e in entries:
            f.write(f"--- Entry at {datetime.now():%Y-%m-%d %H:%M:%S} ---\n")
            for col in COLUMNS:
                f.write(f"{col}: {e.get(col, '')}\n")
            f.write("\n")
    path = os.path.abspath(OUTPUT_FILE)
    print(f"\n✓ Saved {len(entries)} entr{'y' if len(entries)==1 else 'ies'} to:\n  {path}\n")
    try:
        os.system(f"open -R '{path}'")  # macOS only
    except Exception:
        pass

def main():
    entries = []
    while True:
        entries.append(collect_one_entry())
        done = input("Are you done? (Y/n): ").strip().lower()
        if done in ("", "y", "yes"):
            save_entries(entries)
            sys.exit(0)
        # else loop for another sensor

if __name__ == "__main__":
    main()




