#!/usr/bin/env python3

import csv
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

TIMESTAMP_RE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?P<tz>Z|[+-]\d{2}:?\d{2})?"
)


def parse_timestamp_from_name(name):
    match = TIMESTAMP_RE.search(name)
    if not match:
        return None
    ts = match.group("ts")
    tz = match.group("tz") or ""
    if tz == "Z":
        tz = "+00:00"
    iso = f"{ts}{tz}"
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def find_newest_file(directory):
    candidates = []
    for path in Path(directory).iterdir():
        if not path.is_file():
            continue
        dt = parse_timestamp_from_name(path.name)
        if dt is None:
            continue
        candidates.append((dt, path))
    if not candidates:
        raise RuntimeError(f"No timestamped files found in {directory}")
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def load_mau_by_software(csv_path):
    totals = defaultdict(int)
    with open(csv_path, newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            software = (row.get("software") or "").strip()
            if not software:
                continue
            mau_raw = (row.get("active_month") or "").strip()
            if not mau_raw:
                continue
            try:
                mau = int(mau_raw)
            except ValueError:
                continue
            totals[software] += mau
    return totals


def main():
    repo_root = Path(__file__).resolve().parents[2]
    input_dir = repo_root / "data" / "fedi-mau"
    output_dir = repo_root / "data" / "fedi-software"
    output_dir.mkdir(parents=True, exist_ok=True)

    newest_file = find_newest_file(input_dir)
    mau_totals = load_mau_by_software(newest_file)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    output_path = output_dir / f"{timestamp}.csv"

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["software", "mau"])
        for software in sorted(mau_totals):
            writer.writerow([software, mau_totals[software]])

    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
