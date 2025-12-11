#!/usr/bin/env python3
import sys
import os
import json
import csv
from typing import List, Optional

def extract_fields(nodeinfo_wrapper: dict):
    hostname = nodeinfo_wrapper.get("hostname", "")

    ni = nodeinfo_wrapper.get("nodeinfo") or {}

    software_name: Optional[str] = None
    users_total: Optional[int] = None
    active_month: Optional[int] = None
    protocols: Optional[List[str]] = None

    software = ni.get("software") or {}
    if isinstance(software, dict):
        software_name = software.get("name")

    usage = ni.get("usage") or {}
    if isinstance(usage, dict):
        users = usage.get("users") or {}
        if isinstance(users, dict):
            users_total = users.get("total")
            active_month = users.get("activeMonth")

    if isinstance(ni.get("protocols"), list):
        protocols = [str(p) for p in ni["protocols"]]
    else:
        metadata = ni.get("metadata")
        if isinstance(metadata, dict) and isinstance(metadata.get("protocols"), list):
            protocols = [str(p) for p in metadata["protocols"]]

    protocols_str = ";".join(protocols) if protocols else ""

    return hostname, software_name, users_total, active_month, protocols_str

def main() -> None:
    import datetime

    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print(f"Usage: {sys.argv[0]} nodeinfo_dir output.csv [max_age_days=30]", file=sys.stderr)
        sys.exit(1)

    nodeinfo_dir = sys.argv[1]
    output_csv = sys.argv[2]
    max_age_days = int(sys.argv[3]) if len(sys.argv) == 4 else 30

    cutoff = datetime.timedelta(days=max_age_days)
    # Make 'now' timezone-aware (UTC)
    now = datetime.datetime.now(datetime.timezone.utc)

    hostname_dirs = [
        os.path.join(nodeinfo_dir, d)
        for d in os.listdir(nodeinfo_dir)
        if os.path.isdir(os.path.join(nodeinfo_dir, d))
    ]

    selected_files = []

    for hdir in hostname_dirs:
        hostname = os.path.basename(hdir)
        candidates = []

        for fn in os.listdir(hdir):
            if not fn.endswith(".json"):
                continue
            path = os.path.join(hdir, fn)
            stem = fn[:-5]  # strip ".json"
            try:
                ts = datetime.datetime.fromisoformat(stem.replace("Z", "+00:00"))
                # Ensure parsed timestamp is also aware UTC
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=datetime.timezone.utc)
                else:
                    ts = ts.astimezone(datetime.timezone.utc)
            except Exception:
                continue
            candidates.append((ts, path))

        if not candidates:
            continue

        ts_newest, path_newest = max(candidates, key=lambda x: x[0])

        if now - ts_newest > cutoff:
            continue

        selected_files.append(path_newest)

    selected_files.sort()

    with open(output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["hostname", "software", "users_total", "active_month", "protocols"])

        for path in selected_files:
            try:
                with open(path, "r", encoding="utf-8") as jf:
                    wrapper = json.load(jf)
            except Exception as e:
                print(f"# Skipping {path}: {e}", file=sys.stderr)
                continue

            hostname, software_name, users_total, active_month, protocols_str = extract_fields(wrapper)

            writer.writerow([
                hostname or "",
                software_name or "",
                users_total if users_total is not None else "",
                active_month if active_month is not None else "",
                protocols_str,
            ])

    print(f"# Wrote {len(selected_files)} rows to {output_csv}", file=sys.stderr)

if __name__ == "__main__":
    main()
