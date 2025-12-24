#!/usr/bin/env python3
import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from typing import Dict, List


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Compute per-PDS MAU from one or more accounts snapshot JSON files.\n"
            "For each domain, we take the maximum MAU seen in any input file."
        )
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help="Input JSON snapshot files produced by the firehose tracker.",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="pds_mau.csv",
        help="Output CSV file (default: pds_mau.csv)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Lookback window in days for 'active' accounts (default: 30)",
    )
    return parser.parse_args()


def counts_from_snapshot(path: str, cutoff: datetime) -> Counter:
    """
    Given a single snapshot JSON file (accounts_snapshot.json-style), return
    a Counter mapping domain -> MAU (number of active DIDs on that domain).
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    counts = Counter()

    for did, entry in data.items():
        last_seen_str = entry.get("last_seen")
        pds = entry.get("pds")

        if not last_seen_str or not pds:
            continue

        try:
            last_seen = datetime.fromisoformat(last_seen_str)
        except Exception:
            continue

        if last_seen < cutoff:
            # Not active in the last N days
            continue

        # Strip PDS URL down to hostname only
        parsed = urlparse(pds)
        domain = parsed.hostname or parsed.path or pds

        counts[domain] += 1

    return counts


def combine_counts_max(counters: List[Counter]) -> Dict[str, int]:
    """
    Combine multiple domain->MAU Counters into one mapping, taking the
    maximum MAU per domain across all Counters (not the sum).
    """
    combined: Dict[str, int] = {}

    for c in counters:
        for domain, mau in c.items():
            if domain not in combined:
                combined[domain] = mau
            else:
                if mau > combined[domain]:
                    combined[domain] = mau

    return combined


def main():
    args = parse_args()

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=args.days)

    per_file_counts: List[Counter] = []
    for path in args.inputs:
        print(f"Processing snapshot: {path}")
        c = counts_from_snapshot(path, cutoff)
        print(f"  Found {len(c)} active domains in {path}")
        per_file_counts.append(c)

    combined = combine_counts_max(per_file_counts)

    # Write CSV: domain,mau
    with open(args.output, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["domain", "mau"])
        # Sort by MAU descending, then domain
        for domain, mau in sorted(combined.items(), key=lambda x: (-x[1], x[0])):
            writer.writerow([domain, mau])

    print(f"Wrote {len(combined)} domains to {args.output}")


if __name__ == "__main__":
    main()
