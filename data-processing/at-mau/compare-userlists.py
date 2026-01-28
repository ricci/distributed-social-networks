#!/usr/bin/env python3
import argparse
import json
import sys
import urllib.parse
from datetime import datetime, timedelta, timezone


def host(url):
    return urllib.parse.urlparse(url).hostname


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare per-PDS user lists from two snapshot files."
    )
    parser.add_argument("file_a", help="First snapshot JSON file")
    parser.add_argument("file_b", help="Second snapshot JSON file")
    parser.add_argument(
        "--days",
        type=float,
        default=30,
        help="Lookback window in days for 'active' accounts (default: 30)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )
    return parser.parse_args()


def in_gap(ts, gaps):
    for start, end, _ in gaps:
        if start < ts < end:
            return True
    return False


def load_snapshot(fn, cutoff):
    with open(fn) as f:
        data = json.load(f)

    by_pds = {}
    by_did = {}
    by_did_last_seen = {}
    times = []
    for did, entry in data.items():
        last_seen_str = entry.get("last_seen")
        if not last_seen_str:
            continue
        try:
            last_seen = datetime.fromisoformat(last_seen_str)
        except Exception:
            continue
        if last_seen < cutoff:
            continue
        times.append(last_seen)

        pds = entry.get("pds")
        if not pds:
            continue
        pds_host = host(pds) or pds
        by_pds.setdefault(pds_host, set()).add(did)
        by_did[did] = pds_host
        by_did_last_seen[did] = last_seen

    return by_pds, by_did, by_did_last_seen, sorted(times)


def find_gaps(times, minimum_gap):
    gaps = []
    for i in range(1, len(times)):
        prev = times[i - 1]
        cur = times[i]
        gap = cur - prev
        if gap >= minimum_gap:
            gaps.append((prev, cur, gap))
    return gaps


args = parse_args()
file_a = args.file_a
file_b = args.file_b

cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)

A_by_pds, A_by_did, A_last_seen, A_times = load_snapshot(file_a, cutoff)
B_by_pds, B_by_did, B_last_seen, B_times = load_snapshot(file_b, cutoff)

min_gap = timedelta(minutes=10)
gaps_by_file = {}
for label, path, times in (("A", file_a, A_times), ("B", file_b, B_times)):
    gaps = find_gaps(times, min_gap)
    gaps_by_file[path] = gaps

if A_times and B_times:
    A_end = A_times[-1]
    B_end = B_times[-1]
    if A_end < B_end:
        gaps_by_file[file_a].append((A_end, B_end, B_end - A_end))
    elif B_end < A_end:
        gaps_by_file[file_b].append((B_end, A_end, A_end - B_end))

A = A_by_pds
B = B_by_pds

all_pds = set(A.keys()) | set(B.keys())
only_in_A_pds = sorted(set(A.keys()) - set(B.keys()))
only_in_B_pds = sorted(set(B.keys()) - set(A.keys()))
rows = []
totals = {"common": 0, "only_a": 0, "only_b": 0}
overall = {"common": 0, "only_a": 0, "only_b": 0}
identical_pds = 0
differing_pds = 0
skipped_a_dids = 0
skipped_b_dids = 0
skipped_a_pds = {}
skipped_b_pds = {}

all_a_dids = set(A_by_did.keys())
all_b_dids = set(B_by_did.keys())
shared_dids = all_a_dids & all_b_dids
pds_mismatch_dids = {
    did for did in shared_dids if A_by_did.get(did) != B_by_did.get(did)
}
only_in_a = all_a_dids - all_b_dids
only_in_b = all_b_dids - all_a_dids
ignored_a_dids = {
    did for did in only_in_a if in_gap(A_last_seen[did], gaps_by_file[file_b])
}
ignored_b_dids = {
    did for did in only_in_b if in_gap(B_last_seen[did], gaps_by_file[file_a])
}
overall["common"] = len(shared_dids)
overall["only_a"] = len(only_in_a - ignored_a_dids)
overall["only_b"] = len(only_in_b - ignored_b_dids)
skipped_a_dids = len(ignored_a_dids)
skipped_b_dids = len(ignored_b_dids)
for did in ignored_a_dids:
    pds = A_by_did.get(did)
    if pds:
        skipped_a_pds[pds] = skipped_a_pds.get(pds, 0) + 1
for did in ignored_b_dids:
    pds = B_by_did.get(did)
    if pds:
        skipped_b_pds[pds] = skipped_b_pds.get(pds, 0) + 1

for pds in all_pds:
    a_dids = A.get(pds, set()) - ignored_a_dids
    b_dids = B.get(pds, set()) - ignored_b_dids

    if not a_dids or not b_dids:
        continue

    common = len(a_dids & b_dids)
    only_a = len(a_dids - b_dids)
    only_b = len(b_dids - a_dids)

    if a_dids == b_dids:
        identical_pds += 1
        continue

    differing_pds += 1
    totals["common"] += common
    totals["only_a"] += only_a
    totals["only_b"] += only_b
    rows.append((only_a + only_b, pds, common, only_a, only_b))

only_a_rows = []
only_b_rows = []
if only_in_A_pds:
    only_a_rows = sorted(
        ((len(A[pds] - ignored_a_dids), pds) for pds in only_in_A_pds),
        reverse=True,
    )

if only_in_B_pds:
    only_b_rows = sorted(
        ((len(B[pds] - ignored_b_dids), pds) for pds in only_in_B_pds),
        reverse=True,
    )

total_pds_a = len(A)
total_pds_b = len(B)
total_users_a = len(A_by_did)
total_users_b = len(B_by_did)
shared_pds = len(set(A.keys()) & set(B.keys()))

period_start_candidates = []
period_end_candidates = []
if A_times:
    period_start_candidates.append(A_times[0])
    period_end_candidates.append(A_times[-1])
if B_times:
    period_start_candidates.append(B_times[0])
    period_end_candidates.append(B_times[-1])
period_start = min(period_start_candidates) if period_start_candidates else None
period_end = max(period_end_candidates) if period_end_candidates else None

if args.json:
    output = {
        "meta": {
            "inputs": {"a": file_a, "b": file_b},
            "period_start": period_start.isoformat() if period_start else None,
            "period_end": period_end.isoformat() if period_end else None,
        },
        "pds_only_in": {
            "a": [{"pds": pds, "users": count} for count, pds in only_a_rows],
            "b": [{"pds": pds, "users": count} for count, pds in only_b_rows],
        },
        "summary": {
            "total_pds_in_a": total_pds_a,
            "total_pds_in_b": total_pds_b,
            "total_users_in_a": total_users_a,
            "total_users_in_b": total_users_b,
            "shared_pds": shared_pds,
            "identical_pds": identical_pds,
            "differing_pds": differing_pds,
            "pds_only_in_a": len(only_in_A_pds),
            "pds_only_in_b": len(only_in_B_pds),
            "users_only_in_a": overall["only_a"],
            "users_only_in_b": overall["only_b"],
            "users_common": overall["common"],
            "skipped_users_in_a": skipped_a_dids,
            "skipped_users_in_b": skipped_b_dids,
            "skipped_pds_in_a": len(skipped_a_pds),
            "skipped_pds_in_b": len(skipped_b_pds),
        },
        "gaps": {
            "a": [
                {
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "minutes": int(gap.total_seconds() // 60),
                }
                for start, end, gap in gaps_by_file.get(file_a, [])
            ],
            "b": [
                {
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "minutes": int(gap.total_seconds() // 60),
                }
                for start, end, gap in gaps_by_file.get(file_b, [])
            ],
        },
        "pds_differences": [
            {
                "pds": pds,
                "common": common,
                "only_in_a": only_a,
                "only_in_b": only_b,
            }
            for _, pds, common, only_a, only_b in sorted(rows, reverse=True)
        ],
        "pds_differences_total": (
            {
                "common": totals["common"],
                "only_in_a": totals["only_a"],
                "only_in_b": totals["only_b"],
            }
            if rows
            else None
        ),
    }
    print(json.dumps(output, indent=2, sort_keys=False))
    sys.exit(0)

if only_a_rows:
    total_a_users = sum(count for count, _ in only_a_rows)
    print(f"PDS only in {file_a}")
    for count, pds in only_a_rows:
        print(f"  {count:8d}  {pds}")
    print(f"  total_pds: {len(only_a_rows)}")
    print(f"  total_users: {total_a_users}")

if only_b_rows:
    total_b_users = sum(count for count, _ in only_b_rows)
    print(f"PDS only in {file_b}")
    for count, pds in only_b_rows:
        print(f"  {count:8d}  {pds}")
    print(f"  total_pds: {len(only_b_rows)}")
    print(f"  total_users: {total_b_users}")

print("SUMMARY")
print(f"  file_a: {file_a}")
print(f"  file_b: {file_b}")
print(f"  total_pds_in_{file_a}: {total_pds_a}")
print(f"  total_pds_in_{file_b}: {total_pds_b}")
print(f"  total_users_in_{file_a}: {total_users_a}")
print(f"  total_users_in_{file_b}: {total_users_b}")
print(f"  shared_pds: {shared_pds}")
print(f"  identical_pds: {identical_pds}")
print(f"  differing_pds: {differing_pds}")
print(f"  pds_only_in_{file_a}: {len(only_in_A_pds)}")
print(f"  pds_only_in_{file_b}: {len(only_in_B_pds)}")
print(f"  users_only_in_{file_a}: {overall['only_a']}")
print(f"  users_only_in_{file_b}: {overall['only_b']}")
print(f"  users_common: {overall['common']}")
print(f"  users_pds_mismatch: {len(pds_mismatch_dids)}")
print(f"  skipped_users_in_{file_a}: {skipped_a_dids}")
print(f"  skipped_users_in_{file_b}: {skipped_b_dids}")
print(f"  skipped_pds_in_{file_a}: {len(skipped_a_pds)}")
print(f"  skipped_pds_in_{file_b}: {len(skipped_b_pds)}")

for path in (file_a, file_b):
    gaps = gaps_by_file.get(path, [])
    print(f"GAPS in {path}")
    if not gaps:
        print("  (none)")
        continue
    for start, end, gap in gaps:
        minutes = int(gap.total_seconds() // 60)
        print(f"  {start.isoformat()} -> {end.isoformat()} ({minutes} min)")

for _, pds, common, only_a, only_b in sorted(rows, reverse=True):
    print(f"{pds}")
    print(f"  common: {common}")
    print(f"  only_in_{file_a}: {only_a}")
    print(f"  only_in_{file_b}: {only_b}")

if rows:
    print("TOTAL")
    print(f"  common: {totals['common']}")
    print(f"  only_in_{file_a}: {totals['only_a']}")
    print(f"  only_in_{file_b}: {totals['only_b']}")

if pds_mismatch_dids:
    print("PDS mismatches")
    for did in sorted(pds_mismatch_dids):
        print(f"  {did}  {A_by_did[did]}  {B_by_did[did]}")
