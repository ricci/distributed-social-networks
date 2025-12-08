#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

def get_endpoint(rec):
    op = rec.get("operation") or {}
    services = op.get("services") or {}
    pds_info = services.get("atproto_pds") or {}
    return pds_info.get("endpoint")

def get_rotation_keys(rec):
    op = rec.get("operation") or {}
    keys = op.get("rotationKeys") or []
    # Normalize: unique + sorted for stable comparison / output
    return sorted(set(keys))

def pick_non_nullified_sorted(data, path_for_err):
    """Return list of non-nullified records sorted by createdAt (string)."""
    if not isinstance(data, list) or not data:
        return []

    non_nullified = [rec for rec in data if not rec.get("nullified", False)]
    if not non_nullified:
        return []

    try:
        non_nullified.sort(key=lambda r: r.get("createdAt", ""))
    except Exception as e:
        print(f"Error sorting by createdAt in {path_for_err}: {e}", file=sys.stderr)
        return []

    return non_nullified

def process_file(path: Path):
    """
    For one audit-log JSON file, return a list of "interesting" changes:
    operations where rotation keys change but PDS endpoint stays the same.
    """
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading {path}: {e}", file=sys.stderr)
        return []

    records = pick_non_nullified_sorted(data, path)
    if len(records) < 2:
        return []

    results = []

    # We assume all records in a file are for the same DID, but be defensive.
    default_did = records[0].get("did")

    prev = records[0]
    for cur in records[1:]:
        did_prev = prev.get("did") or default_did
        did_cur = cur.get("did") or default_did
        if not did_prev or not did_cur or did_prev != did_cur:
            prev = cur
            continue

        ep_prev = get_endpoint(prev)
        ep_cur = get_endpoint(cur)

        # We only care about key changes that *don't* move PDS
        if ep_prev != ep_cur:
            prev = cur
            continue

        keys_prev = get_rotation_keys(prev)
        keys_cur = get_rotation_keys(cur)

        if keys_prev == keys_cur:
            prev = cur
            continue

        set_prev = set(keys_prev)
        set_cur = set(keys_cur)
        added = sorted(set_cur - set_prev)
        removed = sorted(set_prev - set_cur)

        if added and not removed:
            change_type = "keys_added"
        elif removed and not added:
            change_type = "keys_removed"
        else:
            change_type = "keys_changed"

        results.append({
            "did": did_cur,
            "createdAt": cur.get("createdAt", ""),
            "pds": ep_cur or "",
            "old_keys": keys_prev,
            "new_keys": keys_cur,
            "added_keys": added,
            "removed_keys": removed,
            "change_type": change_type,
        })

        prev = cur

    return results

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Find operations that change rotation keys without moving PDS, "
            "from PLC audit log JSON files."
        )
    )
    parser.add_argument(
        "directory",
        nargs="?",
        default=".",
        help="Directory containing JSON files (default: current directory)",
    )
    parser.add_argument(
        "--pattern",
        default="*.json",
        help="Glob pattern for files (default: *.json)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=32,
        help="Number of worker threads for parsing (default: 32)",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=100,
        help="Chunksize hint for executor.map (default: 100)",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=0,
        help="If >0, print a progress line to stderr every N files processed",
    )
    args = parser.parse_args()

    base_dir = Path(args.directory)
    if not base_dir.is_dir():
        print(f"{base_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    files_iter = base_dir.glob(args.pattern)

    # Header
    print("# Rotation key changes without PDS move")
    print("did\tcreatedAt\tpds_endpoint\tchange_type\told_keys\tnew_keys\tadded_keys\tremoved_keys")

    processed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        for changes in executor.map(process_file, files_iter, chunksize=args.chunksize):
            processed += 1
            if args.progress_every and processed % args.progress_every == 0:
                print(f"Processed {processed} files...", file=sys.stderr)

            if not changes:
                continue

            for ch in changes:
                join = lambda xs: ";".join(xs)
                print(
                    f"{ch['did']}\t"
                    f"{ch['createdAt']}\t"
                    f"{ch['pds']}\t"
                    f"{ch['change_type']}\t"
                    f"{join(ch['old_keys'])}\t"
                    f"{join(ch['new_keys'])}\t"
                    f"{join(ch['added_keys'])}\t"
                    f"{join(ch['removed_keys'])}"
                )

if __name__ == "__main__":
    main()

