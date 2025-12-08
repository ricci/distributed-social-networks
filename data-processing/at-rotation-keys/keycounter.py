#!/usr/bin/env python3
import argparse
import csv
import json
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from collections import Counter

def pick_latest_non_nullified(data, path_for_err):
    """Return latest non-nullified record by createdAt (string compare)."""
    if not isinstance(data, list) or not data:
        return None

    non_nullified = [rec for rec in data if not rec.get("nullified", False)]
    if not non_nullified:
        return None

    # ISO8601 strings sort correctly as strings if format is consistent
    try:
        latest = max(non_nullified, key=lambda r: r.get("createdAt", ""))
    except Exception as e:
        print(f"Error choosing latest createdAt in {path_for_err}: {e}", file=sys.stderr)
        return None

    return latest

def process_file(path: Path):
    """
    Parse one JSON file and return (did, rotation_keys, endpoint) for the
    latest non-nullified record with an atproto_pds endpoint.
    Returns None if the file isn't usable.
    """
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading {path}: {e}", file=sys.stderr)
        return None

    latest = pick_latest_non_nullified(data, path)
    if latest is None:
        return None

    did = latest.get("did") or (isinstance(data, list) and data[0].get("did"))
    if not did:
        return None

    op = latest.get("operation") or {}
    services = op.get("services") or {}
    pds_info = services.get("atproto_pds") or {}
    endpoint = pds_info.get("endpoint")
    if not endpoint:
        return None

    rotation_keys = op.get("rotationKeys") or []
    # De-dup within a single file, just in case
    rotation_keys = list(set(rotation_keys))

    if not rotation_keys:
        return None

    return did, rotation_keys, endpoint

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Guess which users may have set their own rotation keys, based on "
            "PLC audit logs."
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
        "--rotation-summary-filename",
        default="rotation-summary.csv",
        help="Filename for global rotation-key summary (default: rotation-summary.csv)",
    )
    parser.add_argument(
        "--pds-summary-filename",
        default="pds-summary.csv",
        help="Filename for PDS classification summary (default: pds-summary.csv)",
    )
    parser.add_argument(
        "--did-classification-filename",
        default="did-classification.csv",
        help="Filename for DID classification (default: did-classification.csv)",
    )
    parser.add_argument(
        "--solo-users-filename",
        default="solo-users.csv",
        help="Filename for DID classification (default: solo-users.csv)",
    )
    args = parser.parse_args()

    base_dir = Path(args.directory)
    if not base_dir.is_dir():
        print(f"{base_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    # rotation_key -> {"file_count": int, "endpoints": set(), "dids": set()}
    agg = {}
    # did -> {"keys": set(), "pds": set()}
    did_info = {}
    # pds_endpoint -> {"dids": set(), "key_counts": Counter()}
    pds_info = {}

    files_iter = base_dir.glob(args.pattern)
    processed = 0

    print("Processing files")
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        for res in executor.map(process_file, files_iter):
            processed += 1
            if processed % 100000 == 0:
                print(f"Processed {processed} files...", file=sys.stderr)

            if res is None:
                continue

            did, rotation_keys, endpoint = res

            # Update DID info
            di = did_info.setdefault(did, {"keys": set(), "pds": set()})
            di["keys"].update(rotation_keys)
            di["pds"].add(endpoint)

            # Update PDS info
            pi = pds_info.setdefault(endpoint, {"dids": set(), "key_counts": Counter()})
            pi["dids"].add(did)
            for rk in rotation_keys:
                pi["key_counts"][rk] += 1

            # Update rotation key aggregates (global, across all PDSes)
            for rk in rotation_keys:
                rk_info = agg.setdefault(
                    rk,
                    {"file_count": 0, "endpoints": set(), "dids": set()},
                )
                rk_info["file_count"] += 1
                rk_info["endpoints"].add(endpoint)
                rk_info["dids"].add(did)

    # --- PDS classification ---

    print("Classifying PDSes")
    # pds_type[endpoint] in {"solo_pds", "unique_pds", "shared_pds"}
    pds_type = {}
    for endpoint, info in pds_info.items():
        did_count = len(info["dids"])
        key_counts = info["key_counts"]

        if did_count == 1:
            pds_type[endpoint] = "solo_pds"
        else:
            if key_counts and all(c == 1 for c in key_counts.values()):
                pds_type[endpoint] = "unique_pds"
            else:
                pds_type[endpoint] = "shared_pds"

    # --- DID-level classification ---

    # did_class[did] -> dict with:
    #   "pds": sorted list of endpoints
    #   "classification": string
    #   "custom_keys": list (may be empty)
    did_class = {}

    print("Classifying DIDs")
    for did, info in did_info.items():
        keys = info["keys"]
        pds_set = info["pds"]

        if not pds_set:
            did_class[did] = {
                "pds": [],
                "classification": "no_pds",
                "custom_keys": [],
            }
            continue

        if len(pds_set) > 1:
            did_class[did] = {
                "pds": sorted(pds_set),
                "classification": "multi_pds_unsupported",
                "custom_keys": [],
            }
            continue

        endpoint = next(iter(pds_set))
        ptype = pds_type.get(endpoint, "unknown")

        if ptype == "solo_pds":
            classification = "solo_pds"
            custom_keys = []

        elif ptype == "unique_pds":
            classification = "unknown_unique_pds"
            custom_keys = []

        elif ptype == "shared_pds":
            key_counts = pds_info[endpoint]["key_counts"]
            custom_keys = sorted(
                k for k in keys if key_counts.get(k, 0) == 1
            )
            if custom_keys:
                classification = "likely_custom"
            else:
                classification = "likely_pds_managed"

        else:
            classification = "unknown"
            custom_keys = []

        did_class[did] = {
            "pds": [endpoint],
            "classification": classification,
            "custom_keys": custom_keys,
        }

    # === OUTPUT ===

    # Rotation key summary
    if args.rotation_summary_filename:
        print(f"Writing rotation key summary to {args.rotation_summary_filename}")
        with open(args.rotation_summary_filename,'w',newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["rotation_key","file_count","endpoints"])
            for rk, info in sorted(
                agg.items(),
                key=lambda kv: (-kv[1]["file_count"], kv[0]),
            ):
                endpoints = ";".join(sorted(info["endpoints"]))
                csvwriter.writerow([rk, info['file_count'], endpoints])

    # PDS classification summary
    if args.pds_summary_filename:
        print(f"Writing PDS classifiation to {args.pds_summary_filename}")
        with open(args.pds_summary_filename,'w',newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["pds_endpoint","pds_type","did_count","rotation_key_count"])
            for endpoint, info in sorted(pds_info.items()):
                did_count = len(info["dids"])
                key_count = len(info["key_counts"])
                csvwriter.writerow([endpoint,pds_type.get(endpoint, 'unknown'),did_count,key_count])

    # DID-level classification
    if args.did_classification_filename:
        print(f"Writing DID classifiation to {args.did_classification_filename}")
        with open(args.did_classification_filename,'w',newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["did","pds_endpoint","classification","custom_keys"])

            for did, info in sorted(did_class.items()):
                pds_str = ";".join(info["pds"])
                custom_keys_str = ";".join(info["custom_keys"])
                csvwriter.writerow([did,pds_str,info['classification'],custom_keys_str])

    # Special solo PDS rules
    if args.solo_users_filename:
        print(f"Writing solo PDS users to {args.solo_users_filename}")
        #solo_pds = {ep for ep, dids in pds_dids.items() if len(dids) == 1}
        solo_pds = {ep for ep, typ in pds_type.items() if typ == "solo_pds"}
        with open(args.solo_users_filename,'w',newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["did", "pds_endpoint", "num_keys"])
            # For each solo PDS, check its single DID's latest key count
            for ep in sorted(solo_pds):
                info = pds_info[ep]
                keycount = len(info["key_counts"])
                did = next(iter(info["dids"]))
                csvwriter.writerow([did,ep,keycount])

if __name__ == "__main__":
    main()

