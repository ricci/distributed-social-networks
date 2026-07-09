#!/usr/bin/env python3

import argparse
import glob
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from centralization_stats import stats_from_csv

DATA_HISTORY_DIR = REPO_ROOT / "data" / "historical"


def main():
    parser = argparse.ArgumentParser(
        description="Recompute stored stats in data/historical/*.json from each "
        "section's raw CSV. Run after regenerating the CSVs (e.g. "
        "helpers/recalc-fedi-nodeinfo.sh --apply) or changing "
        "centralization_stats.py, then run build-history.py.")
    parser.add_argument("sections", nargs="*", default=["fedi"],
                        help="sections to recompute (default: fedi)")
    parser.add_argument("--history-dir", default=str(DATA_HISTORY_DIR),
                        help="directory of dated snapshot JSON files")
    parser.add_argument("--apply", action="store_true",
                        help="write the changes (default: dry run, report only)")
    args = parser.parse_args()
    sections = args.sections or ["fedi"]

    changed = 0
    missing = 0
    files = sorted(glob.glob(str(Path(args.history_dir) / "*.json")))
    for path in files:
        try:
            data = json.loads(Path(path).read_text())
        except (json.JSONDecodeError, OSError):
            continue

        file_changed = False
        for section in sections:
            entry = data.get(section)
            if not isinstance(entry, dict) or not entry.get("dataFile"):
                continue
            csv_path = REPO_ROOT / entry["dataFile"]
            if not csv_path.exists():
                print(f"missing {section} CSV for {Path(path).name}: {entry['dataFile']}")
                missing += 1
                continue
            stats = stats_from_csv(str(csv_path))
            if entry.get("shannon") != stats["shannon"] or entry.get("HHI") != stats["HHI"]:
                file_changed = True
                if not args.apply:
                    print(f"{Path(path).name} {section}: "
                          f"shannon {entry.get('shannon')} -> {stats['shannon']}, "
                          f"HHI {entry.get('HHI')} -> {stats['HHI']}")
            if args.apply:
                entry.update(stats)

        if file_changed:
            changed += 1
            if args.apply:
                Path(path).write_text(json.dumps(data, indent=2, ensure_ascii=True) + "\n")

    verb = "Updated" if args.apply else "Would update"
    print(f"{verb} {changed} of {len(files)} snapshots (sections: {', '.join(sections)})")
    if missing:
        print(f"{missing} section/CSV reference(s) missing and skipped")


if __name__ == "__main__":
    main()
