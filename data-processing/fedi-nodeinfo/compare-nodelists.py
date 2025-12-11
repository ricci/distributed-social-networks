#!/usr/bin/env python3
"""
Compare successive nodelist snapshots and report additions/removals.

Each snapshot is expected to be a JSON array of hostnames. The script walks
through the files in chronological order (based on filename sort) and shows
what changed compared to the previous snapshot.
"""

import argparse
import json
from pathlib import Path
from typing import Set


def load_snapshot(path: Path) -> Set[str]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"{path} does not contain a JSON list")

    return {str(item) for item in data}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Report added/removed entries between nodelist snapshots."
    )
    parser.add_argument(
        "nodelist_dir",
        nargs="?",
        default="data/nodelists",
        help="Directory containing timestamped nodelist JSON files.",
    )
    args = parser.parse_args()

    nodelist_dir = Path(args.nodelist_dir)
    if not nodelist_dir.is_dir():
        parser.error(f"{nodelist_dir} is not a directory")

    files = sorted(nodelist_dir.glob("*.json"))
    if len(files) < 2:
        parser.error("Need at least two JSON files to compare")

    previous_path = files[0]
    previous_snapshot = load_snapshot(previous_path)

    print(f"Base snapshot {previous_path.name}: {len(previous_snapshot)} entries")

    for current_path in files[1:]:
        current_snapshot = load_snapshot(current_path)

        added = sorted(current_snapshot - previous_snapshot)
        removed = sorted(previous_snapshot - current_snapshot)

        print(f"\nChanges for {current_path.name} (vs {previous_path.name})")
        print(f"Added ({len(added)}):")
        for item in added:
            print(f"  {item}")

        print(f"Removed ({len(removed)}):")
        for item in removed:
            print(f"  {item}")

        previous_snapshot = current_snapshot
        previous_path = current_path


if __name__ == "__main__":
    main()
