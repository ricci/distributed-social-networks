#!/usr/bin/env python3
"""Build a compact per-section time series (www/history.js) from the dated
snapshots in data/historical/, for the index-page sparklines."""

import argparse
import glob
import json
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HISTORY_DIR = REPO_ROOT / "data" / "historical"
DEFAULT_OUT = REPO_ROOT / "www" / "history.js"


def parse_snapshot_datetime(path):
    """Parse the timestamp from a snapshot filename (e.g. 2026-01-14T05:28:51Z).

    Returns None for anything that isn't a valid timestamp, which skips the
    'latest' symlink and any other stray files.
    """
    stem = Path(path).stem
    if stem.endswith("Z"):
        stem = stem[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(stem)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def is_section(value):
    return isinstance(value, dict) and "HHI" in value and "shannon" in value


def collect(history_dir):
    """Return a chronological list of (date, {section -> {'shannon','hhi'}})."""
    points = []
    for path in glob.glob(str(Path(history_dir) / "*.json")):
        dt = parse_snapshot_datetime(path)
        if dt is None:
            continue
        try:
            with open(path) as handle:
                snap = json.load(handle)
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(snap, dict):
            continue
        metrics = {
            name: {
                "shannon": value["shannon"],
                "hhi": value["HHI"],
                "raw": value.get("dataFile"),
            }
            for name, value in snap.items()
            if is_section(value)
        }
        points.append((dt, metrics))
    points.sort(key=lambda p: p[0])
    return points


def build_series(points):
    sections = {}
    for dt, metrics in points:
        stamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        for name, m in metrics.items():
            s = sections.setdefault(name, {"t": [], "shannon": [], "hhi": [], "raw": []})
            s["t"].append(stamp)
            s["shannon"].append(m["shannon"])
            s["hhi"].append(m["hhi"])
            s["raw"].append(m.get("raw"))
    # Drop sections that never move -- the static datasets.
    return {n: s for n, s in sections.items() if len(set(s["shannon"])) > 1}


def write_history_js(path, sections):
    payload = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "sections": sections,
    }
    text = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    # Not `history` -- that collides with the read-only window.history global.
    Path(path).write_text(f"var historyData = {text}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-dir", default=str(DEFAULT_HISTORY_DIR),
                        help="Directory of dated snapshot JSON files")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="Output JS file")
    args = parser.parse_args()

    points = collect(args.history_dir)
    if not points:
        raise SystemExit(f"No usable snapshots found in {args.history_dir}")
    sections = build_series(points)
    write_history_js(args.out, sections)

    print(f"Read {len(points)} snapshots: "
          f"{points[0][0]:%Y-%m-%d} .. {points[-1][0]:%Y-%m-%d}")
    print(f"Wrote {args.out}")
    for name in sorted(sections):
        print(f"  {name:16s} {len(sections[name]['t'])} points")


if __name__ == "__main__":
    main()
