#!/usr/bin/env python3
import sys
import argparse
import time
import json
from pathlib import Path
from urllib import request, error


BASE_URL = "https://plc.directory"


def fetch_audit_log(did: str) -> dict:
    """Fetch the audit log JSON for a single DID from plc.directory."""
    url = f"{BASE_URL}/{did}/log/audit"
    req = request.Request(url, headers={"Accept": "application/json"})
    with request.urlopen(req) as resp:
        # plc.directory returns JSON; decode and load it
        charset = resp.headers.get_content_charset() or "utf-8"
        data = resp.read().decode(charset)
        return json.loads(data)


def main():
    parser = argparse.ArgumentParser(
        description="Fetch plc.directory audit logs for DIDs from stdin."
    )
    parser.add_argument(
        "--sleep",
        "--delay",
        type=float,
        default=1.0,
        help="Seconds to sleep between lookups (default: 1.0)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("."),
        help="Directory to write JSON files into (default: current directory)",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    first = True
    for line in sys.stdin:
        did = line.strip()
        if not did or did.startswith("#"):
            continue  # skip empty lines and comments

        out_path = args.output_dir / f"{did}.json"

        if out_path.exists():
            print(f"Skipping {did}: {out_path} already exists", file=sys.stderr)
            continue

        if not first:
            time.sleep(args.sleep)
        first = False

        print(f"Fetching {did} ...", file=sys.stderr)
        try:
            data = fetch_audit_log(did)
        except error.HTTPError as e:
            print(f"HTTP error for {did}: {e.code} {e.reason}", file=sys.stderr)
            continue
        except error.URLError as e:
            print(f"URL error for {did}: {e.reason}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"Unexpected error for {did}: {e}", file=sys.stderr)
            continue

        try:
            with out_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"Wrote {out_path}", file=sys.stderr)
        except Exception as e:
            print(f"Failed to write {out_path}: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()

