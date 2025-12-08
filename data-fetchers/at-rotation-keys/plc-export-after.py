#!/usr/bin/env python3
import json
import os
import sys
import time
from urllib.parse import urlencode, unquote

import requests

BASE_URL = "https://plc.directory/export"


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <after> <outdir>")
        sys.exit(1)

    after = unquote(sys.argv[1])
    outdir = sys.argv[2]
    os.makedirs(outdir, exist_ok=True)

    batch = 0

    while True:
        batch += 1

        url = f"{BASE_URL}?{urlencode({'after': after})}"
        print(f"Fetching: {url}", file=sys.stderr)

        r = requests.get(url, timeout=300)
        r.raise_for_status()

        text = r.text.strip()
        if not text:
            print("No more data. Done.", file=sys.stderr)
            break

        outpath = os.path.join(outdir, f"batch_{batch:06d}.jsonl")
        with open(outpath, "w", encoding="utf-8") as f:
            f.write(text + "\n")

        print(f"Saved {outpath}", file=sys.stderr)

        # ✅ Read ONLY the last valid JSON line
        last_line = None
        with open(outpath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    last_line = line

        if not last_line:
            print("No valid JSON lines found. Stopping.", file=sys.stderr)
            break

        obj = json.loads(last_line)
        last_created = obj["createdAt"]

        print(f"Advancing after: {after} → {last_created}", file=sys.stderr)
        after = last_created

        time.sleep(0.2)  # be polite to the server


if __name__ == "__main__":
    main()

