#!/usr/bin/env python3

# Extract all DIDs from a set of snapshot files

import sys
import json

pdses = set()

for path in sys.argv[1:]:
    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()
        obj = json.loads(content)
        pdses.update(p["pds"] for p in obj.values())

for pds in pdses:
    print(pds)
