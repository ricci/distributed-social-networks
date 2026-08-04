#!/usr/bin/env python3

import requests
import csv
import sys
from pathlib import Path

URL = "https://repos.ecosyste.ms/api/v1/hosts"
OUTPUT_FILE = (Path(__file__).parent / "../data-static/ecosystems-hosts.csv").resolve()
PER_PAGE = 1000


def fetch_all_hosts():
    hosts = []
    page = 1
    while True:
        r = requests.get(URL, params={"page": page, "per_page": PER_PAGE}, timeout=30)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        hosts.extend(batch)
        page += 1
    return hosts


if __name__ == "__main__":
    outfile = sys.argv[1] if len(sys.argv) == 2 else OUTPUT_FILE

    hosts = fetch_all_hosts()

    headers = ["hostname", "kind", "count"]
    rows = []
    for host in hosts:
        count = host.get("repositories_count") or 0
        if count <= 0:
            continue
        rows.append([host.get("name", ""), host.get("kind", ""), count])

    rows.sort(key=lambda row: row[2], reverse=True)

    # Write CSV
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(headers)
        writer.writerows(rows)
