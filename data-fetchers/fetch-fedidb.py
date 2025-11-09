#!/usr/bin/env python3

import requests
import csv
import sys
from pathlib import Path

URL = "https://api.fedidb.org/v1/servers"

OUTPUT_FILE = (Path(__file__).parent / "../data-static/fedidb-fromapi.csv").resolve()


if __name__ == "__main__":
    outfile = outfile = sys.argv[1] if len(sys.argv) == 2 else OUTPUT_FILE

    rows = []
    columns = ["id","domain","software","users_count","monthly_active_users"]

    nextURL = URL
    while True:
        r = requests.get(nextURL, params = {"limit": 40})
        r.raise_for_status()
        for instance in r.json()["data"]:
            rows.append([instance["id"],instance["domain"],instance["software"].get("name",""),
                         instance["stats"]["user_count"],instance["stats"]["monthly_active_users"]])
        if not r.json()["links"].get("next",None):
            break
        else:
            nextURL = r.json()["links"]["next"]

    # Write CSV to stdout
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(columns)
        writer.writerows(rows)
