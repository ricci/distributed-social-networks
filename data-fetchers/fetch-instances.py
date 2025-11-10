#!/usr/bin/env python3

import requests
import csv
import sys
import os
import json
from pathlib import Path

URL = "https://instances.social/api/1.0/instances/list?count=0"

OUTPUT_FILE = (Path(__file__).parent / "../data-static/instances-fromapi.csv").resolve()


if __name__ == "__main__":
    if not 'INSTANCES_API_TOKEN' in os.environ:
        sys.exit("INSTANCES_API_TOKEN environment variable required, see https://instances.social/api/token")

    outfile = outfile = sys.argv[1] if len(sys.argv) == 2 else OUTPUT_FILE

    headers = { "Authorization": f"Bearer {os.environ['INSTANCES_API_TOKEN']}" }
    r = requests.get(URL, headers=headers)
    r.raise_for_status()

    rows = []
    columns = ["id","name","users","active_users"]
    for instance in r.json()["instances"]:
        rows.append([instance["id"],instance["name"],instance["users"],instance["active_users"]])

    # Write CSV to stdout
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(columns)
        writer.writerows(rows)
