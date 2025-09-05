#!/usr/bin/ env python3

import requests
import csv
import sys

URL = "https://relay1.us-east.bsky.network/xrpc/com.atproto.sync.listHosts"
OUTPUT_FILE = "atproto-bsky-relay.csv"

def fetch_all():
    cursor = None
    all_hosts = []
    pages = 0

    while True:
        params = {}
        if cursor:
            params["cursor"] = cursor

        r = requests.get(URL, params=params, timeout=30)
        r.raise_for_status()

        data = r.json()
        hosts = data.get("hosts", [])

        # Filter out 'offline' hosts as suggested by @bnewbold.net
        hosts = [h for h in hosts if h.get("status") != "offline"]

        all_hosts.extend(hosts)
        pages += 1
        print(f"Fetched page {pages} (+{len(hosts)} hosts), cursor={data.get('cursor')}")

        cursor = data.get("cursor")
        if not cursor:
            break

    return all_hosts

if __name__ == "__main__":
    outfile = outfile = sys.argv[1] if len(sys.argv) == 2 else OUTPUT_FILE

    hosts = fetch_all()

    fieldnames = ["hostname", "status", "accountCount", "seq"]
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for host in hosts:
            row = {k: host.get(k, "") for k in fieldnames}
            writer.writerow(row)

    print(f"Saved {len(hosts)} hosts")

