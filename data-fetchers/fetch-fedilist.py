#!/usr/bin/env python3

import requests
import csv
import sys
from bs4 import BeautifulSoup
from pathlib import Path

URL = "https://fedilist.com/instance"

OUTPUT_FILE = (Path(__file__).parent / "../data-static/fedilist-fromhtml.csv").resolve()

USERAGENT = "curl/7.54.1"

if __name__ == "__main__":
    outfile = outfile = sys.argv[1] if len(sys.argv) == 2 else OUTPUT_FILE

    headers = { "User-Agent": USERAGENT }
    # verify=False should be temporary, they let their cert expire
    r = requests.get(URL, headers=headers, timeout=30, verify=False)
    r.raise_for_status()

    soup = BeautifulSoup(r.content, "html.parser")

    table = soup.select_one("table.instance-list")

    # Extract headers
    header_cells = table.select("tr th")
    headers = [th.get_text(strip=True) for th in header_cells]

    # Extract data rows
    rows = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue  # skip header or empty rows
        row = [td.get_text(separator=" ", strip=True) for td in tds]
        rows.append(row)

    # Write CSV to stdout
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(headers)
        writer.writerows(rows)
