#!/usr/bin/env python3

import requests
import csv
import sys
from bs4 import BeautifulSoup
from pathlib import Path

URL = "https://archive.softwareheritage.org/coverage/"
OUTPUT_FILE = (Path(__file__).parent / "../data-static/sh-fromhtml.csv").resolve()


if __name__ == "__main__":
    outfile = outfile = sys.argv[1] if len(sys.argv) == 2 else OUTPUT_FILE

    r = requests.get(URL, timeout=30)
    r.raise_for_status()

    # Using lxml reduces the parse time from tens of minutes (python's) to
    # seconds, crazy
    soup = BeautifulSoup(r.content, "lxml")

    headers = ["instance", "type", "origins", "empty"]
    rows = []
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue  # skip header or empty rows
            # for now, only pay attention to git, we will deal with others
            # later
            row = [td.get_text(separator=" ", strip=True) for td in tds]
            if not row[1] == "git":
                continue
            # Hack to get real integers
            row[2] = int(row[2].replace(',',''))
            rows.append(row)

    # Write CSV 
    with open(outfile, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(headers)
        writer.writerows(rows)
