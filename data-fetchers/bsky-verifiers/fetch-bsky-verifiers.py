#!/usr/bin/env python3

import csv
from datetime import datetime, timezone
from pathlib import Path
import socket

import requests
from bs4 import BeautifulSoup
from urllib3.util import connection as urllib3_connection

URL = "https://bskycheck.com/stats.php"

def _force_ipv4():
    urllib3_connection.allowed_gai_family = lambda: socket.AF_INET


def _normalize_header(text):
    return " ".join(text.split()).strip().lower()


def find_verifiers_table(soup):
    for table in soup.find_all("table"):
        headers = [
            _normalize_header(th.get_text())
            for th in table.find_all("th")
        ]
        if "verifier" in headers and "users verified" in headers:
            return table
    return None


def extract_rows(table):
    rows = []
    body_rows = table.find_all("tr")
    for tr in body_rows:
        cells = tr.find_all("td")
        if len(cells) < 2:
            continue
        verifier = cells[0].get_text(strip=True)
        users_verified_text = cells[1].get_text(strip=True)
        users_verified = int(users_verified_text.replace(",", ""))
        rows.append((verifier, users_verified))
    return rows


def main():
    _force_ipv4()
    response = requests.get(URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    table = find_verifiers_table(soup)
    if table is None:
        raise RuntimeError("Could not find Trusted Verifiers table with expected headers.")

    rows = extract_rows(table)
    if not rows:
        raise RuntimeError("No rows found in Trusted Verifiers table.")

    repo_root = Path(__file__).resolve().parents[2]
    output_dir = repo_root / "data" / "bsky-verifiers"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    output_file = output_dir / f"{timestamp}.csv"

    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["verifier", "count"])
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {output_file}")


if __name__ == "__main__":
    main()
