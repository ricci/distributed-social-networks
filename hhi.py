#!/usr/bin/env python3

import csv
import sys

# https://en.wikipedia.org/wiki/Herfindahl%E2%80%93Hirschman_index
def calc_hhi(x):
    total = sum(x)
    hhi = sum([(a/total)**2 for a in x])
    return hhi

# Software knows to misreport user accounts
skipped_software = ["NodeBB", "gotosocial", "Yellbot","misskey", "sharkey"]

# Different CSVs use different names for the user count field
def get_usercount(row):
    for key in ("user_count", "mau", "accountcount"):
        val = row.get(key, "")
        if val != "":
            return int(val)
    return 0

def normalize_keys(row):
    return {k.lower(): v for k, v in row.items()}

def filter_rows(rows):
    skiplist = [a.lower() for a in skipped_software]
    for row in rows:
        
        # Normalize header case
        row = normalize_keys(row)

        # Skip negative or zero-user sites
        usercount = get_usercount(row)
        if usercount <= 0:
            continue

        # For fedi instances, skip ones with software known to provide
        # inaccurate user counts; skip this for atproto
        if not "software" in row.keys():
            yield usercount
            continue

        if not any(token.lower() in row["software"].lower() for token in skiplist):
            yield usercount
            continue

def main(filename):
    with open(filename, newline="") as f:
        reader = csv.DictReader(f)

        cleaned_reader = filter_rows(reader)

        user_counts = sorted([ count for count in cleaned_reader ], reverse=True)

    hhi = calc_hhi(user_counts)
    print(f"HHI for user_count: {hhi:.4f}")
    print(f"Total servers: {len(user_counts)}")
    print(f"Biggest server: {user_counts[0]} ({100*user_counts[0]/sum(user_counts):.2f}%)")
    print(f"Rest of the servers: {sum(user_counts[1:])} ({100*sum(user_counts[1:])/sum(user_counts):.2f}%)")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <csvfile>")
        sys.exit(1)
    main(sys.argv[1])

