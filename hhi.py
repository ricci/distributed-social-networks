#!/usr/bin/env python3

import csv
import sys

# https://en.wikipedia.org/wiki/Herfindahl%E2%80%93Hirschman_index
def calc_hhi(x):
    total = sum(x)
    hhi = sum([(a/total)**2 for a in x])
    return hhi

def main(filename):
    with open(filename, newline="") as f:
        reader = csv.DictReader(f)
        # Remove some fediverse software that seems to report inaccurate
        # user numbers
        cleaned_reader = [row for row in reader
            if "software" not in row or row["software"] not in
             ["NodeBB", "gotosocial", "Yellbot","misskey", "sharkey"]]

        # Different CSVs have different row names, and the fediverse one
        # has some empty columns
        user_counts = [
            int(row["user_count"]) if "user_count" in row and row["user_count"] != ""
            else int(row["accountCount"]) if row.get("accountCount", "") != ""
            else 0
            for row in cleaned_reader
        ]

    # Remove clearly bogus data with < 0 users
    user_counts = [a for a in user_counts if a > 0]

    # Sort for some simple stats
    user_counts = sorted(user_counts, reverse=True)

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

