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
        # user numbers; shouldn't affect atproto counts since there are
        # no name collisions
        cleaned_reader = [row for row in reader if row["software"] not in
            ["NodeBB", "gotosocial", "Yellbot","misskey", "sharkey"]]
        user_counts = [int(row["user_count"])
            if row["user_count"] else 0 for row in cleaned_reader]

    # Remove clearly bogus data with < 0 users; also sort just in 
    # case we ever want to compute CDFs or anything
    user_counts = [a for a in sorted(user_counts) if a > 0]

    hhi = calc_hhi(user_counts)
    print(f"HHI for user_count: {hhi:.4f}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <csvfile>")
        sys.exit(1)
    main(sys.argv[1])

