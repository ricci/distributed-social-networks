#!/usr/bin/env python3

import argparse
import csv
import math
import sys

# https://en.wikipedia.org/wiki/Herfindahl%E2%80%93Hirschman_index
def calc_hhi(x):
    total = sum(x)
    hhi = sum([(a/total)**2 for a in x])
    return hhi

# https://www.statology.org/shannon-diversity-index/
def calc_shannon(x):
    total = sum(x)
    shannon = -sum([((a/total)*math.log(a/total,math.e)) for a in x])
    return shannon

#  https://statologos.com/indice-de-diversidad-de-los-simpson/
def calc_simpson(x):
    total = sum(x)
    simpson = 1 - sum([a*(a-1) for a in x]) / (total*(total-1))
    return simpson

def calc_B(x,n):
    assert(n<=100)
    total = sum(x)
    accum = 0
    for b in range(0, len(x)-1):
        accum += x[b]/total
        if (accum >= n/100.0):
            return(b+1)

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
    shannon = calc_shannon(user_counts)
    simpson = calc_simpson(user_counts)
    bs = [(b, calc_B(user_counts,b)) for b in [25,50,75,90,99] ]
    print(f"HHI for user_count: {hhi:.4f}")
    print(f"Shannon Diversity for user_count: {shannon:.4f}")
    print(f"Simpson Diversity for user_count: {simpson:.4f}")
    print(f"Total servers: {len(user_counts)}")
    print(f"Biggest server: {user_counts[0]} ({100*user_counts[0]/sum(user_counts):.2f}%)")
    print(f"Rest of the servers: {sum(user_counts[1:])} ({100*sum(user_counts[1:])/sum(user_counts):.2f}%)")
    print(f"B values are {bs}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog=f"{sys.argv[0]}",
                    description='Calculates statistics for social networks')
    parser.add_argument('csvfile')

    args = parser.parse_args()
    main(args.csvfile)

