#!/usr/bin/env python3

import argparse
import csv
import json
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


COMBINE_HOSTS = [[
    "mastodon.social", "mastodon.online"
]]
COMBINE_SUFFIXES = [ ".host.bsky.network" ]

def combine_key(row):
    domain = get_domain(row)
    for hlist in COMBINE_HOSTS:
        if domain in hlist:
            return hlist[0]
    for suffix in COMBINE_SUFFIXES:
        if domain.endswith(suffix):
            return suffix
    return domain

# Software known to misreport user accounts
SKIPPED_SOFTWARE = ["nodebb", "gotosocial", "yellbot","misskey", "sharkey"]
def f_software(row):
    if "software" not in row:
        return True
    else:
        return all([s not in row["software"].lower() for s in SKIPPED_SOFTWARE]) 
def f_count(row):
    return get_usercount(row) > 0
        
def normalize_keys(row):
    return {k.lower(): v for k, v in row.items()}

def extract_domain_counts(row):
    return { "domain": get_domain(row), "count": get_usercount(row) }


# Different CSVs use different names for the user count field
def get_usercount(row):
    for key in ("user_count", "mau", "accountcount"):
        val = row.get(key, "")
        if val != "":
            return int(val)
    return 0

# Different CSVs use different columns for the hostname
def get_domain(row):
    for key in ("domain", "hostname"):
        if key in row:
            return row.get(key, "")
    return None

def filter_rows(rows):
    rows = [normalize_keys(r) for r in rows]
    rows = [r for r in rows if f_count(r)]
    rows = [r for r in rows if f_software(r)]

    return rows

def combine_rows(rows):
    combined = dict()
    for row in rows:
        key = combine_key(row)
        combined[key] = combined.get(key,[]) + [row]
    
    newrows = list()
    for k,v in combined.items():
        newrows.append({"domain": k, "count": sum([r["count"] for r in v])})
    return newrows

def main(filename, json_out = False):
    with open(filename, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    rows = filter_rows(rows)

    extracted = [extract_domain_counts(row) for  row in rows]
    combined = combine_rows(extracted)

    user_counts = sorted([r["count"]  for r in combined], reverse=True)

    hhi = calc_hhi(user_counts)
    shannon = calc_shannon(user_counts)
    simpson = calc_simpson(user_counts)
    bs = [(b, calc_B(user_counts,b)) for b in [25,50,75,90,99] ]
    servers = len(user_counts)
    biggest_abs = user_counts[0]
    biggest_pct = 100*user_counts[0]/sum(user_counts)
    rest_abs = sum(user_counts[1:])
    rest_pct = 100*rest_abs/sum(user_counts)

    if json_out:
        print(json.dumps({"HHI": hhi,
                          "shannon": shannon,
                          "simpson": simpson,
                          "servers": servers,
                          "biggest_abs": biggest_abs,
                          "biggest_pct": biggest_pct,
                          "rest_abs": rest_abs,
                          "rest_pct": rest_pct,
                          "b_vals": bs}))
    else:
        print(f"HHI for user_count: {hhi:.4f}")
        print(f"Shannon Diversity for user_count: {shannon:.4f}")
        print(f"Simpson Diversity for user_count: {simpson:.4f}")
        print(f"Total servers: {servers}")
        print(f"Biggest server: {biggest_abs} ({biggest_pct:.2f}%)")
        print(f"Rest of the servers: {rest_abs} ({rest_pct:.2f}%)")
        print(f"B values are {bs}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog=f"{sys.argv[0]}",
                    description='Calculates statistics for social networks')
    parser.add_argument('csvfile')
    parser.add_argument('--json', action='store_true')

    args = parser.parse_args()
    main(args.csvfile, args.json)

