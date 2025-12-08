#!/usr/bin/env python3
import json, sys, urllib.parse

def load(fn):
    with open(fn) as f:
        return {v["handle"]: v for v in json.load(f).values()}

A = load(sys.argv[1])
B = load(sys.argv[2])

# helpers
def host(url):
    return urllib.parse.urlparse(url).hostname

only_in_A = {h: host(A[h]["pds"]) for h in A.keys() - B.keys()}
only_in_B = {h: host(B[h]["pds"]) for h in B.keys() - A.keys()}

print("### Handles only in", sys.argv[1])
for h, p in sorted(only_in_A.items()):
    print(f"{h:40}  {p}")

print("\n### Handles only in", sys.argv[2])
for h, p in sorted(only_in_B.items()):
    print(f"{h:40}  {p}")

