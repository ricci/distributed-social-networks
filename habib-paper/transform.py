#!/usr/bin/env python3
import csv
import sys
from collections import Counter

if len(sys.argv) != 3:
    print(f"Usage: {sys.argv[0]} input.csv output.csv")
    sys.exit(1)

input_file, output_file = sys.argv[1], sys.argv[2]

counts = Counter()
rowcount = 0

with open(input_file, newline='', encoding='utf-8') as infile:
    reader = csv.DictReader(infile)
    for row in reader:
        rowcount += 1
        if not (rowcount % 100000):
            print(rowcount)
        # Different files use different columns as key
        found_key = next((k for k in ["org_id","as_name","CA"] if k in row), None)
        org_id = row.get(found_key)
        if org_id:
            counts[org_id] += 1

with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(['org_id', 'count'])
    for org_id, count in counts.items():
        writer.writerow([org_id, count])

