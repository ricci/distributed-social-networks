#!/bin/sh

out_dir="data/fedi-mau"
in_dir="data/nodeinfo"


mkdir -p "$out_dir"

timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
output="$out_dir/$timestamp.csv"

python3 data-processing/fedi-nodeinfo/parse-nodeinfo.py $in_dir $output

echo "Wrote $output"
