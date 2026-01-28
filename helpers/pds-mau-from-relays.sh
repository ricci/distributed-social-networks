#!/bin/sh

set -e

out_dir="data/at-mau"

set -- data/at-mau-watcher-snapshots/*.json
if [ "$1" = "data/at-mau-watcher-snapshots/*.json" ]; then
  echo "No .json files found in data/at-mau-watcher-snapshots/." >&2
  exit 1
fi

mkdir -p "$out_dir"

timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
output="$out_dir/$timestamp.csv"

python3 data-processing/at-mau/pds-mau-from-snapshot.py "$@" --output "$output"

echo "Wrote $output"
