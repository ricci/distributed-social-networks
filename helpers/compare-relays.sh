#!/bin/sh

set -e

snap_dir="data/at-mau-watcher-snapshots"
ref="$snap_dir/bsky.network.json"
out_root="data/at-relay-report"

if [ ! -f "$ref" ]; then
  echo "Missing reference file: $ref" >&2
  exit 1
fi

set -- "$snap_dir"/*.json
if [ "$1" = "$snap_dir/*.json" ]; then
  echo "No .json files found in $snap_dir/." >&2
  exit 1
fi

timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

for path in "$@"; do
  filename=${path##*/}
  if [ "$filename" = "bsky.network.json" ]; then
    continue
  fi
  stem=${filename%.json}
  out_dir="$out_root/$stem"
  mkdir -p "$out_dir"
  output="$out_dir/$timestamp.json"
  python3 data-processing/at-mau/compare-userlists.py \
    "$ref" "$path" --days 1 --json > "$output"
  echo "Wrote $output"
done
