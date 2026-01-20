#!/bin/sh
set -eu

apply=0
out_dir="data/fedi-mau"
in_dir="data/nodeinfo"

usage() {
    echo "Usage: $0 [--apply]" 1>&2
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --apply)
            apply=1
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            usage
            ;;
    esac
done

files_found=0

for path in "$out_dir"/*.csv; do
    [ -e "$path" ] || continue
    files_found=1
    base=$(basename "$path")
    ts=${base%.csv}
    backup="${path}.O"
    if [ -e "$backup" ]; then
        echo "Skipping $path (backup exists: $backup)"
        continue
    fi
    cmd="python3 data-processing/fedi-nodeinfo/parse-nodeinfo.py \"$in_dir\" \"$path\" --now \"$ts\""
    if [ "$apply" -eq 1 ]; then
        mv "$path" "$backup"
        sh -c "$cmd"
    else
        echo "Would run: mv \"$path\" \"$backup\""
        echo "Would run: $cmd"
    fi
done

if [ "$files_found" -eq 0 ]; then
    echo "No CSV files found in $out_dir"
fi
