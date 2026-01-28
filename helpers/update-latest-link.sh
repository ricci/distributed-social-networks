#!/bin/sh

set -e

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <dir> [dir ...]" >&2
  exit 1
fi

for dir in "$@"; do
  if [ ! -d "$dir" ]; then
    echo "Not a directory: $dir" >&2
    continue
  fi

  latest_name=$(
    find "$dir" -maxdepth 1 -type f -printf '%f\n' \
      | grep -v '^latest$' \
      | sort \
      | tail -n 1
  )

  if [ -z "$latest_name" ]; then
    echo "No files found in $dir" >&2
    continue
  fi

  ln -sfn "$latest_name" "$dir/latest"
  echo "Linked $dir/latest -> $latest_name"
done
