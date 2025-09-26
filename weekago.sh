#!/bin/sh
# Find file closest to one week ago using BSD tools only

DAYS=7
TARGET_EPOCH=$(date -j -v-"$DAYS"d +%s)

best_file=""
best_diff=""

while IFS= read -r line; do
  # Extract the first YYYY-MM-DDTHH:MM:SS substring
  TS=$(expr "$line" : '.*\([0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]\)')
  [ -n "$TS" ] || continue

  # Extract timezone if present (-06:00 → -0600), else local
  TZ=$(expr "$line" : '.*T[0-9:]\+\([-+][0-9][0-9]:[0-9][0-9]\)')
  [ -n "$TZ" ] && TZ=$(echo "$TZ" | tr -d :) || TZ=$(date +%z)

  # Convert to epoch; fallback to local if %z parse fails
  EPOCH=$(date -j -f "%Y-%m-%dT%H:%M:%S%z" "${TS}${TZ}" +%s 2>/dev/null \
          || date -j -f "%Y-%m-%dT%H:%M:%S" "$TS" +%s 2>/dev/null) || continue

  diff=$(( EPOCH > TARGET_EPOCH ? EPOCH - TARGET_EPOCH : TARGET_EPOCH - EPOCH ))

  if [ -z "$best_diff" ] || [ "$diff" -lt "$best_diff" ]; then
    best_diff=$diff
    best_file=$line
  fi
done

[ -n "$best_file" ] && echo "$best_file"

