#!/bin/sh
# Usage: ./newest.sh < filelist.txt

ls -1 $1 | awk -F'__' '
{
  ts = $1
  # Strip timezone if present
  sub(/-[0-9][0-9]:[0-9][0-9]$/, "", ts)
  gsub("T", " ", ts)
  file = $0

  # Call BSD date: -j = don’t set system clock, -f = input format
  cmd = "date -j -f \"%Y-%m-%d %H:%M:%S\" \"" ts "\" +%s"
  cmd | getline epoch
  close(cmd)

  if (epoch > max_epoch) {
    max_epoch = epoch
    newest = file
  }
}
END {
  print newest
}
'

