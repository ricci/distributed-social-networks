#!/bin/bash

WHICH=$1
CSV=$2
CSV2=$3

CUR=$(python3 ./centralization-stats.py --json $CSV | jq .shannon)
PREV=$(python3 ./centralization-stats.py --json $CSV2 | jq .shannon)

DIFF=$(echo "$CUR - $PREV" | bc -l)

echo $DIFF
