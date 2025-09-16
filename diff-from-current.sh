#!/bin/bash

CSV=$1
WHICH=$2

CUR=$(jq < www/data.json .$WHICH.shannon)
PREV=$(python3 ./hhi.py --json $CSV | jq .shannon)

DIFF=$(echo "$CUR - $PREV" | bc -l)

echo $DIFF
