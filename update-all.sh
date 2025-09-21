#!/bin/bash

TIMESTAMP=`date +"%Y-%m-%dT%H:%M:%S"`

echo "Running at $TIMESTAMP"

echo "Fetching BlueSky data"
python3 ./fetch-bsky.py data/at/${TIMESTAMP}_atproto-bsky-relay.csv

echo "Fetching Fedi data"
python3 ./fetch-fedilist.py data/fedi/${TIMESTAMP}_fedilist-fromhtml.csv

echo "Fetching Git data"
python3 ./fetch-sh.py data/git/${TIMESTAMP}_sh-fromhtml.csv
