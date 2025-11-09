#!/bin/bash

TIMESTAMP=`date +"%Y-%m-%dT%H:%M:%S"`

echo "Running at $TIMESTAMP"

echo "Fetching BlueSky data"
python3 ./data-fetchers/fetch-bsky.py data/at/${TIMESTAMP}__atproto-bsky-relay.csv

echo "Fetching Fedi data (fedilist)"
python3 ./data-fetchers/fetch-fedilist.py data/fedi/${TIMESTAMP}__fedilist-fromhtml.csv

echo "Fetching Git data"
python3 ./data-fetchers/fetch-sh.py data/git/${TIMESTAMP}__sh-fromhtml.csv
