#!/bin/sh

ATFILE="data/at/$(./newest.sh data/at)"
FEDIFILE="data/fedi/$(./newest.sh data/fedi)"
GITFILE="data/git/$(./newest.sh data/git)"

ATFILE_OLD="data/at/$(ls -1 data/at |  ./weekago.sh)"
FEDIFILE_OLD="data/fedi/$(ls -1 data/fedi | ./weekago.sh)"
GITFILE_OLD="data/git/$(ls -1 data/git | ./weekago.sh)"

/usr/local/bin/jq --argjson val "$(python3 hhi.py --json $FEDIFILE)" '.fedi = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --argjson val "$(python3 hhi.py --json $ATFILE)" '.at = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --argjson val "$(python3 hhi.py --json $GITFILE)" '.git = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --argjson val "$(sh ./diff-from-current.sh fedi $FEDIFILE $FEDIFILE_OLD)" '.trends.fedi.weekly_shannon = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --argjson val "$(sh ./diff-from-current.sh at $ATFILE $ATFILE_OLD)" '.trends.at.weekly_shannon = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --argjson val "$(sh ./diff-from-current.sh fedi $GITFILE $GITFILE_OLD)" '.trends.git.weekly_shannon = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --arg val "$(date +"%m-%d-%Y")" '.lastUpdate = $val' < www/data.json > data.json.tmp && mv data.json.tmp www/data.json

echo "var data =  $(cat www/data.json)" > www/data.js

