#!/bin/sh

ATFILE="data/at/$(./helpers/newest.sh data/at)"
FEDIFILE="data/fedi/$(./helpers/newest.sh data/fedi)"
GITFILE="data/git/$(./helpers/newest.sh data/git)"
HOSTFILE="worldwide.csv"
DNSFILE="dns-byid.csv"
CERTFILE="cert-byid.csv"

ATFILE_OLD="data/at/$(ls -1 data/at |  ./helpers/weekago.sh)"
FEDIFILE_OLD="data/fedi/$(ls -1 data/fedi | ./helpers/weekago.sh)"
GITFILE_OLD="data/git/$(ls -1 data/git | ./helpers/weekago.sh)"

/usr/local/bin/jq --argjson val "$(python3 centralization-stats.py --json $FEDIFILE)" '.fedi = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --argjson val "$(python3 centralization-stats.py --json $ATFILE)" '.at = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --argjson val "$(python3 centralization-stats.py --json $GITFILE)" '.git = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --argjson val "$(python3 centralization-stats.py --json $HOSTFILE)" '.hosting = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --argjson val "$(python3 centralization-stats.py --json $DNSFILE)" '.dns = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --argjson val "$(python3 centralization-stats.py --json $CERTFILE)" '.cert = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --argjson val "$(sh ./helpers/diff-from-current.sh fedi $FEDIFILE $FEDIFILE_OLD)" '.trends.fedi.weekly_shannon = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --argjson val "$(sh ./helphers/diff-from-current.sh at $ATFILE $ATFILE_OLD)" '.trends.at.weekly_shannon = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --argjson val "$(sh ./helpers/diff-from-current.sh fedi $GITFILE $GITFILE_OLD)" '.trends.git.weekly_shannon = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json

/usr/local/bin/jq --arg val "$(date +"%m-%d-%Y")" '.lastUpdate = $val' < www/data.json > data.json.tmp && mv data.json.tmp www/data.json

echo "var data =  $(cat www/data.json)" > www/data.js

