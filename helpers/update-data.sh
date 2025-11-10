#!/bin/bash

python3 ./fetch-bsky.py &&
    python3 ./fetch-fedilist.py &&
    python3 ./fetch-sh.py &&
    jq --argjson val "$(python3 hhi.py --json fedilist-fromhtml.csv)" '.fedi = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json &&
    jq --argjson val "$(python3 hhi.py --json atproto-bsky-relay.csv)" '.at = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json &&
    jq --argjson val "$(python3 hhi.py --json sh-fromhtml.csv)" '.git = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json &&
    jq --arg val "$(date +'%m-%d-%y')" '.lastUpdate = $val' www/data.json > data.json.tmp && mv data.json.tmp www/data.json &&
    echo "var data =  $(cat www/data.json)" > www/data.js

    
    

