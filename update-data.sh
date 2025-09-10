#!/bin/bash

python3 ./fetch-bsky.py &&
    python3 ./fetch-fedilist.py &&
    python3 ./fetch-sh.py &&
    jq --argjson val "$(python3 hhi.py --json fedilist-fromhtml.csv)" '.fedi = $val' www/data.js > data.js.tmp && mv data.js.tmp www/data.js &&
    jq --argjson val "$(python3 hhi.py --json atproto-bsky-relay.csv)" '.at = $val' www/data.js > data.js.tmp && mv data.js.tmp www/data.js &&
    jq --argjson val "$(python3 hhi.py --json sh-fromhtml.csv)" '.git = $val' www/data.js > data.js.tmp && mv data.js.tmp www/data.js &&
    jq --arg val "$(date +'%m-%d-%y')" '.lastUpdate = $val' www/data.js > data.js.tmp && mv data.js.tmp www/data.js &&
    echo "var data =  $(cat www/data.json)" > www/data.js

    
    

