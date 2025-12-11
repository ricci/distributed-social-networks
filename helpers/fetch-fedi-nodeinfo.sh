#!/bin/bash

TIMESTAMP=`date +"%Y-%m-%dT%H:%M:%S"`

echo "Running at $TIMESTAMP"

NODESFILE="data/nodelists/${TIMESTAMP}.json"

echo "Fetching fediparty nodelist to ${NODESFILE}"
/usr/local/bin/curl https://nodes.fediverse.party/nodes.json -o ${NODESFILE}

if [ ! -f ${NODESFILE} ]; then
    echo "${NODESFILE} not created properly"
    exit 1
fi

echo "Fetching nodeinfo"
python3 ./data-fetchers/fedi-nodeinfo/fetch-nodeinfo.py ${NODESFILE} data/nodeinfo data/nodeinfo/state.json
