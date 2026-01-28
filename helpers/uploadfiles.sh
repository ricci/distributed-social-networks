#!/bin/sh

AWDY=/home/ricci/are-we-decentralized-yet/
SSH="ssh -i \"$AWDY/private/id_k8scopy\""
RDIR=/usr/share/nginx/html

rsync -avz --copy-links -e "$SSH" $AWDY/www/ rsync@10.1.0.9:$RDIR
rsync -avz -e "$SSH" $AWDY/data/at-mau rsync@10.1.0.9:$RDIR/data
rsync -avz -e "$SSH" $AWDY/data/fedi-mau rsync@10.1.0.9:$RDIR/data
rsync -avz -e "$SSH" $AWDY/data/fedi-software rsync@10.1.0.9:$RDIR/data
rsync -avz -e "$SSH" $AWDY/data/bsky-verifiers rsync@10.1.0.9:$RDIR/data
rsync -avz -e "$SSH" $AWDY/data/at-relay-report rsync@10.1.0.9:$RDIR/data
#rsync -avz -e "$SSH" $AWDY/data/bluesky-relay.json rsync@10.1.0.9:$RDIR/data/at-mau-snapshots/
#rsync -avz -e "$SSH" $AWDY/data/blacksky-relay.json rsync@10.1.0.9:$RDIR/data/at-mau-snapshots/
#rsync -avz -e "$SSH" $AWDY/data/nodelists rsync@10.1.0.9:$RDIR/data/
#rsync -avz -e "$SSH" $AWDY/data/nodeinfo rsync@10.1.0.9:$RDIR/data/

