#!/bin/sh

cd /home/ricci/are-we-decentralized-yet/www && tar cvf - * | kubectl exec -i nginx-distributed-writer-0 -- tar xf - -C /usr/share/nginx/html
