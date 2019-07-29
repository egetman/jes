#!/usr/bin/env bash

for pid in $(pgrep -f 'read-part|write-part'); do kill -9 "$pid"; done

docker stop postgres-store
