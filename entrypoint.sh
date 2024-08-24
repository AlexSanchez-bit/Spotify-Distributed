#!/bin/sh
export NODE_IP=$(hostname -i)
export NODE_PORT=19009
mkdir /tmp/data
mkdir /tmp/songs
exec "$@"
