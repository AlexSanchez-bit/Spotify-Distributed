#!/bin/sh
export NODE_IP=$(hostname -i)
export NODE_PORT=19009
exec "$@"
