#!/bin/sh
export NODE_IP=$(hostname -i)
export NODE_PORT=10009
exec "$@"
