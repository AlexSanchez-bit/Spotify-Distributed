#!/bin/sh
export NODE_IP=$(hostname -i)
exec "$@"
