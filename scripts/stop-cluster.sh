#!/usr/bin/env bash
# Stops the local cluster started by run-cluster.sh.
set -euo pipefail

cd "$(dirname "$0")/.."

for id in 1 2 3; do
    pidfile="data/node${id}.pid"
    if [[ -f "$pidfile" ]]; then
        pid="$(cat "$pidfile")"
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            echo "stopped node $id (pid $pid)"
        else
            echo "node $id not running"
        fi
        rm -f "$pidfile"
    else
        echo "node $id: no pidfile"
    fi
done
