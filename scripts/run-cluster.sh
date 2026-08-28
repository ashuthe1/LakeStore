#!/usr/bin/env bash
# Starts a local 3-node LakeStore cluster on ports 7101-7103.
# Data lives under ./data/nodeN, logs under ./data/nodeN.log,
# PIDs under ./data/nodeN.pid. Stop with scripts/stop-cluster.sh.
set -euo pipefail

cd "$(dirname "$0")/.."

BIN=./bin/lakestored
PEERS="1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103"
# Set WRITE_BUFFER (bytes) to a small value (e.g. 262144) to watch memtable
# flushes and compactions happen live in the inspector UI.
WRITE_BUFFER="${WRITE_BUFFER:-0}"

if [[ ! -x "$BIN" ]]; then
    echo "error: $BIN not built — run 'make build' first" >&2
    exit 1
fi

mkdir -p data

for id in 1 2 3; do
    pidfile="data/node${id}.pid"
    if [[ -f "$pidfile" ]] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
        echo "node $id already running (pid $(cat "$pidfile"))"
        continue
    fi
    port=$((7100 + id))
    debug_port=$((8100 + id))
    "$BIN" \
        --id "$id" \
        --listen "127.0.0.1:${port}" \
        --debug-listen "127.0.0.1:${debug_port}" \
        --data-dir "data/node${id}" \
        --peers "$PEERS" \
        --engine-write-buffer "$WRITE_BUFFER" \
        >> "data/node${id}.log" 2>&1 &
    echo $! > "$pidfile"
    echo "started node $id on 127.0.0.1:${port} (pid $!, inspector http://127.0.0.1:${debug_port})"
done

echo
echo "Cluster is starting. Try:"
echo "  ./bin/lakectl status"
echo "  ./bin/lakectl put hello world"
echo "  ./bin/lakectl get hello"
echo "  open http://127.0.0.1:8101   # inspector UI (memtable, SSTables, raft log)"
