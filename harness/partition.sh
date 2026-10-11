#!/usr/bin/env bash
# One run under a real Partition (A§13.12): five Nodes in Docker, every
# link between them through toxiproxy; Nodes 1 and 2 are cut off from 3, 4
# and 5 part-way through, and the links restored. It is run for the
# consistent store of Rung 8 (three Groups) and for the Leaderless variant,
# with clients that each keep to one Node, so that the two can be set side
# by side: how much of its work each got done through the Partition, and
# what it cost.
#
#   harness/partition.sh <consistent|leaderless>
#
# Environment:
#   W=2 R=2      leaderless: how many Nodes a write and a read wait for
#   LWW=1        leaderless: settle writes made apart by last-write-wins
#   CLIENTS=16  DURATION=10s  FAULT_AT=3  FAULT_FOR=4   (seconds for the last two)
#   KEYS=50  ELEMS=8
#   TAG=name     added to the output file name
#
# Output is also saved to harness/out/partition-<store>.txt.
set -euo pipefail

STORE="${1:?usage: harness/partition.sh <consistent|leaderless>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/harness/out"
ctl="$ROOT/harness/docker.sh"
CLIENTS="${CLIENTS:-16}" DURATION="${DURATION:-10s}" FAULT_AT="${FAULT_AT:-3}" FAULT_FOR="${FAULT_FOR:-4}"
W="${W:-2}" R="${R:-2}"
nodes="http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003,http://127.0.0.1:8004,http://127.0.0.1:8005"

mkdir -p "$OUT" "$ROOT/bin"
go build -o "$ROOT/bin/kvbench" "$ROOT/cmd/kvbench"
case "$STORE" in
  consistent)
    # Replacing Nodes and moving Slots are left off: the run is about what
    # a Partition does, not about what the store then does about it.
    export KVNODE_ARGS="-data-groups 3 -slots 64 -forward-via toxiproxy:30000 -auto=false"
    what="consistent store, 3 data Groups and the Meta Group"
    bench=("$ROOT/bin/kvbench" -nodes "$nodes" -groups 3 -pin -keys "${KEYS:-50}")
    ready() { curl -s -m 2 -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:800$1/v1/kv/ready$2" -d '{"value":"1"}'; }
    ;;
  leaderless)
    export KVNODE_ARGS="-leaderless -w $W -r $R${LWW:+ -lww} -tick 50ms"
    what="Leaderless variant, W=$W R=$R${LWW:+, last-write-wins}"
    bench=("$ROOT/bin/kvbench" -leaderless -nodes "$nodes" -keys "${KEYS:-50}" -elems "${ELEMS:-8}")
    ready() { curl -s -m 2 -o /dev/null -w '%{http_code}' "http://127.0.0.1:800$1/v1/kv/ready$2"; }
    ;;
  *) echo "store must be consistent or leaderless" >&2; exit 2 ;;
esac

log="$OUT/partition-$STORE${TAG:+-$TAG}.txt"
{
  echo "== docker, 5 Nodes, $what; Nodes 1 and 2 cut off from 3, 4 and 5 for ${FAULT_FOR}s at ${FAULT_AT}s; $CLIENTS clients for $DURATION, each keeping to one Node"
  "$ctl" up 5 | tail -1
  trap '"$ctl" down >/dev/null 2>&1 || true' EXIT
  # Every Node must be answering, for keys of every Group, before the load.
  for _ in $(seq 1 100); do
    ok=1
    for n in 1 2 3 4 5; do for k in a b c d e f; do [[ "$(ready "$n" "$k")" == 200 ]] || ok=0; done; done
    [[ $ok == 1 ]] && break
    sleep 0.3
  done
  [[ $ok == 1 ]] || { echo "the store wasn't ready after 30s" >&2; exit 1; }
  set +e
  "${bench[@]}" -clients "$CLIENTS" -duration "$DURATION" -mark "${FAULT_AT}s" -mark-for "${FAULT_FOR}s" &
  pid=$!
  sleep "$FAULT_AT"
  "$ctl" split "1 2"
  sleep "$FAULT_FOR"
  "$ctl" heal
  wait "$pid"
  status=$?
  set -e
  exit "$status"
} 2>&1 | tee "$log"
exit "${PIPESTATUS[0]}"
