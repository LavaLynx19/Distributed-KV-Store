#!/usr/bin/env bash
# One real run of the Leaderless variant (A§13.12), from start to verdict:
# start five Nodes, load them with kvbench -leaderless, optionally inject a
# Fault part-way and repair it, then report whether the Nodes ended
# Converged and whether an Acknowledged add was lost.
#
#   harness/leaderless.sh [fault]
#
# Faults (local processes can be stopped, not cut off from each other):
#   none         (default)
#   pause-nodes  freeze Nodes 1 and 2 for FAULT_FOR seconds, then let them
#                continue. To the other three it is as if two Nodes were cut
#                off; the clients of the two get no answer at all
#   kill-node    kill -9 Node 1, then start it again from its disk
#
# Environment:
#   W=2 R=2      how many Nodes a write and a read wait for
#   LWW=1        settle writes made apart by last-write-wins
#   CLIENTS=8  DURATION=10s  FAULT_AT=3  FAULT_FOR=3   (seconds for the last two)
#   KEYS=50  ELEMS=8
#   TICK=50ms    one core tick; the store's waits are counted in ticks
#   TAG=name     added to the output file name
#
# Output is also saved to harness/out/leaderless-<fault>.txt.
set -euo pipefail

FAULT="${1:-none}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/harness/out"
ctl="$ROOT/harness/local.sh"
CLIENTS="${CLIENTS:-8}" DURATION="${DURATION:-10s}" FAULT_AT="${FAULT_AT:-3}" FAULT_FOR="${FAULT_FOR:-3}"
W="${W:-2}" R="${R:-2}"

case "$FAULT" in
  none | pause-nodes | kill-node) ;;
  *) echo "unknown fault $FAULT" >&2; exit 2 ;;
esac

export LEADERLESS="-w $W -r $R${LWW:+ -lww}"
export KVNODE_FLAGS="${KVNODE_FLAGS:-} -tick ${TICK:-50ms}"
mkdir -p "$OUT" "$ROOT/bin"
go build -o "$ROOT/bin/kvbench" "$ROOT/cmd/kvbench"

log="$OUT/leaderless-$FAULT${TAG:+-$TAG}.txt"
{
  echo "== Leaderless variant, 5 Nodes, W=$W R=$R${LWW:+, last-write-wins}, fault $FAULT, $CLIENTS clients for $DURATION"
  "$ctl" start 5 | tail -1
  trap '"$ctl" stop >/dev/null 2>&1 || true' EXIT
  for _ in $(seq 1 50); do curl -s -m 1 "http://127.0.0.1:8005/v1/status" >/dev/null && break; sleep 0.1; done
  nodes="http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003,http://127.0.0.1:8004,http://127.0.0.1:8005"
  bench=("$ROOT/bin/kvbench" -leaderless -nodes "$nodes" -clients "$CLIENTS" -duration "$DURATION" -keys "${KEYS:-50}" -elems "${ELEMS:-8}")
  set +e
  if [[ $FAULT == none ]]; then
    "${bench[@]}"
    status=$?
  else
    "${bench[@]}" -mark "${FAULT_AT}s" -mark-for "${FAULT_FOR}s" &
    pid=$!
    sleep "$FAULT_AT"
    case "$FAULT" in
      pause-nodes) "$ctl" pause 1; "$ctl" pause 2; sleep "$FAULT_FOR"; "$ctl" resume 1; "$ctl" resume 2 ;;
      kill-node)   "$ctl" kill 1; sleep "$FAULT_FOR"; "$ctl" restart 1 ;;
    esac
    wait "$pid"
    status=$?
  fi
  set -e
  exit "$status"
} 2>&1 | tee "$log"
exit "${PIPESTATUS[0]}"
