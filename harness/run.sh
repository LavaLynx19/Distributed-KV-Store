#!/usr/bin/env bash
# One real run from start to verdict (A§8.3): start a Group, load it with
# kvbench, optionally inject a Fault part-way and repair it, then report the
# three verdicts. Exits non-zero if the History isn't Linearizable or Members
# end with different data.
#
#   harness/run.sh <local|docker> <members> [fault]
#
# Faults:
#   none             (default) no Fault; docker runs connect Members directly
#   pause-leader     freeze the Leader, then let it continue
#   isolate-leader   cut the Leader off from every other Member, then heal
#                    (docker only: it needs toxiproxy)
#
# Environment:
#   CLIENTS=8  DURATION=10s  FAULT_AT=3  FAULT_FOR=3   (seconds for the last two)
#
# Output is also saved to harness/out/run-<backend>-<members>-<fault>.txt.
set -euo pipefail

BACKEND="${1:?usage: harness/run.sh <local|docker> <members> [fault]}"
N="${2:?usage: harness/run.sh <local|docker> <members> [fault]}"
FAULT="${3:-none}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/harness/out"
CLIENTS="${CLIENTS:-8}" DURATION="${DURATION:-10s}" FAULT_AT="${FAULT_AT:-3}" FAULT_FOR="${FAULT_FOR:-3}"

case "$BACKEND" in
  local) ctl="$ROOT/harness/local.sh"; start=(start "$N"); stop=stop ;;
  docker)
    ctl="$ROOT/harness/docker.sh"; stop=down
    if [[ $FAULT == none ]]; then start=(up "$N" direct); else start=(up "$N"); fi ;;
  *) echo "backend must be local or docker" >&2; exit 2 ;;
esac
case "$FAULT" in
  none | pause-leader) ;;
  isolate-leader) [[ $BACKEND == docker ]] || { echo "isolate-leader needs the docker backend" >&2; exit 2; } ;;
  *) echo "unknown fault $FAULT" >&2; exit 2 ;;
esac

mkdir -p "$OUT" "$ROOT/bin"
go build -o "$ROOT/bin/kvbench" "$ROOT/cmd/kvbench"
nodes=()
for i in $(seq 1 "$N"); do nodes+=("http://127.0.0.1:$((8000 + i))"); done

# leader prints the id of the Member that says it leads, or nothing.
leader() {
  for i in $(seq 1 "$N"); do
    if curl -s -m 1 "http://127.0.0.1:$((8000 + i))/v1/status" | grep -q '"role":"leader"'; then
      echo "$i"; return
    fi
  done
}

log="$OUT/run-$BACKEND-$N-$FAULT.txt"
{
  echo "== $BACKEND, $N Members, fault $FAULT, $CLIENTS clients for $DURATION"
  "$ctl" "${start[@]}" | tail -1
  trap '"$ctl" "$stop" >/dev/null 2>&1 || true' EXIT
  for _ in $(seq 1 100); do [[ -n "$(leader)" ]] && break; sleep 0.1; done
  [[ -n "$(leader)" ]] || { echo "no Leader after 10s" >&2; exit 1; }

  bench=("$ROOT/bin/kvbench" -nodes "$(IFS=,; echo "${nodes[*]}")" -clients "$CLIENTS" -duration "$DURATION")
  if [[ $FAULT == none ]]; then
    "${bench[@]}"
    status=$?
  else
    "${bench[@]}" -mark "${FAULT_AT}s" &
    pid=$!
    sleep "$FAULT_AT"
    target="$(leader)"
    case "$FAULT" in
      pause-leader)   "$ctl" pause "$target";   sleep "$FAULT_FOR"; "$ctl" resume "$target" ;;
      isolate-leader) "$ctl" isolate "$target"; sleep "$FAULT_FOR"; "$ctl" heal ;;
    esac
    set +e
    wait "$pid"
    status=$?
    set -e
  fi
  exit "$status"
} 2>&1 | tee "$log"
exit "${PIPESTATUS[0]}"
