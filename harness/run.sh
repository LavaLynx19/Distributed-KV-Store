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
#   kill-leader      kill -9 the Leader, then start it again from its disk
#   restart-all      kill -9 every Member at once, then start them all again
#   corrupt-follower kill -9 a follower, flip a bit in its Log, start it again
#   corrupt-leader   the same to the Leader (both local only)
#   replace-follower kill -9 a follower for good, add a Spare in its place and
#                    remove the dead Member (local only; A§6.5)
#
# With DATA_GROUPS set the run is of a store with several Groups (local only;
# A§11), and the Faults are instead:
#   none             no Fault
#   move-slots       move a Slot to the next Group every half second for
#                    FAULT_FOR seconds, waiting for each to finish
#   kill-node        kill -9 Node 1, which hosts replicas of several Groups,
#                    then start it again
#   move-and-kill    both at once
#   kill-for-good    kill -9 Node 2, which hosts replicas of several Groups,
#                    and never start it again. One more Node is started as a
#                    Spare beforehand, and the store is left to put it in
#                    Node 2's place (A§11.11)
#   skewed-load      no Fault: four requests in five go to the keys of the
#                    Slots Group 1 starts with, and the store is left to move
#                    Slots off it. AUTO=0 turns that off, to compare
#
# Environment:
#   CLIENTS=8  DURATION=10s  FAULT_AT=3  FAULT_FOR=3   (seconds for the last two)
#   RETRY=1    clients open a Session and retry unanswered requests (A§6.3)
#   READ_PCT=35  percentage of requests that are gets
#   TAG=name   added to the output file name, to keep variants apart
#   READS=index|log|lease   how gets are answered (default index, A§6.2)
#   TTL_PCT=0  percentage of puts given a time-to-live (A§6.7)
#   NODATA=1   Members keep nothing on disk, as before Rung 3
#   DATA_GROUPS=3  SLOTS=64   a store with several Groups (see above)
#   AUTO=0     with DATA_GROUPS: the store replaces no Node and moves no Slot
#              by itself
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
  move-slots | kill-node | move-and-kill | kill-for-good | skewed-load)
    [[ -n "${DATA_GROUPS:-}" && $BACKEND == local ]] || { echo "$FAULT needs DATA_GROUPS and the local backend" >&2; exit 2; } ;;
  none | pause-leader | kill-leader | restart-all) ;;
  corrupt-follower | corrupt-leader | replace-follower) [[ $BACKEND == local ]] || { echo "$FAULT needs the local backend" >&2; exit 2; } ;;
  isolate-leader) [[ $BACKEND == docker ]] || { echo "isolate-leader needs the docker backend" >&2; exit 2; } ;;
  *) echo "unknown fault $FAULT" >&2; exit 2 ;;
esac

if [[ -n "${NODATA:-}" ]]; then
  export NODATA DATA_DIR=
fi
if [[ -n "${READS:-}" ]]; then
  export READS KVNODE_FLAGS="${KVNODE_FLAGS:-} -reads $READS"
fi

if [[ ${AUTO:-1} == 0 ]]; then
  export KVNODE_FLAGS="${KVNODE_FLAGS:-} -auto=false"
fi
if [[ $FAULT == kill-for-good ]]; then
  # Short enough for a ten-second run to see the replacement through.
  export KVNODE_FLAGS="${KVNODE_FLAGS:-} -dead-wait 2s"
fi

mkdir -p "$OUT" "$ROOT/bin"
go build -o "$ROOT/bin/kvbench" "$ROOT/cmd/kvbench"
# A replacement needs a Spare running beside the Group from the start.
TOTAL="$N"
if [[ $FAULT == replace-follower ]]; then
  TOTAL=$((N + 1))
  start=(start "$N" 1)
fi
nodes=()
for i in $(seq 1 "$TOTAL"); do nodes+=("http://127.0.0.1:$((8000 + i))"); done

# leader prints the id of the Member that says it leads, or nothing.
leader() {
  for i in $(seq 1 "$N"); do
    if curl -s -m 1 "http://127.0.0.1:$((8000 + i))/v1/status" | grep -q '"role":"leader"'; then
      echo "$i"; return
    fi
  done
}

log="$OUT/run-$BACKEND-$N-$FAULT${TAG:+-$TAG}.txt"
{
  echo "== $BACKEND, $N Members${DATA_GROUPS:+ hosting $DATA_GROUPS data Groups and the Meta Group}, fault $FAULT, $CLIENTS clients for $DURATION, reads by ${READS:-index}, ${READ_PCT:-35}% gets${RETRY:+, retrying in Sessions}"
  "$ctl" "${start[@]}" | tail -1
  trap '"$ctl" "$stop" >/dev/null 2>&1 || true' EXIT
  for _ in $(seq 1 100); do [[ -n "$(leader)" ]] && break; sleep 0.1; done
  [[ -n "$(leader)" ]] || { echo "no Leader after 10s" >&2; exit 1; }
  # In a store with several Groups every Group needs its Leader.
  [[ -z "${DATA_GROUPS:-}" ]] || sleep 1
  if [[ $FAULT == kill-for-good ]]; then
    "$ctl" join "$((N + 1))"
    nodes+=("http://127.0.0.1:$((8000 + N + 1))")
    sleep 1
  fi

  bench=("$ROOT/bin/kvbench" -nodes "$(IFS=,; echo "${nodes[*]}")" -clients "$CLIENTS" -duration "$DURATION")
  [[ -n "${RETRY:-}" ]] && bench+=(-retry)
  [[ -n "${READ_PCT:-}" ]] && bench+=(-read-pct "$READ_PCT")
  [[ -n "${TTL_PCT:-}" ]] && bench+=(-ttl-pct "$TTL_PCT")
  [[ -n "${DATA_GROUPS:-}" ]] && bench+=(-groups "$DATA_GROUPS")
  if [[ $FAULT == none ]]; then
    "${bench[@]}"
    status=$?
  elif [[ $FAULT == skewed-load ]]; then
    "${bench[@]}" -hot-pct 80 -hot-group 1 -slots "${SLOTS:-64}" &
    pid=$!
    # The load figures fade once the clients stop, so the table is read
    # just before they do.
    sleep "$((${DURATION%s} - 1))"
    "$ctl" table || true
    set +e
    wait "$pid"
    status=$?
    set -e
    grep -h "moving Slot" "$OUT"/local/node*.log | sed -E 's/^[0-9/]+ ([0-9:]+) store: node [0-9]+: /  \1 /' || echo "  no Slot was moved"
  else
    # The mark sits one second before the Fault, so the pause the Fault
    # causes always ends after it.
    "${bench[@]}" -mark "$((FAULT_AT - 1))s" &
    pid=$!
    sleep "$FAULT_AT"
    target="$(leader)"
    # moves asks for one Move every half second until time is up, each to the
    # Group after the Slot's present one, and prints how long each took.
    moves() {
      local until=$((SECONDS + FAULT_FOR)) slot=0 round=1
      while ((SECONDS < until)); do
        # Slot s starts in Group s mod DATA_GROUPS + 1, so this is never
        # the Group that has it.
        "$ctl" move "$slot" "$(((slot + round) % DATA_GROUPS + 1))" || true
        slot=$(((slot + 1) % ${SLOTS:-64}))
        ((slot != 0)) || round=$((round + 1))
        sleep 0.5
      done
    }
    [[ $FAULT == corrupt-follower || $FAULT == replace-follower ]] && target=$((target % N + 1))
    case "$FAULT" in
      pause-leader)   "$ctl" pause "$target";   sleep "$FAULT_FOR"; "$ctl" resume "$target" ;;
      isolate-leader) "$ctl" isolate "$target"; sleep "$FAULT_FOR"; "$ctl" heal ;;
      kill-leader)    "$ctl" kill "$target";    sleep "$FAULT_FOR"; "$ctl" restart "$target" ;;
      corrupt-follower | corrupt-leader)
        "$ctl" kill "$target"; "$ctl" corrupt "$target"; sleep "$FAULT_FOR"; "$ctl" restart "$target" ;;
      replace-follower)
        "$ctl" kill "$target"; "$ctl" add "$TOTAL"; "$ctl" remove "$target" ;;
      move-slots) moves ;;
      kill-for-good) "$ctl" kill 2; killed=$(date +%s) ;;
      kill-node)  "$ctl" kill 1; sleep "$FAULT_FOR"; "$ctl" restart 1 ;;
      move-and-kill)
        moves &
        mover=$!
        sleep 1; "$ctl" kill 1; sleep 1; "$ctl" restart 1
        wait "$mover" ;;
      restart-all)
        for i in $(seq 1 "$N"); do "$ctl" kill "$i"; done
        sleep "$FAULT_FOR"
        for i in $(seq 1 "$N"); do "$ctl" restart "$i"; done ;;
    esac
    set +e
    wait "$pid"
    status=$?
    set -e
    if [[ $FAULT == move* ]]; then
      # How long each moved Slot refused its clients.
      grep -h "handed over" "$OUT"/local/node*.log | sed -E 's/.*(Slot [0-9]+) handed over by (Group [0-9]+) after being frozen for ([0-9]+) ms/\3/' | sort -n |
        awk '{ a[NR] = $1 } END { if (NR) printf "move pause:  %d Slots handed over; frozen for a median of %d ms, at most %d ms\n", NR, a[int((NR + 1) / 2)], a[NR] }'
    fi
    if [[ $FAULT == kill-for-good ]]; then
      "$ctl" table || true
      grep -h "is dead by a Majority\|started a replica\|dropped its replica" "$OUT"/local/node*.log | sort | sed -E 's/^[0-9/]+ ([0-9:]+) store: /  \1 /' || true
      echo "killed node 2 at $(date -r "$killed" +%H:%M:%S)"
    fi
    if [[ $FAULT == corrupt-* ]]; then
      grep -h "found damage" "$OUT/local/node$target.log" || echo "node $target found no damage"
    fi
  fi
  exit "$status"
} 2>&1 | tee "$log"
exit "${PIPESTATUS[0]}"
