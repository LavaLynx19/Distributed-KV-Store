#!/usr/bin/env bash
# Runs a Group as local processes: Member i listens for other Members on
# 127.0.0.1:700i and for clients on 127.0.0.1:800i.
#
#   harness/local.sh start [members] [spares]   # default 3 and 0
#   harness/local.sh add <id>          # add a Spare to the Group (kvctl)
#   harness/local.sh remove <id>       # remove a Member from the Group (kvctl)
#   harness/local.sh members           # what each Node says of the Group (kvctl)
#   harness/local.sh table             # with DATA_GROUPS: which Group owns each Slot (kvctl)
#   harness/local.sh move <slot> <group>   # with DATA_GROUPS: move a Slot (kvctl)
#   harness/local.sh join <id>         # with DATA_GROUPS: start one more Node, told only where Node 1 is
#   harness/local.sh nodes             # with DATA_GROUPS: what each Node's gossip thinks (kvctl)
#   harness/local.sh status
#   harness/local.sh pause <id>        # freeze one Member (SIGSTOP)
#   harness/local.sh resume <id>       # let it continue (SIGCONT)
#   harness/local.sh kill <id>         # kill -9 one Member
#   harness/local.sh restart <id>      # start it again from its data directory
#   harness/local.sh corrupt <id>      # flip one bit in the middle of its newest
#                                      # Log segment (kill it first)
#   harness/local.sh stop
#
# Each Member keeps its durable state in harness/out/local/data<id>. "start"
# wipes them; "restart" keeps them. NODATA=1 runs Members with no data
# directory, as before Rung 3.
#
# DATA_GROUPS=3 on "start" runs a store with that many data Groups and a Meta
# Group, each on 3 of the Nodes (A§11). SLOTS sets the number of Slots.
#
# Logs and pid files go to harness/out/local/.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/harness/out/local"
BIN="$ROOT/bin/kvnode"

addrs() { # base-port members
  local list=() i
  for i in $(seq 1 "$2"); do list+=("$i=127.0.0.1:$(($1 + i))"); done
  (IFS=,; echo "${list[*]}")
}

launch() { # id nodes founders
  local data=()
  [[ -n "${NODATA:-}" ]] || data=(-data "$OUT/data$1")
  local shape=(-members "$(seq -s, 1 "$3" | sed 's/,$//')")
  local groups
  groups="$(cat "$OUT/groups" 2>/dev/null || echo 0)"
  [[ $groups -eq 0 ]] || shape=(-data-groups "$groups" -slots "$(cat "$OUT/slots")")
  "$BIN" -id "$1" -peers "$(addrs 7000 "$2")" -clients "$(addrs 8000 "$2")" "${shape[@]}" \
    ${data[@]+"${data[@]}"} ${KVNODE_FLAGS:-} \
    >>"$OUT/node$1.log" 2>&1 &
  echo $! >"$OUT/node$1.pid"
}

# nodes is how many Nodes were started; founders, how many of them the Group
# began with. The rest began as Spares.
nodes() { cat "$OUT/nodes" 2>/dev/null || { echo "no Group running" >&2; exit 1; }; }
founders() { cat "$OUT/founders"; }
urls() {
  local list=() i
  for i in $(seq 1 "$(nodes)"); do list+=("http://127.0.0.1:$((8000 + i))"); done
  (IFS=,; echo "${list[*]}")
}

case "${1:-}" in
  start)
    n="${2:-3}"
    total=$((n + ${3:-0}))
    mkdir -p "$OUT" "$ROOT/bin"
    go build -o "$BIN" "$ROOT/cmd/kvnode"
    go build -o "$ROOT/bin/kvctl" "$ROOT/cmd/kvctl"
    rm -f "$OUT"/node*.log "$OUT"/node*.pid
    rm -rf "$OUT"/data*
    echo "$total" >"$OUT/nodes"
    echo "$n" >"$OUT/founders"
    echo "${DATA_GROUPS:-0}" >"$OUT/groups"
    echo "${SLOTS:-64}" >"$OUT/slots"
    for i in $(seq 1 "$total"); do launch "$i" "$total" "$n"; done
    echo "started $n Members${3:+ and $3 Spares}; clients on 127.0.0.1:8001..$((8000 + total))"
    ;;
  status)
    for i in $(seq 1 "$(nodes)"); do
      printf 'node %s: ' "$i"
      curl -s -m 1 "http://127.0.0.1:$((8000 + i))/v1/status" || printf 'down'
      echo
    done
    ;;
  pause)
    id="${2:?usage: local.sh pause <id>}"
    kill -STOP "$(cat "$OUT/node$id.pid")"
    echo "paused node $id"
    ;;
  resume)
    id="${2:?usage: local.sh resume <id>}"
    kill -CONT "$(cat "$OUT/node$id.pid")"
    echo "resumed node $id"
    ;;
  kill)
    id="${2:?usage: local.sh kill <id>}"
    kill -9 "$(cat "$OUT/node$id.pid")" 2>/dev/null || true
    echo "killed node $id"
    ;;
  restart)
    id="${2:?usage: local.sh restart <id>}"
    launch "$id" "$(nodes)" "$(founders)"
    echo "restarted node $id"
    ;;
  add | remove)
    id="${2:?usage: local.sh $1 <id>}"
    "$ROOT/bin/kvctl" -nodes "$(urls)" "$1" "$id"
    ;;
  members)
    "$ROOT/bin/kvctl" -nodes "$(urls)" status
    ;;
  table)
    "$ROOT/bin/kvctl" -nodes "$(urls)" table
    ;;
  nodes)
    "$ROOT/bin/kvctl" -nodes "$(urls)" nodes
    ;;
  join)
    id="${2:?usage: local.sh join <id>}"
    data=()
    [[ -n "${NODATA:-}" ]] || data=(-data "$OUT/data$id")
    "$BIN" -id "$id" -peers "$id=127.0.0.1:$((7000 + id))" -clients "$id=127.0.0.1:$((8000 + id))" \
      -join "1=127.0.0.1:8001" -founders "$(founders)" -data-groups "$(cat "$OUT/groups")" -slots "$(cat "$OUT/slots")" \
      ${data[@]+"${data[@]}"} ${KVNODE_FLAGS:-} >>"$OUT/node$id.log" 2>&1 &
    echo $! >"$OUT/node$id.pid"
    (( id <= $(nodes) )) || echo "$id" >"$OUT/nodes"
    echo "node $id started, told only where node 1 is"
    ;;
  move)
    "$ROOT/bin/kvctl" -nodes "$(urls)" move "${2:?usage: local.sh move <slot> <group>}" "${3:?usage: local.sh move <slot> <group>}"
    ;;
  corrupt)
    id="${2:?usage: local.sh corrupt <id>}"
    seg="$(ls "$OUT/data$id/log"/*.seg | tail -1)"
    off=$(($(wc -c <"$seg") / 2))
    byte="$(dd if="$seg" bs=1 skip="$off" count=1 2>/dev/null | od -An -tu1 | tr -d ' ')"
    printf "\\$(printf '%03o' $((byte ^ 16)))" | dd of="$seg" bs=1 seek="$off" conv=notrunc 2>/dev/null
    echo "flipped a bit at byte $off of node $id's $(basename "$seg")"
    ;;
  stop)
    for f in "$OUT"/node*.pid; do
      [[ -e "$f" ]] || continue
      kill -CONT "$(cat "$f")" 2>/dev/null || true
      kill "$(cat "$f")" 2>/dev/null || true
      rm -f "$f"
    done
    rm -f "$OUT/nodes" "$OUT/founders" "$OUT/groups" "$OUT/slots"
    echo "stopped"
    ;;
  *)
    sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
