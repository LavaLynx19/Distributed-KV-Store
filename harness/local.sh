#!/usr/bin/env bash
# Runs a Group as local processes: Member i listens for other Members on
# 127.0.0.1:700i and for clients on 127.0.0.1:800i.
#
#   harness/local.sh start [members]   # default 3
#   harness/local.sh status
#   harness/local.sh pause <id>        # freeze one Member (SIGSTOP)
#   harness/local.sh resume <id>       # let it continue (SIGCONT)
#   harness/local.sh kill <id>         # kill -9 one Member
#   harness/local.sh restart <id>      # start it again from its data directory
#   harness/local.sh stop
#
# Each Member keeps its durable state in harness/out/local/data<id>. "start"
# wipes them; "restart" keeps them. NODATA=1 runs Members with no data
# directory, as before Rung 3.
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

launch() { # id members
  local data=()
  [[ -n "${NODATA:-}" ]] || data=(-data "$OUT/data$1")
  "$BIN" -id "$1" -peers "$(addrs 7000 "$2")" -clients "$(addrs 8000 "$2")" ${data[@]+"${data[@]}"} ${KVNODE_FLAGS:-} \
    >>"$OUT/node$1.log" 2>&1 &
  echo $! >"$OUT/node$1.pid"
}

members() { cat "$OUT/members" 2>/dev/null || { echo "no Group running" >&2; exit 1; }; }

case "${1:-}" in
  start)
    n="${2:-3}"
    mkdir -p "$OUT" "$ROOT/bin"
    go build -o "$BIN" "$ROOT/cmd/kvnode"
    rm -f "$OUT"/node*.log "$OUT"/node*.pid
    rm -rf "$OUT"/data*
    echo "$n" >"$OUT/members"
    for i in $(seq 1 "$n"); do launch "$i" "$n"; done
    echo "started $n Members; clients on 127.0.0.1:8001..$((8000 + n))"
    ;;
  status)
    for i in $(seq 1 "$(members)"); do
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
    launch "$id" "$(members)"
    echo "restarted node $id"
    ;;
  stop)
    for f in "$OUT"/node*.pid; do
      [[ -e "$f" ]] || continue
      kill -CONT "$(cat "$f")" 2>/dev/null || true
      kill "$(cat "$f")" 2>/dev/null || true
      rm -f "$f"
    done
    rm -f "$OUT/members"
    echo "stopped"
    ;;
  *)
    sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
