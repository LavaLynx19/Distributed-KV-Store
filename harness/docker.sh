#!/usr/bin/env bash
# Runs a Group in Docker Compose. Member i serves clients on 127.0.0.1:800i.
#
#   harness/docker.sh up [members] [direct]   # default 3; "direct" skips toxiproxy
#   harness/docker.sh status
#   harness/docker.sh isolate <id>            # cut every link to and from one Member
#   harness/docker.sh cut <from> <to>         # cut one direction of one link
#   harness/docker.sh heal                    # restore every link
#   harness/docker.sh pause <id>              # freeze one Member
#   harness/docker.sh resume <id>
#   harness/docker.sh down
#
# By default every Member-to-Member link runs through its own toxiproxy proxy
# (named "<from>-<to>"), so links can be cut one direction at a time. "direct"
# connects Members to each other without it, for undistorted numbers. Until
# Rung 3 a crash is a freeze, as in harness/local.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/harness/out/docker"
COMPOSE=(docker compose -f "$ROOT/deploy/docker-compose.yml")
TOXI=http://127.0.0.1:8474

members() { cat "$OUT/members" 2>/dev/null || { echo "no Group running" >&2; exit 1; }; }
port() { echo $((20000 + $1 * 100 + $2)); } # toxiproxy port for the link from $1 to $2
toggle() { curl -sf -o /dev/null -X POST "$TOXI/proxies/$1" -d "{\"enabled\":$2}"; }

case "${1:-}" in
  up)
    n="${2:-3}"; mode="${3:-proxied}"
    mkdir -p "$OUT" "$ROOT/bin/linux"
    (cd "$ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH="$(go env GOARCH)" go build -o bin/linux/kvnode ./cmd/kvnode)

    clients=() services=()
    for i in $(seq 1 "$n"); do clients+=("$i=127.0.0.1:$((8000 + i))"); services+=("node$i"); done
    export CLIENTS="$(IFS=,; echo "${clients[*]}")"
    for i in $(seq 1 "$n"); do
      peers=()
      for j in $(seq 1 "$n"); do
        if [[ $i == "$j" ]]; then peers+=("$j=0.0.0.0:7000")
        elif [[ $mode == direct ]]; then peers+=("$j=node$j:7000")
        else peers+=("$j=toxiproxy:$(port "$i" "$j")"); fi
      done
      export "PEERS_$i=$(IFS=,; echo "${peers[*]}")"
    done

    "${COMPOSE[@]}" down --remove-orphans >/dev/null 2>&1 || true
    "${COMPOSE[@]}" build --quiet node1
    if [[ $mode != direct ]]; then
      "${COMPOSE[@]}" up -d toxiproxy >/dev/null 2>&1
      for _ in $(seq 1 50); do curl -sf -o /dev/null "$TOXI/version" && break; sleep 0.2; done
      for i in $(seq 1 "$n"); do for j in $(seq 1 "$n"); do
        [[ $i == "$j" ]] && continue
        curl -sf -o /dev/null -X POST "$TOXI/proxies" \
          -d "{\"name\":\"$i-$j\",\"listen\":\"0.0.0.0:$(port "$i" "$j")\",\"upstream\":\"node$j:7000\"}"
      done; done
    fi
    "${COMPOSE[@]}" up -d "${services[@]}" >/dev/null 2>&1
    echo "$n" >"$OUT/members"; echo "$mode" >"$OUT/mode"
    echo "started $n Members ($mode); clients on 127.0.0.1:8001..$((8000 + n))"
    ;;
  status)
    for i in $(seq 1 "$(members)"); do
      printf 'node %s: %s\n' "$i" "$(curl -s -m 1 "http://127.0.0.1:$((8000 + i))/v1/status" || printf 'down')"
    done
    ;;
  isolate)
    id="${2:?usage: docker.sh isolate <id>}"
    for j in $(seq 1 "$(members)"); do
      [[ $j == "$id" ]] && continue
      toggle "$id-$j" false; toggle "$j-$id" false
    done
    echo "isolated node $id"
    ;;
  cut)
    toggle "${2:?usage: docker.sh cut <from> <to>}-${3:?usage: docker.sh cut <from> <to>}" false
    echo "cut $2 → $3"
    ;;
  heal)
    n="$(members)"
    for i in $(seq 1 "$n"); do for j in $(seq 1 "$n"); do
      [[ $i == "$j" ]] || toggle "$i-$j" true
    done; done
    echo "healed"
    ;;
  pause)
    "${COMPOSE[@]}" pause "node${2:?usage: docker.sh pause <id>}" >/dev/null 2>&1
    echo "paused node $2"
    ;;
  resume)
    "${COMPOSE[@]}" unpause "node${2:?usage: docker.sh resume <id>}" >/dev/null 2>&1
    echo "resumed node $2"
    ;;
  down)
    "${COMPOSE[@]}" down --remove-orphans >/dev/null 2>&1
    rm -f "$OUT/members" "$OUT/mode"
    echo "stopped"
    ;;
  *)
    sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
