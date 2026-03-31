#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 3 ]; then
  echo "usage: $0 <variant> <root> <total> [node-args...]" >&2
  exit 1
fi

VARIANT="$1"
ROOT="$2"
TOTAL="$3"
shift 3
NODE_EXTRA_ARGS=("$@")
OBS="$ROOT/observability"
GENERAL_TOPIC="openagent/v1/general"
RENDEZVOUS="openagent/v1/dev"
P2P_BASE=5500
API_BASE=8500
SUDO_PW="${SCALEBENCH_SUDO_PW:-ma123}"

if [ "$TOTAL" -lt 1 ]; then
  echo "total must be >= 1" >&2
  exit 1
fi

if [ "$TOTAL" -le 80 ]; then
  SEED_COUNT=3
  BATCH_SIZE=10
elif [ "$TOTAL" -le 160 ]; then
  SEED_COUNT=4
  BATCH_SIZE=15
else
  SEED_COUNT=5
  BATCH_SIZE=20
fi
BATCH_SLEEP=15
if [ "$SEED_COUNT" -gt "$TOTAL" ]; then
  SEED_COUNT="$TOTAL"
fi

NEIGHBOR_TARGET=6
NEIGHBOR_HARD_CAP=8
SAME_TOPIC_NEIGHBOR_MIN=3
SAME_TOPIC_MESH_MIN=4
BOOTSTRAP_CONNECT_COUNT=2
BOOTSTRAP_CANDIDATE_COUNT=8
NEIGHBOR_LOW_WATERMARK=4
TOPIC_REPLENISH_BATCH=1
TOPIC_REPLENISH_COOLDOWN="20s"

override_arg() {
  local key="$1"
  local default="$2"
  local value="$default"
  local needs_value=0
  for arg in "${NODE_EXTRA_ARGS[@]}"; do
    if [ "$needs_value" -eq 1 ]; then
      value="$arg"
      needs_value=0
      continue
    fi
    if [ "$arg" = "$key" ]; then
      needs_value=1
    fi
  done
  printf '%s' "$value"
}

NEIGHBOR_TARGET="$(override_arg --neighbor-target "$NEIGHBOR_TARGET")"
NEIGHBOR_HARD_CAP="$(override_arg --neighbor-hard-cap "$NEIGHBOR_HARD_CAP")"
SAME_TOPIC_NEIGHBOR_MIN="$(override_arg --same-topic-neighbor-min "$SAME_TOPIC_NEIGHBOR_MIN")"
SAME_TOPIC_MESH_MIN="$(override_arg --same-topic-mesh-min "$SAME_TOPIC_MESH_MIN")"
BOOTSTRAP_CONNECT_COUNT="$(override_arg --bootstrap-connect-count "$BOOTSTRAP_CONNECT_COUNT")"
BOOTSTRAP_CANDIDATE_COUNT="$(override_arg --bootstrap-candidate-count "$BOOTSTRAP_CANDIDATE_COUNT")"
NEIGHBOR_LOW_WATERMARK="$(override_arg --neighbor-low-watermark "$NEIGHBOR_LOW_WATERMARK")"
TOPIC_REPLENISH_BATCH="$(override_arg --topic-replenish-batch "$TOPIC_REPLENISH_BATCH")"
TOPIC_REPLENISH_COOLDOWN="$(override_arg --topic-replenish-cooldown "$TOPIC_REPLENISH_COOLDOWN")"

mkdir -p "$OBS"

sudo_sh() {
  printf '%s\n' "$SUDO_PW" | sudo -S -p '' "$@"
}

cleanup_oa_formal_containers() {
  local retries=12
  local attempt=1
  local remaining=0
  while [ "$attempt" -le "$retries" ]; do
    remaining="$(sudo_sh bash -lc "docker ps -aq --filter name=oa-formal- | wc -l | tr -d '[:space:]'")"
    if [ "${remaining:-0}" -eq 0 ]; then
      return 0
    fi
    sudo_sh bash -lc "docker ps -aq --filter name=oa-formal- | xargs -r docker rm -f >/dev/null 2>&1 || true"
    sleep 2
    attempt=$((attempt + 1))
  done
  remaining="$(sudo_sh bash -lc "docker ps -aq --filter name=oa-formal- | wc -l | tr -d '[:space:]'")"
  if [ "${remaining:-0}" -ne 0 ]; then
    echo "cleanup failed: remaining oa-formal containers=${remaining}" >&2
    sudo_sh bash -lc "docker ps -a --format '{{.Names}}' | grep '^oa-formal-' || true" >&2 || true
    return 1
  fi
  return 0
}

wait_healthz() {
  local api="$1"
  for _ in $(seq 1 180); do
    if curl -sf "http://127.0.0.1:${api}/healthz" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

mkboot() {
  local api="$1"
  local p2p="$2"
  wait_healthz "$api"
  for _ in $(seq 1 30); do
    local resp
    local peer_id
    resp="$(curl -sf "http://127.0.0.1:${api}/healthz" || true)"
    if [ -n "$resp" ]; then
      peer_id="$(printf '%s' "$resp" | python3 -c "import sys,json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
peer_id = d.get('peer_id')
if not peer_id:
    sys.exit(1)
print(peer_id)
" 2>/dev/null || true)"
      if [ -n "$peer_id" ]; then
        printf '/ip4/127.0.0.1/tcp/%s/p2p/%s\n' "$p2p" "$peer_id"
        return 0
      fi
    fi
    sleep 1
  done
  return 1
}

start_node() {
  local idx="$1"; shift || true
  local p2p=$((P2P_BASE + idx - 1))
  local api=$((API_BASE + idx - 1))
  local prof="formal${idx}"
  local data="$ROOT/node${idx}"
  mkdir -p "$data"
  # Defensive cleanup for a single slot. Helps when previous runs exited early.
  sudo_sh docker rm -f "oa-formal-${idx}" >/dev/null 2>&1 || true
  sudo_sh docker run -d \
    --name "oa-formal-${idx}" \
    --network host \
    -v "$data:/data" \
    openagent-node:formal \
    --profile "$prof" \
    --data-dir /data \
    --port "$p2p" \
    --api-port "$api" \
    --rendezvous "$RENDEZVOUS" \
    --experiment-variant "$VARIANT" \
    --topic "$GENERAL_TOPIC" \
    --neighbor-target "$NEIGHBOR_TARGET" \
    --neighbor-hard-cap "$NEIGHBOR_HARD_CAP" \
    --same-topic-neighbor-min "$SAME_TOPIC_NEIGHBOR_MIN" \
    --same-topic-mesh-min "$SAME_TOPIC_MESH_MIN" \
    --bootstrap-connect-count "$BOOTSTRAP_CONNECT_COUNT" \
    --bootstrap-candidate-count "$BOOTSTRAP_CANDIDATE_COUNT" \
    --neighbor-low-watermark "$NEIGHBOR_LOW_WATERMARK" \
    --topic-replenish-batch "$TOPIC_REPLENISH_BATCH" \
    --topic-replenish-cooldown "$TOPIC_REPLENISH_COOLDOWN" \
    "$@" \
    "${NODE_EXTRA_ARGS[@]}" >/dev/null
}

START_TS="$(date +%s)"
cleanup_oa_formal_containers
sudo_sh rm -rf "$ROOT"
mkdir -p "$OBS"

BOOTSTRAPS=()
SEED_INDEXES=()
for idx in $(seq 1 "$SEED_COUNT"); do
  SEED_INDEXES+=("$idx")
done

for idx in "${SEED_INDEXES[@]}"; do
  args=()
  if [ "${#BOOTSTRAPS[@]}" -gt 0 ]; then
    for b in "${BOOTSTRAPS[@]}"; do
      args+=(--bootstrap "$b")
    done
  fi
  start_node "$idx" "${args[@]}"
  boot="$(mkboot $((API_BASE + idx - 1)) $((P2P_BASE + idx - 1)))"
  BOOTSTRAPS+=("$boot")
  sleep 1
done
printf '%s\n' "${BOOTSTRAPS[@]}" > "$OBS/bootstrap_multiaddrs.txt"
printf '%s\n' "${SEED_INDEXES[@]}" > "$OBS/seed_indexes.txt"

NON_SEEDS=()
for idx in $(seq 1 "$TOTAL"); do
  is_seed=0
  for seeded in "${SEED_INDEXES[@]}"; do
    if [ "$seeded" -eq "$idx" ]; then
      is_seed=1
      break
    fi
  done
  if [ "$is_seed" -eq 0 ]; then
    NON_SEEDS+=("$idx")
  fi
done

for ((offset=0; offset<${#NON_SEEDS[@]}; offset+=BATCH_SIZE)); do
  batch_end=$((offset + BATCH_SIZE))
  if [ "$batch_end" -gt "${#NON_SEEDS[@]}" ]; then
    batch_end="${#NON_SEEDS[@]}"
  fi
  for ((i=offset; i<batch_end; i++)); do
    idx="${NON_SEEDS[$i]}"
    args=()
    for b in "${BOOTSTRAPS[@]}"; do
      args+=(--bootstrap "$b")
    done
    start_node "$idx" "${args[@]}"
    sleep 0.2
  done
  sleep "$BATCH_SLEEP"
done

health_ok=0
for api in $(seq "$API_BASE" $((API_BASE + TOTAL - 1))); do
  if wait_healthz "$api"; then
    health_ok=$((health_ok + 1))
  fi
done

printf '%s\n' "$health_ok" > "$OBS/healthz_ok_count.txt"
sudo_sh bash -lc "docker ps --format '{{.Names}}' | grep -c '^oa-formal-' || true" > "$OBS/running_count.txt"
ESTABLISHED_TCP_COUNT="$(ss -tan state established | awk 'NR>1{c++} END{print c+0}')"
printf '%s\n' "$ESTABLISHED_TCP_COUNT" > "$OBS/established_tcp_count.txt"
set +o pipefail
DOCKER_CPU_SUM="$(sudo_sh docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}' $(printf 'oa-formal-%s ' $(seq 1 "$TOTAL")) 2>/dev/null | python3 - <<'PY'
import sys
total = 0.0
for line in sys.stdin:
    parts = line.strip().split("|")
    if len(parts) != 2:
        continue
    cpu = parts[1].strip().rstrip("%")
    try:
        total += float(cpu)
    except ValueError:
        continue
print(f"{total:.2f}")
PY
)"
set -o pipefail
printf '%s\n' "$DOCKER_CPU_SUM" > "$OBS/docker_cpu_pct_sum.txt"

python3 - <<PY > "$OBS/overlay_snapshot.json"
import json
import math
import urllib.request

api_base = $API_BASE
total = $TOTAL
topic = "$GENERAL_TOPIC"


def percentile(values, ratio):
    if not values:
        return None
    values = sorted(values)
    idx = max(0, math.ceil(len(values) * ratio) - 1)
    return round(float(values[idx]), 2)


peer_counts = []
mesh_counts = []
for port in range(api_base, api_base + total):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/v1/peers", timeout=3) as r:
            peers_payload = json.loads(r.read().decode())
        if isinstance(peers_payload, list):
            peer_counts.append(len(peers_payload))
        elif isinstance(peers_payload, dict):
            if isinstance(peers_payload.get("peers"), list):
                peer_counts.append(len(peers_payload["peers"]))
            else:
                peer_counts.append(0)
        else:
            peer_counts.append(0)
    except Exception:
        peer_counts.append(0)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/v1/pubsub", timeout=3) as r:
            pubsub = json.loads(r.read().decode())
        topic_mesh = pubsub.get("topic_mesh") or {}
        mesh = topic_mesh.get(topic) or []
        mesh_counts.append(len(mesh) if isinstance(mesh, list) else 0)
    except Exception:
        mesh_counts.append(0)

snapshot = {
    "peer_count_p50": percentile(peer_counts, 0.5),
    "peer_count_p95": percentile(peer_counts, 0.95),
    "peer_count_max": max(peer_counts) if peer_counts else None,
    "general_topic_mesh_size_p50": percentile(mesh_counts, 0.5),
    "general_topic_mesh_size_p95": percentile(mesh_counts, 0.95),
    "general_topic_mesh_size_max": max(mesh_counts) if mesh_counts else None,
}
print(json.dumps(snapshot, indent=2))
PY
BOOTSTRAP_TIME_SEC=$(( $(date +%s) - START_TS ))
printf '%s\n' "$BOOTSTRAP_TIME_SEC" > "$OBS/bootstrap_time_sec.txt"

python3 - <<PY > "$OBS/summary.json"
import json
import pathlib

base = pathlib.Path(r"$OBS")
running = int((base / "running_count.txt").read_text().strip())
health = int((base / "healthz_ok_count.txt").read_text().strip())
overlay = json.loads((base / "overlay_snapshot.json").read_text())
seed_indexes = [int(line.strip()) for line in (base / "seed_indexes.txt").read_text().splitlines() if line.strip()]
established_tcp_count = int((base / "established_tcp_count.txt").read_text().strip())
docker_cpu_pct_sum = float((base / "docker_cpu_pct_sum.txt").read_text().strip())
bootstrap_time_sec = int((base / "bootstrap_time_sec.txt").read_text().strip())
tcp_per_node_avg = round(established_tcp_count / max(1, $TOTAL), 2)

print(json.dumps({
    "variant": "$VARIANT",
    "node_count": $TOTAL,
    "topic_mode": "global_only",
    "global_topic": "$GENERAL_TOPIC",
    "seed_strategy": "global_fixed_table",
    "seed_count": len(seed_indexes),
    "seed_indexes": seed_indexes,
    "bootstrap_connect_count": int("$BOOTSTRAP_CONNECT_COUNT"),
    "bootstrap_candidate_count": int("$BOOTSTRAP_CANDIDATE_COUNT"),
    "neighbor_target": int("$NEIGHBOR_TARGET"),
    "neighbor_hard_cap": int("$NEIGHBOR_HARD_CAP"),
    "same_topic_neighbor_min": int("$SAME_TOPIC_NEIGHBOR_MIN"),
    "same_topic_mesh_min": int("$SAME_TOPIC_MESH_MIN"),
    "neighbor_low_watermark": int("$NEIGHBOR_LOW_WATERMARK"),
    "topic_replenish_batch": int("$TOPIC_REPLENISH_BATCH"),
    "topic_replenish_cooldown": "$TOPIC_REPLENISH_COOLDOWN",
    "running_count": running,
    "healthz_ok_count": health,
    "missing_count": max(0, $TOTAL - running),
    "missing_nodes": [],
    "bootstrap_time_sec": bootstrap_time_sec,
    "established_tcp_count": established_tcp_count,
    "docker_cpu_pct_sum": docker_cpu_pct_sum,
    "tcp_per_node_avg": tcp_per_node_avg,
    "peer_count_p50": overlay.get("peer_count_p50"),
    "peer_count_p95": overlay.get("peer_count_p95"),
    "peer_count_max": overlay.get("peer_count_max"),
    "general_topic_mesh_size_p50": overlay.get("general_topic_mesh_size_p50"),
    "general_topic_mesh_size_p95": overlay.get("general_topic_mesh_size_p95"),
    "general_topic_mesh_size_max": overlay.get("general_topic_mesh_size_max"),
}, indent=2))
PY
