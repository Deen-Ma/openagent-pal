#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import statistics
import subprocess
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any

from scalebench_capacity_lib import GLOBAL_TOPIC, PUBLIC_WORST_CASE_VARIANT


def get_json(port: int, path: str) -> Any:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as response:
        return json.loads(response.read().decode())


def post_json(port: int, path: str, body: dict[str, Any]) -> Any:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode())


def safe_shell(cmd: str) -> str:
    try:
        return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        return exc.output or str(exc)


def read_iface_bytes(iface: str) -> dict[str, int]:
    base = Path(f"/sys/class/net/{iface}/statistics")
    return {
        "rx_bytes": int((base / "rx_bytes").read_text().strip()),
        "tx_bytes": int((base / "tx_bytes").read_text().strip()),
    }


def detect_default_iface() -> str:
    try:
        output = subprocess.check_output(
            ["sh", "-lc", "ip route show default | awk 'NR==1 {print $5}'"],
            text=True,
        ).strip()
    except Exception:
        output = ""
    if output:
        return output
    for candidate in ("eno1np0", "enp0s31f6", "eth0"):
        if Path(f"/sys/class/net/{candidate}").exists():
            return candidate
    raise RuntimeError("unable to detect default physical interface")


def parse_load1() -> float:
    return float(Path("/proc/loadavg").read_text().split()[0])


def parse_mem_used_bytes() -> int:
    values = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, raw = line.split(":", 1)
        values[key] = int(raw.strip().split()[0]) * 1024
    total = values.get("MemTotal", 0)
    available = values.get("MemAvailable", 0)
    return max(0, total - available)


def read_proc_stat() -> tuple[int, int]:
    fields = Path("/proc/stat").read_text().splitlines()[0].split()[1:]
    values = [int(value) for value in fields]
    idle = values[3] + values[4]
    total = sum(values)
    busy = total - idle
    return total, busy


def parse_ctxt_switches(pid: int) -> int:
    status_path = Path(f"/proc/{pid}/status")
    if not status_path.exists():
        return 0
    total = 0
    for line in status_path.read_text().splitlines():
        if line.startswith(("voluntary_ctxt_switches", "nonvoluntary_ctxt_switches")):
            total += int(line.split(":", 1)[1].strip())
    return total


def parse_thread_count(pid: int) -> int:
    status_path = Path(f"/proc/{pid}/status")
    if not status_path.exists():
        return 0
    for line in status_path.read_text().splitlines():
        if line.startswith("Threads:"):
            return int(line.split(":", 1)[1].strip())
    return 0


def running_container_pids() -> list[int]:
    try:
        output = subprocess.check_output(
            [
                "sh",
                "-lc",
                "printf '%s\\n' \"${SCALEBENCH_SUDO_PW:-ma123}\" | "
                "sudo -S -p '' sh -lc "
                "\"docker inspect -f '{{.State.Pid}}' \\$(docker ps -q --filter name=oa-formal-) 2>/dev/null\"",
            ],
            text=True,
        )
    except Exception:
        return []
    pids: list[int] = []
    for line in output.splitlines():
        line = line.strip()
        if not line or not re.fullmatch(r"[0-9]+", line):
            continue
        pid = int(line)
        if pid > 0:
            pids.append(pid)
    return pids


def count_established_sockets() -> int:
    output = safe_shell("ss -tan state established")
    count = 0
    for line in output.splitlines():
        if not line or line.startswith(("Recv-Q", "Netid")):
            continue
        count += 1
    return count


def parse_docker_stats_cpu_sum(raw_text: str) -> float | None:
    cpu_values = []
    for line in raw_text.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 2 or not parts[0].startswith("oa-formal-"):
            continue
        cpu_text = parts[1].strip()
        if cpu_text.endswith("%"):
            cpu_text = cpu_text[:-1]
        try:
            cpu_values.append(float(cpu_text))
        except ValueError:
            continue
    if not cpu_values:
        return None
    return round(sum(cpu_values), 2)


def docker_cpu_pct_sum(node_count: int, sudo_pw: str) -> float:
    command = (
        f"printf '{sudo_pw}\\n' | sudo -S -p '' docker stats --no-stream "
        "--format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.PIDs}}' "
        f"$(printf 'oa-formal-%s ' $(seq 1 {node_count}))"
    )
    value = parse_docker_stats_cpu_sum(safe_shell(command))
    return float(value) if value is not None else 0.0


def percentile(values: list[float], ratio: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * ratio) - 1)
    return round(float(ordered[index]), 2)


def stats(values: list[float | int | None]) -> dict[str, float | int]:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return {"count": 0}
    cleaned = sorted(cleaned)
    return {
        "count": len(cleaned),
        "min": round(cleaned[0], 2),
        "p50": round(float(statistics.median(cleaned)), 2),
        "p95": round(cleaned[max(0, math.ceil(len(cleaned) * 0.95) - 1)], 2),
        "p99": round(cleaned[max(0, math.ceil(len(cleaned) * 0.99) - 1)], 2),
        "max": round(cleaned[-1], 2),
        "avg": round(sum(cleaned) / len(cleaned), 2),
    }


def cover_latency(values_ms: list[float], threshold_count: int) -> float | None:
    if len(values_ms) < threshold_count:
        return None
    return round(sorted(values_ms)[threshold_count - 1], 2)


def parse_peer_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        peers = payload.get("peers")
        if isinstance(peers, list):
            return len(peers)
    return 0


def parse_topic_mesh_size(payload: Any, topic: str) -> int:
    if not isinstance(payload, dict):
        return 0
    topic_mesh = payload.get("topic_mesh") or {}
    if isinstance(topic_mesh, dict):
        mesh = topic_mesh.get(topic) or []
        if isinstance(mesh, list):
            return len(mesh)
    return 0


def variation_ratio(values: list[float | int]) -> float:
    if not values:
        return 0.0
    values = [float(value) for value in values]
    baseline = max(1e-6, abs(statistics.median(values)))
    return abs(max(values) - min(values)) / baseline


def collect_overlay_snapshot(
    *,
    ports: list[int],
    node_count: int,
    global_topic: str,
    sudo_pw: str,
) -> dict[str, Any]:
    healthz_ok = 0
    peer_counts: list[int] = []
    mesh_counts: list[int] = []
    for port in ports:
        try:
            get_json(port, "/healthz")
            healthz_ok += 1
        except Exception:
            pass
        peer_count = 0
        try:
            peer_count = parse_peer_count(get_json(port, "/v1/peers"))
        except Exception:
            try:
                topology = get_json(port, "/v1/topology")
                known_peers = topology.get("known_peers") if isinstance(topology, dict) else None
                if isinstance(known_peers, list):
                    peer_count = len(known_peers)
            except Exception:
                peer_count = 0
        peer_counts.append(peer_count)
        mesh_size = 0
        try:
            mesh_size = parse_topic_mesh_size(get_json(port, "/v1/pubsub"), global_topic)
        except Exception:
            try:
                topology = get_json(port, "/v1/topology")
                same_topic_mesh = topology.get("same_topic_mesh_sizes") if isinstance(topology, dict) else None
                if isinstance(same_topic_mesh, dict):
                    candidate = same_topic_mesh.get(global_topic)
                    if isinstance(candidate, int):
                        mesh_size = candidate
            except Exception:
                mesh_size = 0
        mesh_counts.append(mesh_size)

    tcp_count = count_established_sockets()
    docker_cpu_sum = docker_cpu_pct_sum(node_count, sudo_pw)
    peer_stats = stats(peer_counts)
    mesh_stats = stats(mesh_counts)
    return {
        "ts": round(time.time(), 3),
        "healthz_ok_count": healthz_ok,
        "docker_cpu_pct_sum": docker_cpu_sum,
        "established_tcp_count": tcp_count,
        "tcp_per_node_avg": round(tcp_count / max(1, node_count), 2),
        "peer_counts": peer_counts,
        "mesh_counts": mesh_counts,
        "peer_count": peer_stats,
        "general_topic_mesh_size": mesh_stats,
        "peer_per_node_p50": peer_stats.get("p50"),
        "mesh_per_node_p50": mesh_stats.get("p50"),
    }


def stabilization_gate_pass(window: list[dict[str, Any]], node_count: int) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if len(window) < 6:
        reasons.append("insufficient_samples")
        return False, reasons

    for sample in window:
        if int(sample.get("healthz_ok_count") or 0) != node_count:
            reasons.append("healthz_not_full")
            break
    for sample in window:
        cpu_per_node = float(sample.get("docker_cpu_pct_sum") or 0.0) / max(1, node_count)
        if cpu_per_node > 10.0:
            reasons.append("docker_cpu_per_node_over_10")
            break

    tcp_values = [float(sample.get("established_tcp_count") or 0.0) for sample in window]
    peer_values = [float(sample.get("peer_per_node_p50") or 0.0) for sample in window]
    mesh_values = [float(sample.get("mesh_per_node_p50") or 0.0) for sample in window]
    if variation_ratio(tcp_values) > 0.05:
        reasons.append("tcp_variation_over_5pct")
    if variation_ratio(peer_values) > 0.10:
        reasons.append("peer_p50_variation_over_10pct")
    if variation_ratio(mesh_values) > 0.10:
        reasons.append("mesh_p50_variation_over_10pct")
    return (not reasons), reasons


class HostSampler:
    def __init__(self, interval_sec: float = 1.0) -> None:
        self.interval_sec = interval_sec
        self.samples: list[dict[str, float | int]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self.interval_sec * 2 + 1)

    def _run(self) -> None:
        previous_total, previous_busy = read_proc_stat()
        while not self._stop.wait(self.interval_sec):
            current_total, current_busy = read_proc_stat()
            total_delta = current_total - previous_total
            busy_delta = current_busy - previous_busy
            if total_delta > 0:
                pids = running_container_pids()
                self.samples.append(
                    {
                        "ts": round(time.time(), 3),
                        "cpu_pct": round((busy_delta / total_delta) * 100.0, 2),
                        "load1": round(parse_load1(), 2),
                        "mem_used_bytes": parse_mem_used_bytes(),
                        "process_count": len(pids),
                        "thread_count": sum(parse_thread_count(pid) for pid in pids),
                        "ctx_switches": sum(parse_ctxt_switches(pid) for pid in pids),
                        "socket_count": count_established_sockets(),
                    }
                )
            previous_total, previous_busy = current_total, current_busy


def build_payload_detail(label: str, round_index: int, payload_bytes: int) -> dict[str, Any]:
    detail = {"label": label, "round": round_index}
    if payload_bytes <= 0:
        return detail
    base = len(json.dumps(detail, separators=(",", ":")).encode())
    target = max(payload_bytes - base - len('"blob":""'), 0)
    detail["blob"] = "x" * target
    return detail


def fetch_payload(port: int, detail_ref: str) -> bool:
    try:
        response = post_json(port, "/v1/fetch", {"detail_ref": detail_ref})
    except Exception:
        return False
    return bool(response.get("found"))


def task_visible(port: int, task_id: str) -> bool:
    try:
        rows = get_json(port, "/v1/tasks")
    except Exception:
        return False
    for row in rows:
        if row.get("task_id") == task_id:
            return True
        envelope = row.get("envelope") or {}
        if envelope.get("task_id") == task_id:
            return True
    return False


def load_cluster_state(root: Path, node_count: int, live_health_ok: int) -> dict[str, Any]:
    summary_path = root / "observability" / "summary.json"
    cluster_summary: dict[str, Any] = {}
    if summary_path.exists():
        cluster_summary = json.loads(summary_path.read_text())
    running_count = int(cluster_summary.get("running_count", node_count if live_health_ok == node_count else 0))
    health_count = int(cluster_summary.get("healthz_ok_count", live_health_ok))
    seed_indexes = cluster_summary.get("seed_indexes")
    if not isinstance(seed_indexes, list) or not seed_indexes:
        seed_indexes = [1]
    return {
        "summary": cluster_summary,
        "seed_indexes": [int(value) for value in seed_indexes],
        "running_count": running_count,
        "healthz_ok_count": health_count,
        "launch_ok": running_count == node_count,
        "health_ok": health_count == node_count,
    }


def write_host_samples(out: Path, samples: list[dict[str, float | int]]) -> None:
    with (out / "host_samples.jsonl").open("w") as handle:
        for sample in samples:
            handle.write(json.dumps(sample) + "\n")


def choose_publishers(node_count: int, api_base: int, seed_indexes: list[int]) -> tuple[list[int], list[str]]:
    seeds = sorted(set(seed_indexes))
    seed0 = seeds[0]
    seed1 = seeds[1] if len(seeds) > 1 else seed0
    non_seed_indexes = [index for index in range(1, node_count + 1) if index not in seeds]
    if non_seed_indexes:
        first_non_seed = non_seed_indexes[0]
        median_non_seed = non_seed_indexes[len(non_seed_indexes) // 2]
        last_non_seed = non_seed_indexes[-1]
    else:
        first_non_seed = seed0
        median_non_seed = seed0
        last_non_seed = seed0
    publisher_indexes = [seed0, first_non_seed, median_non_seed, last_non_seed, seed1]
    roles = [
        "seed_0",
        "first_non_seed",
        "median_non_seed",
        "last_non_seed",
        "seed_1_or_seed_0",
    ]
    ports = [api_base + index - 1 for index in publisher_indexes]
    return ports, roles


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--variant", required=True)
    parser.add_argument("--node-count", required=True, type=int)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--rounds-per-shard", type=int, default=0)
    parser.add_argument("--publish-port", type=int, default=8500)
    parser.add_argument("--api-base", type=int, default=8500)
    parser.add_argument("--observe-sec", type=int, default=60)
    parser.add_argument("--stabilize-sec", type=int, default=180)
    parser.add_argument("--stabilize-poll-sec", type=float, default=5.0)
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument("--cpu-sample-sec", type=float, default=1.0)
    parser.add_argument("--phys-iface", default="")
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument("--task-budget", type=int, default=0)
    parser.add_argument("--global-topic", default=GLOBAL_TOPIC)
    parser.add_argument(
        "--latency-mode",
        choices=("global_probe", "payload_fetch"),
        default="global_probe",
        help="global_probe matches the 60/80 baseline in P2P大规模; payload_fetch uses detail_ref pull completion.",
    )
    args = parser.parse_args()

    if args.node_count < 1:
        raise SystemExit("node-count must be >= 1")

    root = Path(args.root).expanduser()
    out = root / f"bench_shards_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}_{args.label}"
    out.mkdir(parents=True, exist_ok=True)
    sudo_pw = os.environ.get("SCALEBENCH_SUDO_PW", "ma123")
    phys_iface = args.phys_iface or detect_default_iface()

    rounds = args.task_budget if args.task_budget > 0 else args.rounds
    if args.rounds_per_shard > 0:
        rounds = args.rounds_per_shard

    ports = list(range(args.api_base, args.api_base + args.node_count))
    live_health_ok = 0
    for port in ports:
        try:
            get_json(port, "/healthz")
            live_health_ok += 1
        except Exception:
            pass

    cluster_state = load_cluster_state(root, args.node_count, live_health_ok)
    launch_ok = cluster_state["launch_ok"]
    health_ok = cluster_state["health_ok"]
    publisher_ports, publisher_roles = choose_publishers(
        args.node_count,
        args.api_base,
        cluster_state["seed_indexes"],
    )

    round_rows: list[dict[str, Any]] = []
    matrix_rows: list[dict[str, Any]] = []
    stabilization_samples: list[dict[str, Any]] = []
    bench_error: str | None = None
    stabilization_pass = False
    stabilization_reasons: list[str] = []

    header_lat_all: list[float] = []
    payload_lat_all: list[float] = []
    e2e_lat_all: list[float] = []
    cover90_global: list[float] = []
    cover95_global: list[float] = []
    cover99_global: list[float] = []
    cover100_global: list[float] = []
    per_round_header_p50: list[float] = []
    per_round_header_p95: list[float] = []
    per_round_header_p99: list[float] = []
    per_round_payload_p50: list[float] = []
    per_round_payload_p95: list[float] = []
    per_round_payload_p99: list[float] = []
    per_round_e2e_p50: list[float] = []
    per_round_e2e_p95: list[float] = []
    per_round_e2e_p99: list[float] = []
    per_round_e2e_max: list[float] = []
    seen_ratio_global_values: list[float] = []

    host_sampler = HostSampler(interval_sec=args.cpu_sample_sec)

    try:
        if not launch_ok:
            bench_error = f"launch gate failed: running_count={cluster_state['running_count']}/{args.node_count}"
            raise RuntimeError(bench_error)
        if not health_ok:
            bench_error = f"health gate failed: healthz_ok_count={cluster_state['healthz_ok_count']}/{args.node_count}"
            raise RuntimeError(bench_error)

        host_sampler.start()
        stabilize_deadline = time.time() + max(0, args.stabilize_sec)
        while time.time() <= stabilize_deadline:
            sample = collect_overlay_snapshot(
                ports=ports,
                node_count=args.node_count,
                global_topic=args.global_topic,
                sudo_pw=sudo_pw,
            )
            stabilization_samples.append(sample)
            if len(stabilization_samples) >= 6:
                window = stabilization_samples[-6:]
                ok, reasons = stabilization_gate_pass(window, args.node_count)
                if ok:
                    stabilization_pass = True
                    stabilization_reasons = []
                    break
                stabilization_reasons = reasons
            time.sleep(max(0.5, args.stabilize_poll_sec))

        if not stabilization_pass:
            bench_error = "bootstrap_platform:stabilization_gate_timeout"
            raise RuntimeError(bench_error)

        for round_index in range(1, rounds + 1):
            role_index = (round_index - 1) % len(publisher_ports)
            publisher_port = publisher_ports[role_index]
            publisher_role = publisher_roles[role_index]
            task_id = f"scalebench-{args.label}-g{round_index:02d}"
            body = {
                "task_id": task_id,
                "taxonomy": "crowd.data_labeling",
                "topics": [args.global_topic],
                "summary": f"{args.label} global round {round_index}",
                "detail": build_payload_detail(args.label, round_index, args.payload_bytes),
                "conf": 900,
                "ttl_sec": 3600,
            }

            lo_before = read_iface_bytes("lo")
            phys_before = read_iface_bytes(phys_iface)
            start = time.time()
            publish_result = post_json(publisher_port, "/v1/tasks/publish", body)
            detail_ref = publish_result.get("detail_ref") or ""

            first_header: dict[int, float] = {}
            first_payload: dict[int, float] = {}
            fetch_attempted: set[int] = set()
            deadline = start + args.observe_sec
            last_new_header = start

            while time.time() < deadline:
                for port in ports:
                    if port not in first_header and task_visible(port, task_id):
                        seen_at = time.time()
                        first_header[port] = seen_at
                        last_new_header = seen_at
                    if (
                        args.latency_mode == "payload_fetch"
                        and detail_ref
                        and port in first_header
                        and port not in fetch_attempted
                    ):
                        fetch_attempted.add(port)
                        if fetch_payload(port, detail_ref):
                            first_payload[port] = time.time()

                now = time.time()
                if args.latency_mode == "payload_fetch":
                    all_done = len(first_header) == args.node_count and (
                        not detail_ref or len(first_payload) == len(first_header)
                    )
                else:
                    all_done = len(first_header) == args.node_count
                if all_done and (now - last_new_header) >= 2:
                    break
                time.sleep(args.poll_sec)

            lo_after = read_iface_bytes("lo")
            phys_after = read_iface_bytes(phys_iface)
            header_ms = [round((seen_at - start) * 1000.0, 2) for seen_at in first_header.values()]
            if args.latency_mode == "payload_fetch":
                payload_ms = [round((seen_at - start) * 1000.0, 2) for seen_at in first_payload.values()]
                e2e_ms = list(payload_ms)
            else:
                payload_ms = [
                    round((seen_at - start) * 1000.0, 2)
                    for port, seen_at in first_header.items()
                    if port != publisher_port
                ]
                e2e_ms = list(header_ms)

            header_stats = stats(header_ms)
            payload_stats = stats(payload_ms)
            e2e_stats = stats(e2e_ms)
            round_cover90 = cover_latency(e2e_ms, math.ceil(args.node_count * 0.90))
            round_cover95 = cover_latency(e2e_ms, math.ceil(args.node_count * 0.95))
            round_cover99 = cover_latency(e2e_ms, math.ceil(args.node_count * 0.99))
            round_cover100 = cover_latency(e2e_ms, args.node_count)

            if round_cover90 is not None:
                cover90_global.append(round_cover90)
            if round_cover95 is not None:
                cover95_global.append(round_cover95)
            if round_cover99 is not None:
                cover99_global.append(round_cover99)
            if round_cover100 is not None:
                cover100_global.append(round_cover100)
            if header_stats.get("p50") is not None:
                per_round_header_p50.append(float(header_stats["p50"]))
            if header_stats.get("p95") is not None:
                per_round_header_p95.append(float(header_stats["p95"]))
            if header_stats.get("p99") is not None:
                per_round_header_p99.append(float(header_stats["p99"]))
            if payload_stats.get("p50") is not None:
                per_round_payload_p50.append(float(payload_stats["p50"]))
            if payload_stats.get("p95") is not None:
                per_round_payload_p95.append(float(payload_stats["p95"]))
            if payload_stats.get("p99") is not None:
                per_round_payload_p99.append(float(payload_stats["p99"]))
            if e2e_stats.get("p50") is not None:
                per_round_e2e_p50.append(float(e2e_stats["p50"]))
            if e2e_stats.get("p95") is not None:
                per_round_e2e_p95.append(float(e2e_stats["p95"]))
            if e2e_stats.get("p99") is not None:
                per_round_e2e_p99.append(float(e2e_stats["p99"]))
            if e2e_stats.get("max") is not None:
                per_round_e2e_max.append(float(e2e_stats["max"]))

            header_lat_all.extend(header_ms)
            payload_lat_all.extend(payload_ms)
            e2e_lat_all.extend(e2e_ms)
            seen_ratio_global_values.append(round(len(first_header) / max(1, args.node_count), 4))

            overlay = collect_overlay_snapshot(
                ports=ports,
                node_count=args.node_count,
                global_topic=args.global_topic,
                sudo_pw=sudo_pw,
            )
            if args.latency_mode == "payload_fetch":
                payload_fetch_attempts = len(fetch_attempted)
                payload_fetch_success_count = len(first_payload)
                payload_fetch_success_ratio = round(
                    len(first_payload) / max(1, len(fetch_attempted)),
                    4,
                )
            else:
                payload_fetch_attempts = 0
                payload_fetch_success_count = 0
                payload_fetch_success_ratio = None
            row = {
                "round_index": round_index,
                "task_id": task_id,
                "publisher_port": publisher_port,
                "publisher_role": publisher_role,
                "detail_ref": detail_ref,
                "topic": args.global_topic,
                "latency_mode": args.latency_mode,
                "seen_count_global": len(first_header),
                "seen_ratio_global": round(len(first_header) / max(1, args.node_count), 4),
                "payload_fetch_attempts": payload_fetch_attempts,
                "payload_fetch_success_count": payload_fetch_success_count,
                "payload_fetch_success_ratio": payload_fetch_success_ratio,
                "header_latency_ms": header_stats,
                "payload_latency_ms": payload_stats,
                "e2e_latency_ms": e2e_stats,
                "cover90_ms_global": round_cover90,
                "cover95_ms_global": round_cover95,
                "cover99_ms_global": round_cover99,
                "cover100_ms_global": round_cover100,
                "max_latency_ms": e2e_stats.get("max"),
                "net_lo_rx_bytes_delta": lo_after["rx_bytes"] - lo_before["rx_bytes"],
                "net_lo_tx_bytes_delta": lo_after["tx_bytes"] - lo_before["tx_bytes"],
                "net_lo_bytes_delta_total": (
                    (lo_after["rx_bytes"] - lo_before["rx_bytes"])
                    + (lo_after["tx_bytes"] - lo_before["tx_bytes"])
                ),
                f"net_{phys_iface}_rx_bytes_delta": phys_after["rx_bytes"] - phys_before["rx_bytes"],
                f"net_{phys_iface}_tx_bytes_delta": phys_after["tx_bytes"] - phys_before["tx_bytes"],
                f"net_{phys_iface}_bytes_delta_total": (
                    (phys_after["rx_bytes"] - phys_before["rx_bytes"])
                    + (phys_after["tx_bytes"] - phys_before["tx_bytes"])
                ),
                "overlay": overlay,
            }
            round_rows.append(row)

            for port in ports:
                matrix_rows.append(
                    {
                        "round_index": round_index,
                        "task_id": task_id,
                        "topic": args.global_topic,
                        "publisher_port": publisher_port,
                        "publisher_role": publisher_role,
                        "api_port": port,
                        "seen": port in first_header,
                        "payload_fetched": port in first_payload if args.latency_mode == "payload_fetch" else None,
                        "first_seen_ms": round((first_header[port] - start) * 1000.0, 2)
                        if port in first_header
                        else None,
                        "payload_done_ms": round((first_payload[port] - start) * 1000.0, 2)
                        if port in first_payload
                        else None,
                    }
                )
            with (out / "per_round.jsonl").open("a") as handle:
                handle.write(json.dumps(row) + "\n")
            time.sleep(10)
    except Exception as exc:
        bench_error = bench_error or f"{type(exc).__name__}: {exc}"
    finally:
        host_sampler.stop()
        write_host_samples(out, host_sampler.samples)
        with (out / "stabilization_samples.jsonl").open("w") as handle:
            for sample in stabilization_samples:
                handle.write(json.dumps(sample) + "\n")
        with (out / "first_seen_matrix.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "round_index",
                    "task_id",
                    "topic",
                    "publisher_port",
                    "publisher_role",
                    "api_port",
                    "seen",
                    "first_seen_ms",
                    "payload_fetched",
                    "payload_done_ms",
                ],
            )
            writer.writeheader()
            writer.writerows(matrix_rows)

        cpu_values = [float(sample["cpu_pct"]) for sample in host_sampler.samples if "cpu_pct" in sample]
        load_samples = [float(sample["load1"]) for sample in host_sampler.samples if "load1" in sample]
        mem_samples = [float(sample["mem_used_bytes"]) for sample in host_sampler.samples if "mem_used_bytes" in sample]
        process_samples = [float(sample["process_count"]) for sample in host_sampler.samples if "process_count" in sample]
        thread_samples = [float(sample["thread_count"]) for sample in host_sampler.samples if "thread_count" in sample]
        ctx_samples = [float(sample["ctx_switches"]) for sample in host_sampler.samples if "ctx_switches" in sample]
        socket_samples = [float(sample["socket_count"]) for sample in host_sampler.samples if "socket_count" in sample]

        final_overlay = (
            round_rows[-1]["overlay"]
            if round_rows
            else (
                stabilization_samples[-1]
                if stabilization_samples
                else collect_overlay_snapshot(
                    ports=ports,
                    node_count=args.node_count,
                    global_topic=args.global_topic,
                    sudo_pw=sudo_pw,
                )
            )
        )
        tcp_values = [float(sample.get("established_tcp_count") or 0.0) for sample in stabilization_samples]
        if not tcp_values:
            tcp_values = [float(final_overlay.get("established_tcp_count") or 0.0)]

        cluster_summary = cluster_state.get("summary", {})
        neighbor_hard_cap = int(cluster_summary.get("neighbor_hard_cap", 8))
        bootstrap_connect_count = int(cluster_summary.get("bootstrap_connect_count", 2))
        peer_per_node_p50 = final_overlay.get("peer_per_node_p50")
        mesh_per_node_p50 = final_overlay.get("mesh_per_node_p50")
        tcp_per_node_avg = final_overlay.get("tcp_per_node_avg")
        peer_limit = neighbor_hard_cap + bootstrap_connect_count + 2
        connection_scaling_alert = (
            peer_per_node_p50 is not None and float(peer_per_node_p50) > float(peer_limit)
        )
        connection_alert_reasons = []
        if connection_scaling_alert:
            connection_alert_reasons.append("peer_density_above_limit")

        round_count = len(round_rows)
        expected_round_count = rounds
        bench_ok = (
            bench_error is None
            and stabilization_pass
            and round_count == expected_round_count
            and stats(per_round_e2e_p95).get("count", 0) == round_count
            and stats(seen_ratio_global_values).get("p50", 0) == 1.0
        )

        summary = {
            "label": args.label,
            "variant": args.variant,
            "topic_mode": "global_only",
            "global_topic": args.global_topic,
            "latency_mode": args.latency_mode,
            "node_count": args.node_count,
            "seed_count": len(cluster_state["seed_indexes"]),
            "seed_indexes": cluster_state["seed_indexes"],
            "publisher_ports": publisher_ports,
            "publisher_roles": publisher_roles,
            "round_count": round_count,
            "expected_round_count": expected_round_count,
            "launch_ok": cluster_state["launch_ok"],
            "health_ok": cluster_state["health_ok"],
            "bootstrap_pass": bool(cluster_state["launch_ok"] and cluster_state["health_ok"] and stabilization_pass),
            "stabilization_pass": stabilization_pass,
            "stabilization_fail_reasons": stabilization_reasons,
            "bench_ok": bench_ok,
            "bench_error": bench_error,
            "running_count": cluster_state["running_count"],
            "healthz_ok_count": cluster_state["healthz_ok_count"],
            "neighbor_hard_cap": neighbor_hard_cap,
            "bootstrap_connect_count": bootstrap_connect_count,
            "bootstrap_time_sec": cluster_summary.get("bootstrap_time_sec"),
            "header_latency_ms": stats(header_lat_all),
            "payload_latency_ms": stats(payload_lat_all),
            "e2e_latency_ms": stats(e2e_lat_all),
            "cover90_ms_global": stats(cover90_global),
            "cover95_ms_global": stats(cover95_global),
            "cover99_ms_global": stats(cover99_global),
            "cover100_ms_global": stats(cover100_global),
            "max_latency_ms": stats(per_round_e2e_max),
            "target_cover50_ms": stats([row for row in per_round_header_p50]),
            "target_cover95_ms": stats([row for row in per_round_header_p95]),
            "target_cover99_ms": stats([row for row in per_round_header_p99]),
            "target_cover100_ms": stats(cover100_global),
            "global_cover50_ms": stats(cover90_global),
            "global_cover90_ms": stats(cover90_global),
            "global_cover95_ms": stats(cover95_global),
            "global_cover99_ms": stats(cover99_global),
            "global_cover100_ms": stats(cover100_global),
            "payload_cover50_ms": stats(per_round_payload_p50),
            "payload_cover95_ms": stats(per_round_payload_p95),
            "payload_cover99_ms": stats(per_round_payload_p99),
            "payload_cover100_ms": stats(cover100_global),
            "e2e_cover50_ms": stats(per_round_e2e_p50),
            "e2e_cover95_ms": stats(per_round_e2e_p95),
            "e2e_cover99_ms": stats(per_round_e2e_p99),
            "e2e_cover100_ms": stats(cover100_global),
            "e2e_max_ms": stats(per_round_e2e_max),
            "seen_ratio_global": stats(seen_ratio_global_values),
            "payload_fetch_success_ratio": stats(
                [row.get("payload_fetch_success_ratio") for row in round_rows]
            ),
            "payload_fetch_attempts": stats([row.get("payload_fetch_attempts") for row in round_rows]),
            "payload_fetch_success_count": stats([row.get("payload_fetch_success_count") for row in round_rows]),
            "load1": stats(load_samples),
            "mem_used_bytes": stats(mem_samples),
            "host_cpu_busy_pct": stats(cpu_values),
            "process_count": stats(process_samples),
            "thread_count": stats(thread_samples),
            "ctx_switches": stats(ctx_samples),
            "socket_count": stats(socket_samples),
            "tcp_count": stats(tcp_values),
            "peer_count": final_overlay.get("peer_count", {"count": 0}),
            "general_topic_mesh_size": final_overlay.get("general_topic_mesh_size", {"count": 0}),
            "established_tcp_count": int(final_overlay.get("established_tcp_count") or 0),
            "tcp_per_node_avg": tcp_per_node_avg,
            "peer_per_node_p50": peer_per_node_p50,
            "mesh_per_node_p50": mesh_per_node_p50,
            "connection_scaling_alert": connection_scaling_alert,
            "connection_scaling_alert_reasons": connection_alert_reasons,
            "result_dir": str(out),
        }
        (out / "summary.json").write_text(json.dumps(summary, indent=2))
        print(json.dumps(summary, indent=2))

    if bench_error is not None:
        raise SystemExit(bench_error)


if __name__ == "__main__":
    main()
