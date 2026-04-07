#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import statistics
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_IMAGE = "openagent-node:formal"
DEFAULT_GENERAL_TOPIC = "openagent/v1/general"
DEFAULT_RENDEZVOUS = "openagent/v1/dev"


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def append_event(path: Path, event: str, **fields: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"ts": now_iso(), "event": event}
    record.update(fields)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True) + "\n")


def read_loadavg1() -> float:
    return float(Path("/proc/loadavg").read_text(encoding="utf-8").split()[0])


def safe_shell(cmd: str) -> str:
    try:
        return subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        return exc.output or str(exc)


def parse_size_to_bytes(raw_value: str) -> Optional[int]:
    token = raw_value.strip()
    if not token:
        return None
    token = token.replace("iB", "B")
    units = {
        "B": 1,
        "KB": 1000,
        "MB": 1000 ** 2,
        "GB": 1000 ** 3,
        "TB": 1000 ** 4,
        "K": 1024,
        "M": 1024 ** 2,
        "G": 1024 ** 3,
        "T": 1024 ** 4,
    }
    number = token
    unit = "B"
    for suffix in sorted(units.keys(), key=len, reverse=True):
        if token.endswith(suffix):
            number = token[: -len(suffix)].strip()
            unit = suffix
            break
    try:
        return int(float(number) * units[unit])
    except Exception:
        return None


def parse_docker_stats(raw_text: str) -> Dict[str, Optional[float]]:
    cpu_values: List[float] = []
    memory_values: List[float] = []
    container_count = 0
    for line in raw_text.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 3 or not parts[0].startswith("oa-formal-"):
            continue
        container_count += 1
        cpu_text = parts[1].strip()
        if cpu_text.endswith("%"):
            cpu_text = cpu_text[:-1]
        try:
            cpu_values.append(float(cpu_text))
        except ValueError:
            pass
        memory_text = parts[2].strip().split("/", 1)[0].strip()
        memory_bytes = parse_size_to_bytes(memory_text)
        if memory_bytes is not None:
            memory_values.append(float(memory_bytes))
    return {
        "docker_cpu_pct_sum": round(sum(cpu_values), 2) if cpu_values else None,
        "container_memory_usage_bytes_sum": round(sum(memory_values), 2) if memory_values else None,
        "docker_stats_container_count": float(container_count),
    }


def connection_count_from_output(raw_text: str) -> int:
    lines = [line for line in raw_text.splitlines() if line.strip()]
    if not lines:
        return 0
    if lines[0].lower().startswith("recv-q"):
        return max(0, len(lines) - 1)
    return len(lines)


def stat_block(values: List[Optional[float]]) -> Dict[str, Optional[float]]:
    cleaned = sorted(float(value) for value in values if value is not None)
    if not cleaned:
        return {
            "p50": None,
            "p95": None,
            "max": None,
        }
    count = len(cleaned)
    p50_index = max(0, round((count - 1) * 0.50))
    p95_index = max(0, round((count - 1) * 0.95))
    return {
        "p50": round(cleaned[p50_index], 2),
        "p95": round(cleaned[p95_index], 2),
        "max": round(cleaned[-1], 2),
    }


def overlay_snapshot(api_base: int, node_count: int, global_topic: str) -> Dict[str, Optional[float]]:
    peer_counts: List[Optional[float]] = []
    mesh_sizes: List[Optional[float]] = []
    for port in range(api_base, api_base + node_count):
        peers_payload = get_json(port, "/v1/peers")
        if isinstance(peers_payload, list):
            peer_counts.append(float(len(peers_payload)))
        else:
            peer_counts.append(None)

        pubsub_payload = get_json(port, "/v1/pubsub")
        mesh = None
        if isinstance(pubsub_payload, dict):
            topic_mesh = pubsub_payload.get("topic_mesh") or {}
            if isinstance(topic_mesh, dict):
                peers = topic_mesh.get(global_topic) or []
                if isinstance(peers, list):
                    mesh = float(len(peers))
        mesh_sizes.append(mesh)

    peer_stats = stat_block(peer_counts)
    mesh_stats = stat_block(mesh_sizes)
    return {
        "peer_count_p50": peer_stats["p50"],
        "peer_count_p95": peer_stats["p95"],
        "peer_count_max": peer_stats["max"],
        "general_topic_mesh_size_p50": mesh_stats["p50"],
        "general_topic_mesh_size_p95": mesh_stats["p95"],
        "general_topic_mesh_size_max": mesh_stats["max"],
    }


def get_json(port: int, path: str, timeout: float = 3.0) -> Optional[Dict[str, Any]]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=timeout) as response:
            return json.loads(response.read().decode())
    except Exception:
        return None


def run_command(cmd: List[str], sudo_password: Optional[str] = None) -> subprocess.CompletedProcess[str]:
    if sudo_password is None:
        return subprocess.run(cmd, check=False, text=True, capture_output=True)
    sudo_cmd = ["sudo", "-S"] + cmd
    return subprocess.run(
        sudo_cmd,
        input=sudo_password + "\n",
        check=False,
        text=True,
        capture_output=True,
    )


def docker_count(name_prefix: str, sudo_password: str, *, include_stopped: bool = False) -> int:
    cmd = [
        "docker",
        "ps",
    ]
    if include_stopped:
        cmd.append("-a")
    cmd.extend(
        [
            "--format",
            "{{.Names}}",
        ]
    )
    result = run_command(
        cmd,
        sudo_password=sudo_password,
    )
    if result.returncode != 0:
        return 0
    return sum(1 for line in result.stdout.splitlines() if line.startswith(name_prefix))


def cleanup_environment(root: Path, sudo_password: str, event_log_path: Path) -> None:
    append_event(event_log_path, "cleanup_begin", root=str(root))
    remaining = docker_count("oa-formal-", sudo_password, include_stopped=True)
    for attempt in range(1, 4):
        if remaining == 0:
            break
        append_event(event_log_path, "cleanup_remove_attempt", attempt=attempt, remaining=remaining)
        run_command(
            ["bash", "-lc", "docker ps -aq --filter name=oa-formal- | xargs -r docker rm -f"],
            sudo_password=sudo_password,
        )
        time.sleep(3.0)
        remaining = docker_count("oa-formal-", sudo_password, include_stopped=True)

    if remaining != 0:
        append_event(event_log_path, "cleanup_restart_docker", remaining=remaining)
        run_command(["systemctl", "restart", "docker"], sudo_password=sudo_password)
        time.sleep(5.0)
        run_command(
            ["bash", "-lc", "docker ps -aq --filter name=oa-formal- | xargs -r docker rm -f"],
            sudo_password=sudo_password,
        )
        time.sleep(3.0)
        remaining = docker_count("oa-formal-", sudo_password, include_stopped=True)
        append_event(event_log_path, "cleanup_post_restart", remaining=remaining)

    run_command(["rm", "-rf", str(root)], sudo_password=sudo_password)
    ensure_dir(root)
    append_event(event_log_path, "cleanup_end", root=str(root), remaining=remaining)


def seed_indexes(total: int, seed_count: int) -> List[int]:
    return list(range(1, min(total, seed_count) + 1))


def batch_profile(total: int, batch_size_small: int, batch_size_medium: int, batch_pause_sec: int) -> Tuple[int, int]:
    if total <= 100:
        return batch_size_small, batch_pause_sec
    if total <= 200:
        return batch_size_medium, batch_pause_sec
    return batch_size_medium, batch_pause_sec


class LoadavgSampler:
    def __init__(self, interval_sec: float = 1.0) -> None:
        self.interval_sec = interval_sec
        self.samples: List[Dict[str, Any]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self.interval_sec * 2 + 1.0)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_sec):
            self.samples.append({"ts": now_iso(), "loadavg1": read_loadavg1()})


def write_samples(path: Path, samples: List[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for sample in samples:
            handle.write(json.dumps(sample, ensure_ascii=True) + "\n")


def live_health_count(api_base: int, node_count: int) -> int:
    count = 0
    for port in range(api_base, api_base + node_count):
        if get_json(port, "/healthz") is not None:
            count += 1
    return count


def stabilization_snapshot(
    node_count: int,
    api_base: int,
    global_topic: str,
    sudo_password: str,
    event_log_path: Path,
    started_at: float,
) -> Dict[str, Any]:
    docker_stats_cmd = (
        "printf '%s\\n' | sudo -S docker stats --no-stream "
        "--format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.PIDs}}' "
        "$(printf 'oa-formal-%%s ' $(seq 1 %d))"
    ) % (sudo_password, node_count)
    docker_stats_text = safe_shell(docker_stats_cmd)
    ss_text = safe_shell("ss -tan state established")
    docker_stats = parse_docker_stats(docker_stats_text)
    overlay = overlay_snapshot(api_base, node_count, global_topic)
    established_tcp_count = connection_count_from_output(ss_text)
    sample = {
        "ts": now_iso(),
        "elapsed_sec": round(time.time() - started_at, 2),
        "loadavg1": read_loadavg1(),
        "docker_cpu_pct_sum": docker_stats["docker_cpu_pct_sum"],
        "established_tcp_count": established_tcp_count,
        "tcp_per_node_avg": round(float(established_tcp_count) / float(max(1, node_count)), 2),
        "healthz_ok_count": live_health_count(api_base, node_count),
        **overlay,
    }
    append_event(
        event_log_path,
        "stabilization_sample",
        elapsed_sec=sample["elapsed_sec"],
        docker_cpu_pct_sum=sample["docker_cpu_pct_sum"],
        established_tcp_count=sample["established_tcp_count"],
        peer_count_p50=sample["peer_count_p50"],
        mesh_size_p50=sample["general_topic_mesh_size_p50"],
        healthz_ok_count=sample["healthz_ok_count"],
    )
    return sample


def scalar_variation_ratio(samples: List[Dict[str, Any]], key: str) -> Optional[float]:
    values = [float(sample[key]) for sample in samples if sample.get(key) is not None]
    if not values:
        return None
    baseline = statistics.median(values)
    if baseline <= 0:
        return 0.0 if max(values) == 0.0 else None
    return round((max(values) - min(values)) / baseline, 4)


def stabilization_window_ok(
    samples: List[Dict[str, Any]],
    *,
    node_count: int,
    required_window_samples: int,
    quiescent_cpu_per_node_threshold: float,
    quiescent_conn_variation_ratio: float,
    quiescent_peer_variation_ratio: float,
    quiescent_mesh_variation_ratio: float,
) -> bool:
    if len(samples) < required_window_samples:
        return False
    window = samples[-required_window_samples:]
    if any(sample.get("healthz_ok_count") != node_count for sample in window):
        return False
    for sample in window:
        cpu_sum = sample.get("docker_cpu_pct_sum")
        if cpu_sum is None:
            return False
        if (float(cpu_sum) / float(node_count)) > float(quiescent_cpu_per_node_threshold):
            return False
    tcp_variation = scalar_variation_ratio(window, "established_tcp_count")
    if tcp_variation is None or tcp_variation > float(quiescent_conn_variation_ratio):
        return False
    peer_variation = scalar_variation_ratio(window, "peer_count_p50")
    if peer_variation is None or peer_variation > float(quiescent_peer_variation_ratio):
        return False
    mesh_variation = scalar_variation_ratio(window, "general_topic_mesh_size_p50")
    if mesh_variation is None or mesh_variation > float(quiescent_mesh_variation_ratio):
        return False
    return True


def build_docker_run_command(
    idx: int,
    *,
    root: Path,
    variant: str,
    image: str,
    p2p_base: int,
    api_base: int,
    rendezvous: str,
    global_topic: str,
    bootstraps: List[str],
    neighbor_target: int,
    neighbor_hard_cap: int,
    same_topic_neighbor_min: int,
    same_topic_mesh_min: int,
    neighbor_low_watermark: int,
    bootstrap_connect_count: int,
    bootstrap_candidate_count: int,
    bootstrap_refresh_sec: int,
    topic_replenish_batch: int,
    topic_replenish_cooldown_sec: int,
) -> List[str]:
    p2p = p2p_base + idx - 1
    api = api_base + idx - 1
    data_dir = root / ("node%d" % idx)
    ensure_dir(data_dir)
    command = [
        "docker",
        "run",
        "-d",
        "--name",
        "oa-formal-%d" % idx,
        "--network",
        "host",
        "-v",
        "%s:/data" % data_dir,
        image,
        "--profile",
        "formal%d" % idx,
        "--data-dir",
        "/data",
        "--port",
        str(p2p),
        "--api-port",
        str(api),
        "--rendezvous",
        rendezvous,
        "--experiment-variant",
        variant,
        "--topic",
        global_topic,
        "--neighbor-target",
        str(neighbor_target),
        "--neighbor-hard-cap",
        str(neighbor_hard_cap),
        "--same-topic-neighbor-min",
        str(same_topic_neighbor_min),
        "--same-topic-mesh-min",
        str(same_topic_mesh_min),
        "--neighbor-low-watermark",
        str(neighbor_low_watermark),
        "--bootstrap-connect-count",
        str(bootstrap_connect_count),
        "--bootstrap-candidate-count",
        str(bootstrap_candidate_count),
        "--bootstrap-refresh",
        "%ds" % bootstrap_refresh_sec,
        "--topic-replenish-batch",
        str(topic_replenish_batch),
        "--topic-replenish-cooldown",
        "%ds" % topic_replenish_cooldown_sec,
    ]
    for bootstrap in bootstraps:
        command.extend(["--bootstrap", bootstrap])
    return command


def start_node(
    idx: int,
    *,
    root: Path,
    variant: str,
    image: str,
    p2p_base: int,
    api_base: int,
    rendezvous: str,
    global_topic: str,
    bootstraps: List[str],
    neighbor_target: int,
    neighbor_hard_cap: int,
    same_topic_neighbor_min: int,
    same_topic_mesh_min: int,
    neighbor_low_watermark: int,
    bootstrap_connect_count: int,
    bootstrap_candidate_count: int,
    bootstrap_refresh_sec: int,
    topic_replenish_batch: int,
    topic_replenish_cooldown_sec: int,
    sudo_password: str,
    event_log_path: Path,
) -> Tuple[bool, Optional[str]]:
    command = build_docker_run_command(
        idx,
        root=root,
        variant=variant,
        image=image,
        p2p_base=p2p_base,
        api_base=api_base,
        rendezvous=rendezvous,
        global_topic=global_topic,
        bootstraps=bootstraps,
        neighbor_target=neighbor_target,
        neighbor_hard_cap=neighbor_hard_cap,
        same_topic_neighbor_min=same_topic_neighbor_min,
        same_topic_mesh_min=same_topic_mesh_min,
        neighbor_low_watermark=neighbor_low_watermark,
        bootstrap_connect_count=bootstrap_connect_count,
        bootstrap_candidate_count=bootstrap_candidate_count,
        bootstrap_refresh_sec=bootstrap_refresh_sec,
        topic_replenish_batch=topic_replenish_batch,
        topic_replenish_cooldown_sec=topic_replenish_cooldown_sec,
    )
    result = run_command(command, sudo_password=sudo_password)
    append_event(
        event_log_path,
        "container_start",
        idx=idx,
        returncode=result.returncode,
        stdout=result.stdout.strip(),
        stderr=result.stderr.strip(),
    )
    if result.returncode != 0:
        return False, result.stderr.strip() or result.stdout.strip() or "docker_run_failed"
    return True, None


def wait_health(
    api_port: int,
    *,
    deadline: float,
    metrics: Dict[str, Any],
    event_log_path: Path,
    first_healthy_ref: Dict[str, Optional[float]],
) -> Optional[Dict[str, Any]]:
    while time.time() < deadline:
        payload = get_json(api_port, "/healthz")
        if payload is not None:
            if first_healthy_ref["value"] is None:
                first_healthy_ref["value"] = time.time()
            append_event(event_log_path, "health_ok", api_port=api_port)
            return payload
        metrics["healthcheck_retry_count"] += 1
        time.sleep(1.0)
    append_event(event_log_path, "health_timeout", api_port=api_port)
    return None


def collect_bootstrap(
    idx: int,
    *,
    api_base: int,
    p2p_base: int,
    deadline: float,
    metrics: Dict[str, Any],
    event_log_path: Path,
    first_healthy_ref: Dict[str, Optional[float]],
) -> Optional[str]:
    api_port = api_base + idx - 1
    p2p_port = p2p_base + idx - 1
    payload = wait_health(
        api_port,
        deadline=deadline,
        metrics=metrics,
        event_log_path=event_log_path,
        first_healthy_ref=first_healthy_ref,
    )
    if payload is None:
        return None
    peer_id = payload.get("peer_id")
    if not peer_id:
        append_event(event_log_path, "peer_id_missing", idx=idx, api_port=api_port)
        return None
    multiaddr = "/ip4/127.0.0.1/tcp/%d/p2p/%s" % (p2p_port, peer_id)
    append_event(event_log_path, "bootstrap_collected", idx=idx, multiaddr=multiaddr)
    return multiaddr


def poll_all_healthy(
    ports: List[int],
    *,
    deadline: float,
    metrics: Dict[str, Any],
    event_log_path: Path,
    first_healthy_ref: Dict[str, Optional[float]],
) -> Tuple[int, Optional[float]]:
    healthy_ports = set()
    time_to_all = None
    while time.time() < deadline:
        for api_port in ports:
            if api_port in healthy_ports:
                continue
            payload = get_json(api_port, "/healthz")
            if payload is not None:
                healthy_ports.add(api_port)
                if first_healthy_ref["value"] is None:
                    first_healthy_ref["value"] = time.time()
                append_event(event_log_path, "health_ok", api_port=api_port)
            else:
                metrics["healthcheck_retry_count"] += 1
        if len(healthy_ports) == len(ports):
            time_to_all = time.time()
            break
        time.sleep(1.0)
    return len(healthy_ports), time_to_all


def bootstrap_fail_reason(metrics: Dict[str, Any]) -> str:
    if metrics.get("explicit_fail_reason"):
        return str(metrics["explicit_fail_reason"])
    if metrics.get("container_start_fail_count", 0) > 0:
        return "container_start_failures"
    if metrics.get("running_count", 0) < metrics.get("node_count", 0):
        return "containers_not_fully_started"
    if metrics.get("healthz_ok_count", 0) < metrics.get("node_count", 0):
        return "health_checks_incomplete"
    return "bootstrap_timeout_exceeded"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--variant", required=True)
    parser.add_argument("--node-count", required=True, type=int)
    parser.add_argument("--bootstrap-timeout-sec", type=int, default=1200)
    parser.add_argument("--warmup-sec", type=int, default=120)
    parser.add_argument("--stabilization-min-sec", type=int, default=60)
    parser.add_argument("--stabilization-timeout-sec", type=int, default=300)
    parser.add_argument("--stabilization-sample-sec", type=float, default=5.0)
    parser.add_argument("--quiescent-cpu-per-node-threshold", type=float, default=10.0)
    parser.add_argument("--quiescent-conn-variation-ratio", type=float, default=0.05)
    parser.add_argument("--quiescent-peer-variation-ratio", type=float, default=0.10)
    parser.add_argument("--quiescent-mesh-variation-ratio", type=float, default=0.10)
    parser.add_argument("--api-base", type=int, default=8500)
    parser.add_argument("--p2p-base", type=int, default=5500)
    parser.add_argument("--seed-count", type=int, default=3)
    parser.add_argument("--batch-size-small", type=int, default=10)
    parser.add_argument("--batch-size-medium", type=int, default=15)
    parser.add_argument("--batch-pause-sec", type=int, default=15)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--global-topic", default=DEFAULT_GENERAL_TOPIC)
    parser.add_argument("--rendezvous", default=DEFAULT_RENDEZVOUS)
    parser.add_argument("--neighbor-target", type=int, default=6)
    parser.add_argument("--neighbor-hard-cap", type=int, default=8)
    parser.add_argument("--same-topic-neighbor-min", type=int, default=3)
    parser.add_argument("--same-topic-mesh-min", type=int, default=4)
    parser.add_argument("--neighbor-low-watermark", type=int, default=4)
    parser.add_argument("--bootstrap-connect-count", type=int, default=2)
    parser.add_argument("--bootstrap-candidate-count", type=int, default=8)
    parser.add_argument("--bootstrap-refresh-sec", type=int, default=30)
    parser.add_argument("--topic-replenish-batch", type=int, default=1)
    parser.add_argument("--topic-replenish-cooldown-sec", type=int, default=20)
    args = parser.parse_args()

    run_root = Path(args.root).expanduser()
    event_log_path = run_root / "logs" / "bootstrap_events.jsonl"
    load_samples_path = run_root / "metrics" / "bootstrap_loadavg_samples.jsonl"
    stabilization_samples_path = run_root / "metrics" / "stabilization_samples.jsonl"
    config_snapshot_path = run_root / "configs" / "bootstrap_config.json"
    summary_path = run_root / "summaries" / "bootstrap_summary.json"
    metrics_path = run_root / "metrics" / "bootstrap_metrics.json"
    config_snapshot = {
        "root": str(run_root),
        "variant": args.variant,
        "node_count": args.node_count,
        "bootstrap_timeout_sec": args.bootstrap_timeout_sec,
        "warmup_sec": args.warmup_sec,
        "stabilization_min_sec": args.stabilization_min_sec,
        "stabilization_timeout_sec": args.stabilization_timeout_sec,
        "stabilization_sample_sec": args.stabilization_sample_sec,
        "quiescent_cpu_per_node_threshold": args.quiescent_cpu_per_node_threshold,
        "quiescent_conn_variation_ratio": args.quiescent_conn_variation_ratio,
        "quiescent_peer_variation_ratio": args.quiescent_peer_variation_ratio,
        "quiescent_mesh_variation_ratio": args.quiescent_mesh_variation_ratio,
        "api_base": args.api_base,
        "p2p_base": args.p2p_base,
        "seed_count": args.seed_count,
        "batch_size_small": args.batch_size_small,
        "batch_size_medium": args.batch_size_medium,
        "batch_pause_sec": args.batch_pause_sec,
        "image": args.image,
        "global_topic": args.global_topic,
        "rendezvous": args.rendezvous,
        "neighbor_target": args.neighbor_target,
        "neighbor_hard_cap": args.neighbor_hard_cap,
        "same_topic_neighbor_min": args.same_topic_neighbor_min,
        "same_topic_mesh_min": args.same_topic_mesh_min,
        "neighbor_low_watermark": args.neighbor_low_watermark,
        "bootstrap_connect_count": args.bootstrap_connect_count,
        "bootstrap_candidate_count": args.bootstrap_candidate_count,
        "bootstrap_refresh_sec": args.bootstrap_refresh_sec,
        "topic_replenish_batch": args.topic_replenish_batch,
        "topic_replenish_cooldown_sec": args.topic_replenish_cooldown_sec,
        "generated_at": now_iso(),
    }
    sudo_password = os.environ.get("SCALEBENCH_SUDO_PW", "ma123")
    sampler = LoadavgSampler(interval_sec=1.0)
    first_healthy_ref: Dict[str, Optional[float]] = {"value": None}
    start_time = time.time()
    deadline = start_time + float(args.bootstrap_timeout_sec)
    metrics: Dict[str, Any] = {
        "node_count": args.node_count,
        "container_start_fail_count": 0,
        "healthcheck_retry_count": 0,
        "successful_container_starts": 0,
        "explicit_fail_reason": "",
    }
    stabilization_samples: List[Dict[str, Any]] = []

    bootstrap_multiaddrs: List[str] = []
    try:
        sampler.start()
        cleanup_environment(run_root, sudo_password, run_root / "logs" / "bootstrap_events.jsonl")
        configs_dir = ensure_dir(run_root / "configs")
        logs_dir = ensure_dir(run_root / "logs")
        metrics_dir = ensure_dir(run_root / "metrics")
        ensure_dir(run_root / "results")
        summaries_dir = ensure_dir(run_root / "summaries")

        event_log_path = logs_dir / "bootstrap_events.jsonl"
        load_samples_path = metrics_dir / "bootstrap_loadavg_samples.jsonl"
        stabilization_samples_path = metrics_dir / "stabilization_samples.jsonl"
        config_snapshot_path = configs_dir / "bootstrap_config.json"
        summary_path = summaries_dir / "bootstrap_summary.json"
        metrics_path = metrics_dir / "bootstrap_metrics.json"

        write_json(config_snapshot_path, config_snapshot)
        append_event(event_log_path, "cleanup_end", root=str(run_root))

        seed_ids = seed_indexes(args.node_count, args.seed_count)
        batch_size, batch_sleep = batch_profile(
            args.node_count,
            args.batch_size_small,
            args.batch_size_medium,
            args.batch_pause_sec,
        )
        append_event(
            event_log_path,
            "bootstrap_begin",
            node_count=args.node_count,
            seed_count=len(seed_ids),
            batch_size=batch_size,
            batch_sleep=batch_sleep,
        )

        launched_ids: List[int] = []
        for idx in seed_ids:
            if time.time() >= deadline:
                metrics["explicit_fail_reason"] = "bootstrap_timeout_before_seed_launch"
                break
            ok, error_text = start_node(
                idx,
                root=run_root,
                variant=args.variant,
                image=args.image,
                p2p_base=args.p2p_base,
                api_base=args.api_base,
                rendezvous=args.rendezvous,
                global_topic=args.global_topic,
                bootstraps=bootstrap_multiaddrs,
                neighbor_target=args.neighbor_target,
                neighbor_hard_cap=args.neighbor_hard_cap,
                same_topic_neighbor_min=args.same_topic_neighbor_min,
                same_topic_mesh_min=args.same_topic_mesh_min,
                neighbor_low_watermark=args.neighbor_low_watermark,
                bootstrap_connect_count=args.bootstrap_connect_count,
                bootstrap_candidate_count=args.bootstrap_candidate_count,
                bootstrap_refresh_sec=args.bootstrap_refresh_sec,
                topic_replenish_batch=args.topic_replenish_batch,
                topic_replenish_cooldown_sec=args.topic_replenish_cooldown_sec,
                sudo_password=sudo_password,
                event_log_path=event_log_path,
            )
            if not ok:
                metrics["container_start_fail_count"] += 1
                metrics["explicit_fail_reason"] = "seed_start_failed_node_%d:%s" % (idx, error_text)
                break
            metrics["successful_container_starts"] += 1
            launched_ids.append(idx)
            multiaddr = collect_bootstrap(
                idx,
                api_base=args.api_base,
                p2p_base=args.p2p_base,
                deadline=deadline,
                metrics=metrics,
                event_log_path=event_log_path,
                first_healthy_ref=first_healthy_ref,
            )
            if not multiaddr:
                metrics["explicit_fail_reason"] = "seed_bootstrap_failed_node_%d" % idx
                break
            bootstrap_multiaddrs.append(multiaddr)
            time.sleep(1.0)

        non_seed_ids = [idx for idx in range(1, args.node_count + 1) if idx not in launched_ids]
        for offset in range(0, len(non_seed_ids), batch_size):
            if metrics["explicit_fail_reason"]:
                break
            if time.time() >= deadline:
                metrics["explicit_fail_reason"] = "bootstrap_timeout_before_non_seed_launch"
                break
            batch = non_seed_ids[offset : offset + batch_size]
            append_event(event_log_path, "non_seed_batch_begin", ids=batch, batch_index=(offset // batch_size) + 1)
            for idx in batch:
                ok, error_text = start_node(
                    idx,
                    root=run_root,
                    variant=args.variant,
                    image=args.image,
                    p2p_base=args.p2p_base,
                    api_base=args.api_base,
                    rendezvous=args.rendezvous,
                    global_topic=args.global_topic,
                    bootstraps=bootstrap_multiaddrs,
                    neighbor_target=args.neighbor_target,
                    neighbor_hard_cap=args.neighbor_hard_cap,
                    same_topic_neighbor_min=args.same_topic_neighbor_min,
                    same_topic_mesh_min=args.same_topic_mesh_min,
                    neighbor_low_watermark=args.neighbor_low_watermark,
                    bootstrap_connect_count=args.bootstrap_connect_count,
                    bootstrap_candidate_count=args.bootstrap_candidate_count,
                    bootstrap_refresh_sec=args.bootstrap_refresh_sec,
                    topic_replenish_batch=args.topic_replenish_batch,
                    topic_replenish_cooldown_sec=args.topic_replenish_cooldown_sec,
                    sudo_password=sudo_password,
                    event_log_path=event_log_path,
                )
                if ok:
                    metrics["successful_container_starts"] += 1
                    launched_ids.append(idx)
                else:
                    metrics["container_start_fail_count"] += 1
                    append_event(event_log_path, "non_seed_start_failed", idx=idx, error=error_text)
                time.sleep(0.2)
            append_event(event_log_path, "non_seed_batch_end", ids=batch, running_count=docker_count("oa-formal-", sudo_password))
            if offset + batch_size < len(non_seed_ids):
                time.sleep(batch_sleep)

        ports = list(range(args.api_base, args.api_base + args.node_count))
        healthz_ok_count, all_healthy_at = poll_all_healthy(
            ports,
            deadline=deadline,
            metrics=metrics,
            event_log_path=event_log_path,
            first_healthy_ref=first_healthy_ref,
        )
        running_count = docker_count("oa-formal-", sudo_password)
        elapsed = time.time() - start_time
        time_to_first_healthy = None
        if first_healthy_ref["value"] is not None:
            time_to_first_healthy = round(first_healthy_ref["value"] - start_time, 2)
        time_to_all_healthy = None
        if all_healthy_at is not None:
            time_to_all_healthy = round(all_healthy_at - start_time, 2)

        load_values = [sample["loadavg1"] for sample in sampler.samples]
        max_loadavg = round(max(load_values), 2) if load_values else round(read_loadavg1(), 2)
        bootstrap_elapsed_sec = round(elapsed, 2)
        container_create_rate = round(
            float(metrics["successful_container_starts"]) / max(1.0, elapsed),
            4,
        )
        launch_ok = (
            metrics["container_start_fail_count"] == 0
            and running_count == args.node_count
        )
        health_ok = healthz_ok_count == args.node_count
        bootstrap_ready = (
            launch_ok
            and health_ok
            and time_to_all_healthy is not None
            and time_to_all_healthy <= float(args.bootstrap_timeout_sec)
            and not metrics["explicit_fail_reason"]
        )
        stabilization_passed: Optional[bool] = None
        stabilization_elapsed_sec = None
        stabilization_started_at = None
        required_window_samples = max(1, int(math.ceil(float(args.stabilization_min_sec) / float(args.stabilization_sample_sec))))
        if bootstrap_ready and args.warmup_sec > 0:
            append_event(event_log_path, "warmup_begin", warmup_sec=args.warmup_sec)
            time.sleep(args.warmup_sec)
            append_event(event_log_path, "warmup_end", warmup_sec=args.warmup_sec)
        if bootstrap_ready:
            stabilization_passed = False
            stabilization_started_at = time.time()
            append_event(
                event_log_path,
                "stabilization_begin",
                stabilization_min_sec=args.stabilization_min_sec,
                stabilization_timeout_sec=args.stabilization_timeout_sec,
                stabilization_sample_sec=args.stabilization_sample_sec,
                required_window_samples=required_window_samples,
            )
            stabilization_deadline = time.time() + float(args.stabilization_timeout_sec)
            while time.time() < stabilization_deadline:
                sample = stabilization_snapshot(
                    args.node_count,
                    args.api_base,
                    args.global_topic,
                    sudo_password,
                    event_log_path,
                    stabilization_started_at,
                )
                stabilization_samples.append(sample)
                if stabilization_window_ok(
                    stabilization_samples,
                    node_count=args.node_count,
                    required_window_samples=required_window_samples,
                    quiescent_cpu_per_node_threshold=args.quiescent_cpu_per_node_threshold,
                    quiescent_conn_variation_ratio=args.quiescent_conn_variation_ratio,
                    quiescent_peer_variation_ratio=args.quiescent_peer_variation_ratio,
                    quiescent_mesh_variation_ratio=args.quiescent_mesh_variation_ratio,
                ):
                    stabilization_passed = True
                    stabilization_elapsed_sec = round(time.time() - stabilization_started_at, 2)
                    break
                time.sleep(args.stabilization_sample_sec)
            append_event(
                event_log_path,
                "stabilization_end",
                stabilization_passed=stabilization_passed,
                stabilization_elapsed_sec=stabilization_elapsed_sec,
                sample_count=len(stabilization_samples),
            )
        elapsed = time.time() - start_time
        bootstrap_elapsed_sec = round(elapsed, 2)
        bootstrap_pass = bootstrap_ready

        final_stabilization_sample = stabilization_samples[-1] if stabilization_samples else {}
        summary = {
            "variant": args.variant,
            "node_count": args.node_count,
            "launch_ok": launch_ok,
            "health_ok": health_ok,
            "bootstrap_pass": bootstrap_pass,
            "ceiling_type": "none" if bootstrap_pass else "bootstrap_platform",
            "bootstrap_timeout_sec": args.bootstrap_timeout_sec,
            "warmup_sec": args.warmup_sec,
            "stabilization_min_sec": args.stabilization_min_sec,
            "stabilization_timeout_sec": args.stabilization_timeout_sec,
            "stabilization_sample_sec": args.stabilization_sample_sec,
            "stabilization_window_samples": required_window_samples,
            "stabilization_passed": stabilization_passed,
            "stabilization_elapsed_sec": stabilization_elapsed_sec,
            "stabilization_fail_reason": "" if stabilization_passed or not bootstrap_ready else "stabilization_timeout",
            "quiescent_cpu_per_node_threshold": args.quiescent_cpu_per_node_threshold,
            "quiescent_conn_variation_ratio": args.quiescent_conn_variation_ratio,
            "quiescent_peer_variation_ratio": args.quiescent_peer_variation_ratio,
            "quiescent_mesh_variation_ratio": args.quiescent_mesh_variation_ratio,
            "bootstrap_elapsed_sec": bootstrap_elapsed_sec,
            "time_to_first_healthy_sec": time_to_first_healthy,
            "time_to_all_healthy_sec": time_to_all_healthy,
            "max_loadavg_during_bootstrap": max_loadavg,
            "container_create_rate": container_create_rate,
            "container_start_fail_count": metrics["container_start_fail_count"],
            "healthcheck_retry_count": metrics["healthcheck_retry_count"],
            "running_count": running_count,
            "healthz_ok_count": healthz_ok_count,
            "global_topic": args.global_topic,
            "seed_strategy": "global_fixed_count",
            "seed_count": len(seed_ids),
            "neighbor_target": args.neighbor_target,
            "neighbor_hard_cap": args.neighbor_hard_cap,
            "same_topic_neighbor_min": args.same_topic_neighbor_min,
            "same_topic_mesh_min": args.same_topic_mesh_min,
            "neighbor_low_watermark": args.neighbor_low_watermark,
            "bootstrap_connect_count": args.bootstrap_connect_count,
            "bootstrap_candidate_count": args.bootstrap_candidate_count,
            "topic_replenish_batch": args.topic_replenish_batch,
            "topic_replenish_cooldown_sec": args.topic_replenish_cooldown_sec,
            "batch_size": batch_size,
            "batch_sleep_sec": batch_sleep,
            "established_tcp_count": final_stabilization_sample.get("established_tcp_count"),
            "tcp_per_node_avg": final_stabilization_sample.get("tcp_per_node_avg"),
            "peer_count_p50": final_stabilization_sample.get("peer_count_p50"),
            "peer_count_p95": final_stabilization_sample.get("peer_count_p95"),
            "peer_count_max": final_stabilization_sample.get("peer_count_max"),
            "general_topic_mesh_size_p50": final_stabilization_sample.get("general_topic_mesh_size_p50"),
            "general_topic_mesh_size_p95": final_stabilization_sample.get("general_topic_mesh_size_p95"),
            "general_topic_mesh_size_max": final_stabilization_sample.get("general_topic_mesh_size_max"),
            "bootstrap_fail_reason": "" if bootstrap_pass else bootstrap_fail_reason(
                {
                    "explicit_fail_reason": metrics["explicit_fail_reason"],
                    "container_start_fail_count": metrics["container_start_fail_count"],
                    "running_count": running_count,
                    "healthz_ok_count": healthz_ok_count,
                    "node_count": args.node_count,
                }
            ),
            "bootstrap_multiaddr_count": len(bootstrap_multiaddrs),
        }
        write_json(summary_path, summary)
        write_json(metrics_path, summary)
        write_samples(load_samples_path, sampler.samples)
        write_samples(stabilization_samples_path, stabilization_samples)
        append_event(
            event_log_path,
            "bootstrap_end",
            bootstrap_pass=bootstrap_pass,
            running_count=running_count,
            healthz_ok_count=healthz_ok_count,
            fail_reason=summary["bootstrap_fail_reason"],
        )
        print(json.dumps(summary, indent=2))
    except Exception as exc:
        sampler.stop()
        ensure_dir(load_samples_path.parent)
        ensure_dir(summary_path.parent)
        ensure_dir(metrics_path.parent)
        ensure_dir(event_log_path.parent)
        write_samples(load_samples_path, sampler.samples)
        write_samples(stabilization_samples_path, stabilization_samples)
        summary = {
            "variant": args.variant,
            "node_count": args.node_count,
            "launch_ok": False,
            "health_ok": False,
            "bootstrap_pass": False,
            "ceiling_type": "bootstrap_platform",
            "bootstrap_timeout_sec": args.bootstrap_timeout_sec,
            "warmup_sec": args.warmup_sec,
            "stabilization_min_sec": args.stabilization_min_sec,
            "stabilization_timeout_sec": args.stabilization_timeout_sec,
            "stabilization_sample_sec": args.stabilization_sample_sec,
            "stabilization_window_samples": max(
                1, int(math.ceil(float(args.stabilization_min_sec) / float(args.stabilization_sample_sec)))
            ),
            "stabilization_passed": None,
            "stabilization_elapsed_sec": None,
            "stabilization_fail_reason": "",
            "bootstrap_elapsed_sec": round(time.time() - start_time, 2),
            "time_to_first_healthy_sec": None,
            "time_to_all_healthy_sec": None,
            "max_loadavg_during_bootstrap": round(
                max([sample["loadavg1"] for sample in sampler.samples] or [read_loadavg1()]),
                2,
            ),
            "container_create_rate": 0.0,
            "container_start_fail_count": metrics["container_start_fail_count"],
            "healthcheck_retry_count": metrics["healthcheck_retry_count"],
            "running_count": docker_count("oa-formal-", sudo_password),
            "healthz_ok_count": 0,
            "global_topic": args.global_topic,
            "seed_strategy": "global_fixed_count",
            "seed_count": args.seed_count,
            "bootstrap_fail_reason": "unexpected_exception:%s" % exc,
        }
        write_json(summary_path, summary)
        write_json(metrics_path, summary)
        append_event(event_log_path, "bootstrap_exception", error=str(exc))
        print(json.dumps(summary, indent=2))
    finally:
        sampler.stop()


if __name__ == "__main__":
    main()
