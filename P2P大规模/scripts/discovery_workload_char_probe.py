#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shlex
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml

from discovery_workload_lib import (
    DEFAULT_CAPABILITY_TAGS,
    DEFAULT_DEADLINE_CLASSES,
    DEFAULT_DOMAINS,
    DEFAULT_FAMILIES,
    DEFAULT_LATENCY_TIERS,
    DEFAULT_SPECIALIZATION_TAGS,
    DEFAULT_TOOLS,
    DEFAULT_TRUST_DOMAINS,
    dump_json,
    ensure_dir,
    now_iso,
)


DEFAULT_REMOTE = "ma@172.31.100.142"
DEFAULT_PORT = "30002"
DEFAULT_PASSWORD = "ma123"
DEFAULT_REMOTE_SUDO_PASSWORD = "ma123"
DEFAULT_REMOTE_TOOLS_DIR = "/home/ma/discovery_workload_tools"
DEFAULT_JUMP_HOST = ""
DEFAULT_JUMP_PORT = "22"
DEFAULT_JUMP_USER = ""
DEFAULT_JUMP_PASSWORD = ""
DEFAULT_VARIANT = "ablation_no_semantic_public_topic"
DEFAULT_PROFILE_NAME = "workload_char_public_discovery_two_stage_n100"

REMOTE = DEFAULT_REMOTE
PORT = DEFAULT_PORT
PASSWORD = DEFAULT_PASSWORD
REMOTE_SUDO_PASSWORD = DEFAULT_REMOTE_SUDO_PASSWORD
JUMP_HOST = DEFAULT_JUMP_HOST
JUMP_PORT = DEFAULT_JUMP_PORT
JUMP_USER = DEFAULT_JUMP_USER
JUMP_PASSWORD = DEFAULT_JUMP_PASSWORD


def run(cmd: List[str], cwd: Path | None = None) -> None:
    subprocess.run(cmd, cwd=cwd, check=True)


def run_capture(cmd: List[str], cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        cmd,
        cwd=cwd,
        check=False,
        text=True,
        capture_output=True,
    )
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            cmd,
            output=result.stdout,
            stderr=result.stderr,
        )
    return result


def run_capture_with_timeout(
    cmd: List[str],
    *,
    timeout_sec: int,
    cwd: Path | None = None,
) -> tuple[subprocess.CompletedProcess[str], bool]:
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            check=False,
            text=True,
            capture_output=True,
            timeout=max(1, int(timeout_sec)),
        )
        return result, False
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        stderr = (stderr + "\ncommand_timeout").strip()
        return subprocess.CompletedProcess(cmd, 124, stdout, stderr), True


def proxy_command() -> str | None:
    if not JUMP_HOST:
        return None
    target = f"{JUMP_USER}@{JUMP_HOST}"
    return (
        f"sshpass -p {shlex.quote(JUMP_PASSWORD)} "
        f"ssh -o StrictHostKeyChecking=no "
        f"-o PreferredAuthentications=password -o PubkeyAuthentication=no -o IdentitiesOnly=yes "
        f"-p {shlex.quote(JUMP_PORT)} "
        f"{shlex.quote(target)} -W %h:%p"
    )


def ssh_cmd(command: str) -> List[str]:
    cmd = [
        "sshpass",
        "-p",
        PASSWORD,
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "PreferredAuthentications=password",
        "-o",
        "PubkeyAuthentication=no",
        "-o",
        "IdentitiesOnly=yes",
    ]
    proxy = proxy_command()
    if proxy:
        cmd.extend(["-o", f"ProxyCommand={proxy}"])
    cmd.extend(["-p", PORT, REMOTE, command])
    return cmd


def scp_to_remote(local_path: Path, remote_path: str) -> None:
    cmd = [
        "sshpass",
        "-p",
        PASSWORD,
        "scp",
        "-P",
        PORT,
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "PreferredAuthentications=password",
        "-o",
        "PubkeyAuthentication=no",
        "-o",
        "IdentitiesOnly=yes",
    ]
    proxy = proxy_command()
    if proxy:
        cmd.extend(["-o", f"ProxyCommand={proxy}"])
    cmd.extend([str(local_path), f"{REMOTE}:{remote_path}"])
    run(
        cmd
    )


def scp_from_remote(remote_path: str, local_parent: Path) -> None:
    ensure_dir(local_parent)
    cmd = [
        "sshpass",
        "-p",
        PASSWORD,
        "scp",
        "-r",
        "-P",
        PORT,
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "PreferredAuthentications=password",
        "-o",
        "PubkeyAuthentication=no",
        "-o",
        "IdentitiesOnly=yes",
    ]
    proxy = proxy_command()
    if proxy:
        cmd.extend(["-o", f"ProxyCommand={proxy}"])
    cmd.extend([f"{REMOTE}:{remote_path}", str(local_parent)])
    last_error: subprocess.CalledProcessError | None = None
    for attempt in range(1, 4):
        try:
            run(cmd)
            return
        except subprocess.CalledProcessError as exc:
            last_error = exc
            if attempt == 3:
                break
            time.sleep(float(attempt))
    if last_error is not None:
        raise last_error


def remote_path_exists(remote_path: str) -> bool:
    result = run_capture(
        ssh_cmd(f"test -e {shlex.quote(remote_path)}"),
        check=False,
    )
    return result.returncode == 0


def remote_env_prefix() -> str:
    return f"SCALEBENCH_SUDO_PW={shlex.quote(REMOTE_SUDO_PASSWORD)}"


def sync_remote_run_artifacts(remote_run_root: str, local_run_root: Path) -> None:
    ensure_dir(local_run_root)
    for subdir in ("configs", "logs", "metrics", "results", "summaries"):
        remote_path = f"{remote_run_root}/{subdir}"
        if not remote_path_exists(remote_path):
            continue
        local_subdir = local_run_root / subdir
        if local_subdir.exists():
            shutil.rmtree(local_subdir)
        scp_from_remote(remote_path, local_run_root)


def upload_scripts(repo: Path, remote_tools_dir: str) -> None:
    assets = [
        repo / "scripts" / "capacity_bootstrap_remote_cluster.py",
        repo / "scripts" / "discovery_workload_lib.py",
        repo / "scripts" / "discovery_workload_remote_bench.py",
    ]
    run(ssh_cmd(f"mkdir -p {shlex.quote(remote_tools_dir)}"))
    for asset in assets:
        scp_to_remote(asset, f"{remote_tools_dir}/{asset.name}")
    chmod_targets = " ".join(
        shlex.quote(f"{remote_tools_dir}/{name}")
        for name in ("capacity_bootstrap_remote_cluster.py", "discovery_workload_remote_bench.py")
    )
    run(ssh_cmd(f"chmod +x {chmod_targets}"))


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    if not rows:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([])
        return
    keys = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def read_yaml(path: Path) -> Dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}
    return payload


def get_nested(payload: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    cursor: Any = payload
    for key in keys:
        if not isinstance(cursor, dict):
            return default
        if key not in cursor:
            return default
        cursor = cursor[key]
    return cursor


def apply_yaml_defaults(args: argparse.Namespace, yaml_payload: Dict[str, Any]) -> None:
    mapping = {
        "profile_name": ("experiment", "name"),
        "variant": ("experiment", "variant"),
        "matching_mode": ("experiment", "matching_mode"),
        "node_count": ("experiment", "node_count"),
        "requests": ("experiment", "requests"),
        "payload_bytes": ("experiment", "payload_bytes"),
        "repetitions": ("experiment", "repetitions"),
        "random_seed": ("experiment", "random_seed"),
        "global_topic": ("experiment", "global_topic"),
        "capabilities_per_node": ("experiment", "capabilities_per_node"),
        "capability_tags": ("experiment", "capability_tags"),
        "families": ("experiment", "families"),
        "domains": ("experiment", "domains"),
        "tools": ("experiment", "tools"),
        "trust_domains": ("experiment", "trust_domains"),
        "latency_tiers": ("experiment", "latency_tiers"),
        "specialization_tags": ("experiment", "specialization_tags"),
        "deadline_classes": ("experiment", "deadline_classes"),
        "families_per_node": ("experiment", "families_per_node"),
        "domains_min": ("experiment", "domains_min"),
        "domains_max": ("experiment", "domains_max"),
        "tools_min": ("experiment", "tools_min"),
        "tools_max": ("experiment", "tools_max"),
        "specialization_max": ("experiment", "specialization_max"),
        "required_tools_min": ("experiment", "required_tools_min"),
        "required_tools_max": ("experiment", "required_tools_max"),
        "specialization_preference_prob": ("experiment", "specialization_preference_prob"),
        "specialization_hard": ("experiment", "specialization_hard"),
        "constraint_mode": ("experiment", "constraint_mode"),
        "exclude_requester_from_match_count": ("experiment", "exclude_requester_from_match_count"),
        "pull_policy": ("experiment", "pull_policy"),
        "pull_probability": ("experiment", "pull_probability"),
        "pull_topk": ("experiment", "pull_topk"),
        "declaration_observe_sec": ("experiment", "declaration_observe_sec"),
        "response_observe_sec": ("experiment", "response_observe_sec"),
        "poll_sec": ("experiment", "poll_sec"),
        "request_gap_sec": ("experiment", "request_gap_sec"),
        "bootstrap_cmd_timeout_sec": ("experiment", "bootstrap_cmd_timeout_sec"),
        "runtime_cmd_timeout_sec": ("experiment", "runtime_cmd_timeout_sec"),
        "remote": ("remote", "host"),
        "port": ("remote", "ssh_port"),
        "password": ("remote", "password"),
        "remote_sudo_password": ("remote", "sudo_password"),
        "remote_tools_dir": ("remote", "remote_tools_dir"),
        "jump_host": ("remote", "jump_host"),
        "jump_port": ("remote", "jump_port"),
        "jump_user": ("remote", "jump_user"),
        "jump_password": ("remote", "jump_password"),
        "bootstrap_timeout_sec": ("bootstrap", "bootstrap_timeout_sec"),
        "warmup_sec": ("bootstrap", "warmup_sec"),
        "stabilization_min_sec": ("bootstrap", "stabilization_min_sec"),
        "stabilization_timeout_sec": ("bootstrap", "stabilization_timeout_sec"),
        "stabilization_sample_sec": ("bootstrap", "stabilization_sample_sec"),
        "seed_count": ("docker", "seed_count"),
        "batch_size_small": ("docker", "batch_size_small"),
        "batch_size_medium": ("docker", "batch_size_medium"),
        "batch_pause_sec": ("docker", "batch_pause_sec"),
        "neighbor_target": ("docker", "neighbor_target"),
        "neighbor_hard_cap": ("docker", "neighbor_hard_cap"),
        "same_topic_neighbor_min": ("docker", "same_topic_neighbor_min"),
        "same_topic_mesh_min": ("docker", "same_topic_mesh_min"),
        "neighbor_low_watermark": ("docker", "neighbor_low_watermark"),
        "bootstrap_connect_count": ("docker", "bootstrap_connect_count"),
        "bootstrap_candidate_count": ("docker", "bootstrap_candidate_count"),
        "bootstrap_refresh_sec": ("docker", "bootstrap_refresh_sec"),
        "topic_replenish_batch": ("docker", "topic_replenish_batch"),
        "topic_replenish_cooldown_sec": ("docker", "topic_replenish_cooldown_sec"),
        "api_base": ("docker", "api_base"),
        "p2p_base": ("docker", "p2p_base"),
        "image": ("docker", "image"),
        "rendezvous": ("docker", "rendezvous"),
    }
    for attr, key_chain in mapping.items():
        current_value = getattr(args, attr, None)
        configured_value = get_nested(yaml_payload, *key_chain, default=None)
        if configured_value is None:
            continue
        if attr in {
            "capability_tags",
            "families",
            "domains",
            "tools",
            "trust_domains",
            "latency_tiers",
            "specialization_tags",
            "deadline_classes",
        } and isinstance(configured_value, list):
            configured_value = ",".join(str(token).strip() for token in configured_value if str(token).strip())
        if current_value == parser_defaults().get(attr):
            setattr(args, attr, configured_value)


def parser_defaults() -> Dict[str, Any]:
    return {
        "profile_name": DEFAULT_PROFILE_NAME,
        "variant": DEFAULT_VARIANT,
        "matching_mode": "deterministic_two_stage",
        "node_count": 100,
        "requests": 200,
        "payload_bytes": 32768,
        "repetitions": 1,
        "random_seed": 20260402,
        "global_topic": "openagent/v1/general",
        "capabilities_per_node": 2,
        "capability_tags": ",".join(DEFAULT_CAPABILITY_TAGS),
        "families": ",".join(DEFAULT_FAMILIES),
        "domains": ",".join(DEFAULT_DOMAINS),
        "tools": ",".join(DEFAULT_TOOLS),
        "trust_domains": ",".join(DEFAULT_TRUST_DOMAINS),
        "latency_tiers": ",".join(DEFAULT_LATENCY_TIERS),
        "specialization_tags": ",".join(DEFAULT_SPECIALIZATION_TAGS),
        "deadline_classes": ",".join(DEFAULT_DEADLINE_CLASSES),
        "families_per_node": 2,
        "domains_min": 1,
        "domains_max": 2,
        "tools_min": 1,
        "tools_max": 3,
        "specialization_max": 2,
        "required_tools_min": 1,
        "required_tools_max": 2,
        "specialization_preference_prob": 0.55,
        "specialization_hard": 1,
        "constraint_mode": "medium",
        "exclude_requester_from_match_count": 1,
        "pull_policy": "deterministic_coarse_match",
        "pull_probability": 0.30,
        "pull_topk": 5,
        "declaration_observe_sec": 30.0,
        "response_observe_sec": 20.0,
        "poll_sec": 0.5,
        "request_gap_sec": 0.2,
        "bootstrap_cmd_timeout_sec": 5400,
        "runtime_cmd_timeout_sec": 7200,
        "bootstrap_timeout_sec": 1200,
        "warmup_sec": 20,
        "stabilization_min_sec": 60,
        "stabilization_timeout_sec": 300,
        "stabilization_sample_sec": 5.0,
        "seed_count": 4,
        "batch_size_small": 10,
        "batch_size_medium": 15,
        "batch_pause_sec": 15,
        "neighbor_target": 6,
        "neighbor_hard_cap": 8,
        "same_topic_neighbor_min": 3,
        "same_topic_mesh_min": 4,
        "neighbor_low_watermark": 4,
        "bootstrap_connect_count": 2,
        "bootstrap_candidate_count": 8,
        "bootstrap_refresh_sec": 30,
        "topic_replenish_batch": 1,
        "topic_replenish_cooldown_sec": 20,
        "api_base": 8500,
        "p2p_base": 5500,
        "image": "openagent-node:formal",
        "rendezvous": "openagent/v1/dev",
        "remote": DEFAULT_REMOTE,
        "port": DEFAULT_PORT,
        "password": DEFAULT_PASSWORD,
        "remote_sudo_password": DEFAULT_REMOTE_SUDO_PASSWORD,
        "remote_tools_dir": DEFAULT_REMOTE_TOOLS_DIR,
        "jump_host": DEFAULT_JUMP_HOST,
        "jump_port": DEFAULT_JUMP_PORT,
        "jump_user": DEFAULT_JUMP_USER,
        "jump_password": DEFAULT_JUMP_PASSWORD,
    }


def load_run_summary(run_root: Path) -> Dict[str, Any]:
    summary_path = run_root / "summaries" / "summary.json"
    if not summary_path.exists():
        return {}
    return json.loads(summary_path.read_text(encoding="utf-8"))


def main() -> None:
    defaults = parser_defaults()
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--config", default="")
    parser.add_argument("--output-root", default="")
    parser.add_argument("--remote-base", default="")
    parser.add_argument("--profile-name", default=defaults["profile_name"])
    parser.add_argument("--variant", default=defaults["variant"])
    parser.add_argument(
        "--matching-mode",
        choices=["legacy_simple", "deterministic_two_stage"],
        default=defaults["matching_mode"],
    )
    parser.add_argument("--node-count", type=int, default=defaults["node_count"])
    parser.add_argument("--requests", type=int, default=defaults["requests"])
    parser.add_argument("--payload-bytes", type=int, default=defaults["payload_bytes"])
    parser.add_argument("--repetitions", type=int, default=defaults["repetitions"])
    parser.add_argument("--random-seed", type=int, default=defaults["random_seed"])
    parser.add_argument("--global-topic", default=defaults["global_topic"])
    parser.add_argument("--capabilities-per-node", type=int, default=defaults["capabilities_per_node"])
    parser.add_argument("--capability-tags", default=defaults["capability_tags"])
    parser.add_argument("--families", default=defaults["families"])
    parser.add_argument("--domains", default=defaults["domains"])
    parser.add_argument("--tools", default=defaults["tools"])
    parser.add_argument("--trust-domains", default=defaults["trust_domains"])
    parser.add_argument("--latency-tiers", default=defaults["latency_tiers"])
    parser.add_argument("--specialization-tags", default=defaults["specialization_tags"])
    parser.add_argument("--deadline-classes", default=defaults["deadline_classes"])
    parser.add_argument("--families-per-node", type=int, default=defaults["families_per_node"])
    parser.add_argument("--domains-min", type=int, default=defaults["domains_min"])
    parser.add_argument("--domains-max", type=int, default=defaults["domains_max"])
    parser.add_argument("--tools-min", type=int, default=defaults["tools_min"])
    parser.add_argument("--tools-max", type=int, default=defaults["tools_max"])
    parser.add_argument("--specialization-max", type=int, default=defaults["specialization_max"])
    parser.add_argument("--required-tools-min", type=int, default=defaults["required_tools_min"])
    parser.add_argument("--required-tools-max", type=int, default=defaults["required_tools_max"])
    parser.add_argument(
        "--specialization-preference-prob",
        type=float,
        default=defaults["specialization_preference_prob"],
    )
    parser.add_argument("--specialization-hard", type=int, choices=[0, 1], default=defaults["specialization_hard"])
    parser.add_argument("--constraint-mode", choices=["loose", "medium", "tight"], default=defaults["constraint_mode"])
    parser.add_argument(
        "--exclude-requester-from-match-count",
        type=int,
        choices=[0, 1],
        default=defaults["exclude_requester_from_match_count"],
    )
    parser.add_argument(
        "--pull-policy",
        choices=["all", "probabilistic", "topk", "deterministic_coarse_match"],
        default=defaults["pull_policy"],
    )
    parser.add_argument("--pull-probability", type=float, default=defaults["pull_probability"])
    parser.add_argument("--pull-topk", type=int, default=defaults["pull_topk"])
    parser.add_argument("--declaration-observe-sec", type=float, default=defaults["declaration_observe_sec"])
    parser.add_argument("--response-observe-sec", type=float, default=defaults["response_observe_sec"])
    parser.add_argument("--poll-sec", type=float, default=defaults["poll_sec"])
    parser.add_argument("--request-gap-sec", type=float, default=defaults["request_gap_sec"])
    parser.add_argument("--bootstrap-cmd-timeout-sec", type=int, default=defaults["bootstrap_cmd_timeout_sec"])
    parser.add_argument("--runtime-cmd-timeout-sec", type=int, default=defaults["runtime_cmd_timeout_sec"])
    parser.add_argument("--bootstrap-timeout-sec", type=int, default=defaults["bootstrap_timeout_sec"])
    parser.add_argument("--warmup-sec", type=int, default=defaults["warmup_sec"])
    parser.add_argument("--stabilization-min-sec", type=int, default=defaults["stabilization_min_sec"])
    parser.add_argument("--stabilization-timeout-sec", type=int, default=defaults["stabilization_timeout_sec"])
    parser.add_argument("--stabilization-sample-sec", type=float, default=defaults["stabilization_sample_sec"])
    parser.add_argument("--seed-count", type=int, default=defaults["seed_count"])
    parser.add_argument("--batch-size-small", type=int, default=defaults["batch_size_small"])
    parser.add_argument("--batch-size-medium", type=int, default=defaults["batch_size_medium"])
    parser.add_argument("--batch-pause-sec", type=int, default=defaults["batch_pause_sec"])
    parser.add_argument("--neighbor-target", type=int, default=defaults["neighbor_target"])
    parser.add_argument("--neighbor-hard-cap", type=int, default=defaults["neighbor_hard_cap"])
    parser.add_argument("--same-topic-neighbor-min", type=int, default=defaults["same_topic_neighbor_min"])
    parser.add_argument("--same-topic-mesh-min", type=int, default=defaults["same_topic_mesh_min"])
    parser.add_argument("--neighbor-low-watermark", type=int, default=defaults["neighbor_low_watermark"])
    parser.add_argument("--bootstrap-connect-count", type=int, default=defaults["bootstrap_connect_count"])
    parser.add_argument("--bootstrap-candidate-count", type=int, default=defaults["bootstrap_candidate_count"])
    parser.add_argument("--bootstrap-refresh-sec", type=int, default=defaults["bootstrap_refresh_sec"])
    parser.add_argument("--topic-replenish-batch", type=int, default=defaults["topic_replenish_batch"])
    parser.add_argument("--topic-replenish-cooldown-sec", type=int, default=defaults["topic_replenish_cooldown_sec"])
    parser.add_argument("--api-base", type=int, default=defaults["api_base"])
    parser.add_argument("--p2p-base", type=int, default=defaults["p2p_base"])
    parser.add_argument("--image", default=defaults["image"])
    parser.add_argument("--rendezvous", default=defaults["rendezvous"])
    parser.add_argument("--remote", default=defaults["remote"])
    parser.add_argument("--port", default=defaults["port"])
    parser.add_argument("--password", default=defaults["password"])
    parser.add_argument("--remote-sudo-password", default=defaults["remote_sudo_password"])
    parser.add_argument("--remote-tools-dir", default=defaults["remote_tools_dir"])
    parser.add_argument("--jump-host", default=defaults["jump_host"])
    parser.add_argument("--jump-port", default=defaults["jump_port"])
    parser.add_argument("--jump-user", default=defaults["jump_user"])
    parser.add_argument("--jump-password", default=defaults["jump_password"])
    args = parser.parse_args()

    if args.config:
        yaml_payload = read_yaml(Path(args.config).expanduser())
        apply_yaml_defaults(args, yaml_payload)

    if args.matching_mode == "deterministic_two_stage" and args.pull_policy != "deterministic_coarse_match":
        raise SystemExit("deterministic_two_stage requires --pull-policy deterministic_coarse_match")

    global REMOTE, PORT, PASSWORD, REMOTE_SUDO_PASSWORD, JUMP_HOST, JUMP_PORT, JUMP_USER, JUMP_PASSWORD
    REMOTE = str(args.remote)
    PORT = str(args.port)
    PASSWORD = str(args.password)
    REMOTE_SUDO_PASSWORD = str(args.remote_sudo_password)
    JUMP_HOST = str(args.jump_host or "").strip()
    JUMP_PORT = str(args.jump_port or "").strip() or DEFAULT_JUMP_PORT
    JUMP_USER = str(args.jump_user or "").strip()
    JUMP_PASSWORD = str(args.jump_password or "")
    if JUMP_HOST and (not JUMP_USER or not JUMP_PASSWORD):
        raise SystemExit("jump-host requires both --jump-user and --jump-password")

    repo = Path(args.repo).resolve()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_root = (
        Path(args.output_root).resolve()
        if args.output_root
        else repo / "results" / f"{args.profile_name}_{timestamp}"
    )
    ensure_dir(output_root)
    ensure_dir(output_root / "configs")
    ensure_dir(output_root / "logs")
    ensure_dir(output_root / "metrics")
    ensure_dir(output_root / "results")
    ensure_dir(output_root / "summaries")

    remote_base = args.remote_base or f"/home/ma/{args.profile_name}-{timestamp}"
    upload_scripts(repo, str(args.remote_tools_dir))
    run(ssh_cmd(f"mkdir -p {shlex.quote(remote_base)}"))

    config_snapshot = {
        "generated_at": now_iso(),
        "profile_name": args.profile_name,
        "variant": args.variant,
        "matching_mode": args.matching_mode,
        "node_count": args.node_count,
        "requests": args.requests,
        "payload_bytes": args.payload_bytes,
        "repetitions": args.repetitions,
        "random_seed": args.random_seed,
        "global_topic": args.global_topic,
        "capabilities_per_node": args.capabilities_per_node,
        "capability_tags": [token.strip() for token in str(args.capability_tags).split(",") if token.strip()],
        "families": [token.strip() for token in str(args.families).split(",") if token.strip()],
        "domains": [token.strip() for token in str(args.domains).split(",") if token.strip()],
        "tools": [token.strip() for token in str(args.tools).split(",") if token.strip()],
        "trust_domains": [token.strip() for token in str(args.trust_domains).split(",") if token.strip()],
        "latency_tiers": [token.strip() for token in str(args.latency_tiers).split(",") if token.strip()],
        "specialization_tags": [token.strip() for token in str(args.specialization_tags).split(",") if token.strip()],
        "deadline_classes": [token.strip() for token in str(args.deadline_classes).split(",") if token.strip()],
        "families_per_node": args.families_per_node,
        "domains_min": args.domains_min,
        "domains_max": args.domains_max,
        "tools_min": args.tools_min,
        "tools_max": args.tools_max,
        "specialization_max": args.specialization_max,
        "required_tools_min": args.required_tools_min,
        "required_tools_max": args.required_tools_max,
        "specialization_preference_prob": args.specialization_preference_prob,
        "specialization_hard": bool(args.specialization_hard),
        "constraint_mode": args.constraint_mode,
        "exclude_requester_from_match_count": bool(args.exclude_requester_from_match_count),
        "pull_policy": args.pull_policy,
        "pull_probability": args.pull_probability,
        "pull_topk": args.pull_topk,
        "declaration_observe_sec": args.declaration_observe_sec,
        "response_observe_sec": args.response_observe_sec,
        "poll_sec": args.poll_sec,
        "request_gap_sec": args.request_gap_sec,
        "bootstrap_cmd_timeout_sec": args.bootstrap_cmd_timeout_sec,
        "runtime_cmd_timeout_sec": args.runtime_cmd_timeout_sec,
        "bootstrap_timeout_sec": args.bootstrap_timeout_sec,
        "warmup_sec": args.warmup_sec,
        "stabilization_min_sec": args.stabilization_min_sec,
        "stabilization_timeout_sec": args.stabilization_timeout_sec,
        "stabilization_sample_sec": args.stabilization_sample_sec,
        "seed_count": args.seed_count,
        "batch_size_small": args.batch_size_small,
        "batch_size_medium": args.batch_size_medium,
        "batch_pause_sec": args.batch_pause_sec,
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
        "api_base": args.api_base,
        "p2p_base": args.p2p_base,
        "image": args.image,
        "rendezvous": args.rendezvous,
        "remote": {
            "host": REMOTE,
            "port": PORT,
            "remote_base": remote_base,
            "remote_tools_dir": args.remote_tools_dir,
            "jump": {
                "enabled": bool(JUMP_HOST),
                "jump_host": JUMP_HOST,
                "jump_port": JUMP_PORT,
                "jump_user": JUMP_USER,
            },
        },
    }
    dump_json(output_root / "configs" / "experiment_config.json", config_snapshot)

    runs: List[Dict[str, Any]] = []
    for repetition in range(1, int(args.repetitions) + 1):
        run_name = f"run{repetition:02d}"
        remote_run_root = f"{remote_base}/{run_name}"
        local_run_root = output_root / "results" / run_name
        ensure_dir(local_run_root)

        bootstrap_cmd = (
            f"{remote_env_prefix()} python3 {shlex.quote(str(args.remote_tools_dir))}/capacity_bootstrap_remote_cluster.py "
            f"--root {shlex.quote(remote_run_root)} "
            f"--variant {shlex.quote(args.variant)} "
            f"--node-count {int(args.node_count)} "
            f"--bootstrap-timeout-sec {int(args.bootstrap_timeout_sec)} "
            f"--warmup-sec {int(args.warmup_sec)} "
            f"--stabilization-min-sec {int(args.stabilization_min_sec)} "
            f"--stabilization-timeout-sec {int(args.stabilization_timeout_sec)} "
            f"--stabilization-sample-sec {float(args.stabilization_sample_sec)} "
            f"--api-base {int(args.api_base)} "
            f"--p2p-base {int(args.p2p_base)} "
            f"--seed-count {int(args.seed_count)} "
            f"--batch-size-small {int(args.batch_size_small)} "
            f"--batch-size-medium {int(args.batch_size_medium)} "
            f"--batch-pause-sec {int(args.batch_pause_sec)} "
            f"--global-topic {shlex.quote(args.global_topic)} "
            f"--rendezvous {shlex.quote(args.rendezvous)} "
            f"--neighbor-target {int(args.neighbor_target)} "
            f"--neighbor-hard-cap {int(args.neighbor_hard_cap)} "
            f"--same-topic-neighbor-min {int(args.same_topic_neighbor_min)} "
            f"--same-topic-mesh-min {int(args.same_topic_mesh_min)} "
            f"--neighbor-low-watermark {int(args.neighbor_low_watermark)} "
            f"--bootstrap-connect-count {int(args.bootstrap_connect_count)} "
            f"--bootstrap-candidate-count {int(args.bootstrap_candidate_count)} "
            f"--bootstrap-refresh-sec {int(args.bootstrap_refresh_sec)} "
            f"--topic-replenish-batch {int(args.topic_replenish_batch)} "
            f"--topic-replenish-cooldown-sec {int(args.topic_replenish_cooldown_sec)} "
            f"--image {shlex.quote(args.image)}"
        )
        bootstrap_result, bootstrap_timed_out = run_capture_with_timeout(
            ssh_cmd(bootstrap_cmd),
            timeout_sec=int(args.bootstrap_cmd_timeout_sec),
        )

        runtime_cmd = (
            f"{remote_env_prefix()} python3 {shlex.quote(str(args.remote_tools_dir))}/discovery_workload_remote_bench.py "
            f"--root {shlex.quote(remote_run_root)} "
            f"--label {shlex.quote(args.profile_name)} "
            f"--variant {shlex.quote(args.variant)} "
            f"--matching-mode {shlex.quote(args.matching_mode)} "
            f"--node-count {int(args.node_count)} "
            f"--requests {int(args.requests)} "
            f"--payload-bytes {int(args.payload_bytes)} "
            f"--repetitions 1 "
            f"--random-seed {int(args.random_seed) + repetition - 1} "
            f"--global-topic {shlex.quote(args.global_topic)} "
            f"--api-base {int(args.api_base)} "
            f"--declaration-observe-sec {float(args.declaration_observe_sec)} "
            f"--response-observe-sec {float(args.response_observe_sec)} "
            f"--poll-sec {float(args.poll_sec)} "
            f"--request-gap-sec {float(args.request_gap_sec)} "
            f"--capability-tags {shlex.quote(args.capability_tags)} "
            f"--capabilities-per-node {int(args.capabilities_per_node)} "
            f"--families {shlex.quote(args.families)} "
            f"--domains {shlex.quote(args.domains)} "
            f"--tools {shlex.quote(args.tools)} "
            f"--trust-domains {shlex.quote(args.trust_domains)} "
            f"--latency-tiers {shlex.quote(args.latency_tiers)} "
            f"--specialization-tags {shlex.quote(args.specialization_tags)} "
            f"--deadline-classes {shlex.quote(args.deadline_classes)} "
            f"--families-per-node {int(args.families_per_node)} "
            f"--domains-min {int(args.domains_min)} "
            f"--domains-max {int(args.domains_max)} "
            f"--tools-min {int(args.tools_min)} "
            f"--tools-max {int(args.tools_max)} "
            f"--specialization-max {int(args.specialization_max)} "
            f"--required-tools-min {int(args.required_tools_min)} "
            f"--required-tools-max {int(args.required_tools_max)} "
            f"--specialization-preference-prob {float(args.specialization_preference_prob)} "
            f"--specialization-hard {int(args.specialization_hard)} "
            f"--constraint-mode {shlex.quote(args.constraint_mode)} "
            f"--exclude-requester-from-match-count {int(args.exclude_requester_from_match_count)} "
            f"--pull-policy {shlex.quote(args.pull_policy)} "
            f"--pull-probability {float(args.pull_probability)} "
            f"--pull-topk {int(args.pull_topk)}"
        )
        runtime_result, runtime_timed_out = run_capture_with_timeout(
            ssh_cmd(runtime_cmd),
            timeout_sec=int(args.runtime_cmd_timeout_sec),
        )

        sync_remote_run_artifacts(remote_run_root, local_run_root)
        (output_root / "logs" / f"{run_name}_bootstrap_stdout.log").write_text(
            bootstrap_result.stdout,
            encoding="utf-8",
        )
        (output_root / "logs" / f"{run_name}_bootstrap_stderr.log").write_text(
            bootstrap_result.stderr,
            encoding="utf-8",
        )
        (output_root / "logs" / f"{run_name}_runtime_stdout.log").write_text(
            runtime_result.stdout,
            encoding="utf-8",
        )
        (output_root / "logs" / f"{run_name}_runtime_stderr.log").write_text(
            runtime_result.stderr,
            encoding="utf-8",
        )

        run(
            [
                "python3",
                str(repo / "scripts" / "discovery_workload_aggregate.py"),
                "--run-root",
                str(local_run_root),
            ]
        )

        runtime_summary_path = local_run_root / "summaries" / "runtime_summary.json"
        runtime_summary = (
            json.loads(runtime_summary_path.read_text(encoding="utf-8"))
            if runtime_summary_path.exists()
            else {}
        )
        aggregate_summary = load_run_summary(local_run_root)
        metrics = aggregate_summary.get("metrics", {})
        run_row = {
            "run_id": run_name,
            "nodes": args.node_count,
            "matching_mode": args.matching_mode,
            "constraint_mode": args.constraint_mode,
            "bootstrap_pass": bool(runtime_summary.get("bootstrap_pass")),
            "bench_pass": bool(runtime_summary.get("bench_ok")),
            "request_count": aggregate_summary.get("request_count", 0),
            "visible_mean": (metrics.get("visible_count") or {}).get("avg"),
            "coarse_mean": (metrics.get("coarse_match_count") or {}).get("avg"),
            "full_mean": (metrics.get("full_match_count") or {}).get("avg"),
            "actual_pull_mean": (metrics.get("actual_pull_count") or {}).get("avg"),
            "actual_response_mean": (metrics.get("actual_response_count") or {}).get("avg"),
            "eligible_mean": (metrics.get("eligible_count") or {}).get("avg"),
            "pulling_mean": (metrics.get("pulling_count") or {}).get("avg"),
            "responding_mean": (metrics.get("responding_count") or {}).get("avg"),
            "p_coarse_mean": (metrics.get("p_coarse") or {}).get("avg"),
            "p_full_mean": (metrics.get("p_full") or {}).get("avg"),
            "p_pull_mean": (metrics.get("p_pull") or {}).get("avg"),
            "p_resp_mean": (metrics.get("p_resp") or {}).get("avg"),
            "coarse_to_full_mean": (metrics.get("coarse_to_full") or {}).get("avg"),
            "pull_efficiency_mean": (metrics.get("pull_efficiency") or {}).get("avg"),
            "full_match_available_rate": aggregate_summary.get("full_match_available_rate"),
            "zero_full_match_rate": aggregate_summary.get("zero_full_match_rate"),
            "zero_responder_rate": aggregate_summary.get("zero_responder_rate"),
            "first_response_available_rate": aggregate_summary.get("first_response_available_rate"),
            "local_run_root": str(local_run_root),
            "remote_run_root": remote_run_root,
            "bootstrap_rc": bootstrap_result.returncode,
            "runtime_rc": runtime_result.returncode,
            "bootstrap_cmd_timed_out": bootstrap_timed_out,
            "runtime_cmd_timed_out": runtime_timed_out,
        }
        runs.append(
            {
                "run_id": run_name,
                "remote_run_root": remote_run_root,
                "local_run_root": str(local_run_root),
                "bootstrap_rc": bootstrap_result.returncode,
                "runtime_rc": runtime_result.returncode,
                "bootstrap_cmd_timed_out": bootstrap_timed_out,
                "runtime_cmd_timed_out": runtime_timed_out,
                "runtime_summary": runtime_summary,
                "aggregate_summary": aggregate_summary,
                "run_row": run_row,
            }
        )

    write_csv(output_root / "results" / "workload_char_probe_summary.csv", [row["run_row"] for row in runs])
    manifest = {
        "generated_at": now_iso(),
        "profile_name": args.profile_name,
        "config": config_snapshot,
        "runs": runs,
        "results_csv": str(output_root / "results" / "workload_char_probe_summary.csv"),
    }
    dump_json(output_root / "summaries" / "workload_char_manifest.json", manifest)
    print(json.dumps({"output_root": str(output_root), "run_count": len(runs)}, indent=2))


if __name__ == "__main__":
    main()
