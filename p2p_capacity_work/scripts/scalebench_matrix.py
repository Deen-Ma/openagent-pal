#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROFILES: dict[str, dict[str, Any]] = {
    "P0": {"neighbor_target": 10, "neighbor_hard_cap": 16, "same_topic_neighbor_min": 6, "same_topic_mesh_min": 6, "topic_replenish_batch": 2, "bootstrap_connect_count": 4},
    "P1": {"neighbor_target": 8, "neighbor_hard_cap": 12, "same_topic_neighbor_min": 4, "same_topic_mesh_min": 4, "topic_replenish_batch": 2, "bootstrap_connect_count": 4},
    "P2": {"neighbor_target": 8, "neighbor_hard_cap": 14, "same_topic_neighbor_min": 4, "same_topic_mesh_min": 4, "topic_replenish_batch": 2, "bootstrap_connect_count": 4},
    "P3": {"neighbor_target": 9, "neighbor_hard_cap": 14, "same_topic_neighbor_min": 5, "same_topic_mesh_min": 5, "topic_replenish_batch": 2, "bootstrap_connect_count": 4},
    "P4": {"neighbor_target": 10, "neighbor_hard_cap": 14, "same_topic_neighbor_min": 6, "same_topic_mesh_min": 6, "topic_replenish_batch": 1, "bootstrap_connect_count": 4},
    "P5": {"neighbor_target": 10, "neighbor_hard_cap": 16, "same_topic_neighbor_min": 5, "same_topic_mesh_min": 6, "topic_replenish_batch": 1, "bootstrap_connect_count": 4},
    "P6": {"neighbor_target": 10, "neighbor_hard_cap": 18, "same_topic_neighbor_min": 6, "same_topic_mesh_min": 6, "topic_replenish_batch": 2, "bootstrap_connect_count": 4},
    "P7": {"neighbor_target": 12, "neighbor_hard_cap": 16, "same_topic_neighbor_min": 6, "same_topic_mesh_min": 6, "topic_replenish_batch": 2, "bootstrap_connect_count": 4},
    "P8": {"neighbor_target": 10, "neighbor_hard_cap": 16, "same_topic_neighbor_min": 6, "same_topic_mesh_min": 6, "topic_replenish_batch": 2, "bootstrap_connect_count": 6},
    "P9": {"neighbor_target": 10, "neighbor_hard_cap": 16, "same_topic_neighbor_min": 4, "same_topic_mesh_min": 6, "topic_replenish_batch": 2, "bootstrap_connect_count": 4},
}


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=False, text=True, capture_output=True)


def build_node_args(config: dict[str, Any]) -> list[str]:
    args = [
        "--neighbor-target", str(config["neighbor_target"]),
        "--neighbor-hard-cap", str(config["neighbor_hard_cap"]),
        "--same-topic-neighbor-min", str(config["same_topic_neighbor_min"]),
        "--same-topic-mesh-min", str(config["same_topic_mesh_min"]),
        "--topic-replenish-batch", str(config["topic_replenish_batch"]),
        "--bootstrap-connect-count", str(config["bootstrap_connect_count"]),
    ]
    if config.get("connect_on_discover"):
        args.append("--connect-on-discover")
    if config.get("periodic_find"):
        args.append("--periodic-find")
    return args


def profile_config(name: str) -> dict[str, Any]:
    key = name.upper()
    if key not in PROFILES:
        raise KeyError(f"unknown profile {name}")
    return dict(PROFILES[key])


def orchestrate_cmd(args: argparse.Namespace, profile_name: str, config: dict[str, Any], output_root: Path) -> list[str]:
    repo = Path(args.repo).resolve()
    profile_dir = output_root / profile_name.lower()
    remote_base = f"{args.remote_base.rstrip('/')}/{profile_name.lower()}"
    cmd = [
        "python3",
        str(repo / "scripts" / "scalebench_orchestrate.py"),
        "--repo", str(repo),
        "--output-root", str(profile_dir),
        "--remote-base", remote_base,
        "--remote", args.remote,
        "--port", str(args.port),
        "--password", args.password,
        "--remote-sudo-password", args.remote_sudo_password,
        "--variant-label", profile_name.lower(),
        "--variant", args.variant,
        "--coarse-sizes", str(args.size),
        "--coarse-rounds-per-shard", str(args.coarse_rounds_per_shard),
        "--fine-rounds-per-shard", str(args.fine_rounds_per_shard),
        "--coarse-observe-sec", str(args.coarse_observe_sec),
        "--fine-observe-sec", str(args.fine_observe_sec),
        "--poll-sec", str(args.poll_sec),
        "--cpu-sample-sec", str(args.cpu_sample_sec),
        "--fine-repetitions", str(args.fine_repetitions),
        "--retry-attempts", str(args.retry_attempts),
    ]
    if args.phys_iface:
        cmd.extend(["--phys-iface", args.phys_iface])
    for node_arg in build_node_args(config):
        cmd.append(f"--node-arg={node_arg}")
    return cmd


def compare_cmd(repo: Path, output_root: Path, profile_names: list[str], size: int) -> list[str]:
    script = repo / "scripts" / "scalebench_compare.py"
    inputs = [
        str(output_root / name.lower())
        for name in profile_names
        if (output_root / name.lower() / "capacity_manifest.json").exists()
    ]
    return [
        "python3",
        str(script),
        *inputs,
        "--size", str(size),
        "--json-out", str(output_root / "matrix_summary.json"),
        "--markdown-out", str(output_root / "matrix_summary.md"),
        "--csv-out", str(output_root / "matrix_summary.csv"),
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--output-root", default="")
    parser.add_argument("--remote-base", required=True)
    parser.add_argument("--remote", default="zhao@172.31.100.223")
    parser.add_argument("--port", default="22")
    parser.add_argument("--password", default="zhao123")
    parser.add_argument("--remote-sudo-password", default="zhao123")
    parser.add_argument("--variant", default="ablation_no_semantic_public_topic")
    parser.add_argument("--profiles", default="P0,P1,P2,P3,P4,P5,P6,P7,P8,P9")
    parser.add_argument("--size", type=int, default=40)
    parser.add_argument("--coarse-rounds-per-shard", type=int, default=1)
    parser.add_argument("--fine-rounds-per-shard", type=int, default=1)
    parser.add_argument("--coarse-observe-sec", type=int, default=35)
    parser.add_argument("--fine-observe-sec", type=int, default=35)
    parser.add_argument("--poll-sec", type=float, default=0.2)
    parser.add_argument("--cpu-sample-sec", type=float, default=1.0)
    parser.add_argument("--fine-repetitions", type=int, default=1)
    parser.add_argument("--retry-attempts", type=int, default=1)
    parser.add_argument("--phys-iface", default="")
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_root).resolve() if args.output_root else repo / "results" / f"matrix_{args.size}_{stamp}"
    output_root.mkdir(parents=True, exist_ok=True)

    profile_names = [item.strip().upper() for item in args.profiles.split(",") if item.strip()]
    run_records = []
    for profile_name in profile_names:
        config = profile_config(profile_name)
        cmd = orchestrate_cmd(args, profile_name, config, output_root)
        result = run(cmd)
        profile_root = output_root / profile_name.lower()
        profile_root.mkdir(parents=True, exist_ok=True)
        (profile_root / "matrix_stdout.log").write_text(result.stdout)
        (profile_root / "matrix_stderr.log").write_text(result.stderr)
        run_records.append({
            "profile": profile_name,
            "config": config,
            "command": cmd,
            "manifest_path": str(output_root / profile_name.lower() / "capacity_manifest.json"),
            "returncode": result.returncode,
        })
        if result.returncode != 0:
            continue

    (output_root / "matrix_runs.json").write_text(json.dumps(run_records, indent=2))
    compare = compare_cmd(repo, output_root, profile_names, args.size)
    if len(compare) <= 2:
        raise SystemExit("no completed profile manifests to compare")
    compare_result = run(compare)
    if compare_result.returncode != 0:
        raise SystemExit(compare_result.returncode)
    (output_root / "compare_stdout.log").write_text(compare_result.stdout)
    (output_root / "compare_stderr.log").write_text(compare_result.stderr)
    print((output_root / "matrix_summary.md").read_text(), end="")
    print(f"\nartifacts: {shlex.quote(str(output_root))}")


if __name__ == "__main__":
    main()
