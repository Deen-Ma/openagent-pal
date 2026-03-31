#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scalebench_capacity_lib import (
    PUBLIC_WORST_CASE_VARIANT,
    build_reference,
    evaluate_repetition_group,
    evaluate_single_run,
    select_fine_sizes,
)


DEFAULT_REMOTE = "ma@172.31.100.142"
DEFAULT_PORT = "30002"
DEFAULT_PASSWORD = "ma123"
DEFAULT_REMOTE_SUDO_PASSWORD = "ma123"
REMOTE = DEFAULT_REMOTE
PORT = DEFAULT_PORT
PASSWORD = DEFAULT_PASSWORD
REMOTE_SUDO_PASSWORD = DEFAULT_REMOTE_SUDO_PASSWORD


def run(cmd: list[str], cwd: Path | None = None) -> None:
    subprocess.run(cmd, cwd=cwd, check=True)


def run_capture(cmd: list[str], cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(cmd, cwd=cwd, check=False, text=True, capture_output=True)
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            cmd,
            output=result.stdout,
            stderr=result.stderr,
        )
    return result


def capture(cmd: list[str], cwd: Path | None = None) -> str:
    return run_capture(cmd, cwd=cwd).stdout.strip()


def ssh_cmd(command: str) -> list[str]:
    return [
        "sshpass",
        "-p",
        PASSWORD,
        "ssh",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
        "-p",
        PORT,
        REMOTE,
        command,
    ]


def shell_join(parts: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in parts)


def remote_env_prefix() -> str:
    return f"SCALEBENCH_SUDO_PW={shlex.quote(REMOTE_SUDO_PASSWORD)}"


def scp_to_remote(local_path: Path, remote_path: str) -> None:
    run(
        [
            "sshpass",
            "-p",
            PASSWORD,
            "scp",
            "-P",
            PORT,
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=3",
            str(local_path),
            f"{REMOTE}:{remote_path}",
        ]
    )


def scp_from_remote(remote_path: str, local_path: Path) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    run(
        [
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
            "ConnectTimeout=10",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=3",
            f"{REMOTE}:{remote_path}",
            str(local_path),
        ]
    )


def parse_sizes(raw_value: str) -> list[int]:
    values = sorted({int(chunk.strip()) for chunk in raw_value.split(",") if chunk.strip()})
    if not values:
        raise ValueError("at least one size is required")
    return values


def fetch_remote_json(remote_path: str) -> dict[str, Any] | None:
    result = run_capture(
        ssh_cmd(f"test -f {shlex.quote(remote_path)} && cat {shlex.quote(remote_path)}"),
        check=False,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None
    return json.loads(result.stdout)


def fetch_remote_running_count() -> int:
    result = run_capture(
        ssh_cmd(
            f"{remote_env_prefix()} "
            "printf '%s\\n' \"$SCALEBENCH_SUDO_PW\" | sudo -S -p '' docker ps --format '{{.Names}}' | grep -c '^oa-formal-' || true"
        ),
        check=False,
    )
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        return 0
    try:
        return int(lines[-1])
    except ValueError:
        return 0


def upload_scripts(repo: Path) -> None:
    scripts = [
        repo / "scripts" / "scalebench_remote_cluster.sh",
        repo / "scripts" / "scalebench_remote_bench.py",
        repo / "scripts" / "scalebench_report.py",
        repo / "scripts" / "scalebench_capacity_lib.py",
    ]
    run(ssh_cmd("mkdir -p ~/scalebench_tools"))
    for script in scripts:
        scp_to_remote(script, f"~/scalebench_tools/{script.name}")
    run(
        ssh_cmd(
            f"{remote_env_prefix()} "
            "chmod +x "
            "~/scalebench_tools/scalebench_remote_cluster.sh "
            "~/scalebench_tools/scalebench_remote_bench.py "
            "~/scalebench_tools/scalebench_report.py"
        )
    )


def build_synthetic_summary(
    *,
    size: int,
    rounds_per_shard: int,
    observability: dict[str, Any] | None,
    bench_error: str,
) -> dict[str, Any]:
    expected_round_count = rounds_per_shard
    running_count = int((observability or {}).get("running_count", 0))
    healthz_ok_count = int((observability or {}).get("healthz_ok_count", 0))
    return {
        "variant": PUBLIC_WORST_CASE_VARIANT,
        "node_count": size,
        "topic_mode": "global_only",
        "global_topic": "openagent/v1/general",
        "seed_count": int((observability or {}).get("seed_count", 0)),
        "round_count": 0,
        "expected_round_count": expected_round_count,
        "launch_ok": running_count == size,
        "health_ok": healthz_ok_count == size,
        "bench_ok": False,
        "bootstrap_pass": False,
        "stabilization_pass": False,
        "running_count": running_count,
        "healthz_ok_count": healthz_ok_count,
        "bench_error": bench_error,
        "e2e_latency_ms": {"count": 0},
        "cover100_ms_global": {"count": 0},
        "seen_ratio_global": {"count": 0},
        "host_cpu_busy_pct": {"count": 0},
        "peer_count": {"count": 0},
        "general_topic_mesh_size": {"count": 0},
        "tcp_per_node_avg": None,
        "peer_per_node_p50": None,
        "mesh_per_node_p50": None,
        "connection_scaling_alert": False,
    }


def download_attempt_artifacts(
    *,
    remote_root: str,
    remote_bench_dir: str | None,
    local_attempt_root: Path,
) -> tuple[Path | None, Path]:
    local_attempt_root.mkdir(parents=True, exist_ok=True)
    observability_dest = local_attempt_root / "observability"
    scp_from_remote(f"{remote_root}/observability", observability_dest)
    local_bench_dir: Path | None = None
    if remote_bench_dir:
        local_bench_dir = local_attempt_root / Path(remote_bench_dir).name
        scp_from_remote(remote_bench_dir, local_bench_dir)
    return local_bench_dir, observability_dest


def run_single_attempt(
    *,
    stage_name: str,
    size: int,
    attempt: int,
    variant_label: str,
    variant: str,
    remote_base: str,
    local_results_root: Path,
    rounds_per_shard: int,
    observe_sec: int,
    stabilize_sec: int,
    poll_sec: float,
    cpu_sample_sec: float,
    phys_iface: str,
    node_args: list[str],
    payload_bytes: int,
    task_budget: int,
    latency_mode: str,
) -> dict[str, Any]:
    remote_root = f"{remote_base}/{stage_name}/size{size}_{variant_label}_attempt{attempt}"
    local_attempt_root = local_results_root / stage_name / f"size{size}" / f"attempt{attempt}"

    node_args_suffix = ""
    if node_args:
        node_args_suffix = " " + shell_join(node_args)
    cluster_result = run_capture(
        ssh_cmd(
            f"{remote_env_prefix()} "
            "~/scalebench_tools/scalebench_remote_cluster.sh "
            f"{shlex.quote(variant)} {shlex.quote(remote_root)} {size}{node_args_suffix}"
        ),
        check=False,
    )
    observability = fetch_remote_json(f"{remote_root}/observability/summary.json")
    running_count = int((observability or {}).get("running_count", fetch_remote_running_count()))
    healthz_ok_count = int((observability or {}).get("healthz_ok_count", 0))
    launch_ok = running_count == size
    health_ok = healthz_ok_count == size

    cluster_transport_warning = cluster_result.returncode != 0 and launch_ok and health_ok

    bench_result = None
    if launch_ok and health_ok:
        bench_cmd = (
            f"{remote_env_prefix()} "
            "~/scalebench_tools/scalebench_remote_bench.py "
            f"--root {shlex.quote(remote_root)} "
            f"--label {shlex.quote(variant_label)} "
            f"--variant {shlex.quote(variant)} "
            f"--node-count {size} "
            f"--rounds {rounds_per_shard} "
            f"--rounds-per-shard {rounds_per_shard} "
            f"--observe-sec {observe_sec} "
            f"--stabilize-sec {stabilize_sec} "
            f"--poll-sec {poll_sec} "
            f"--cpu-sample-sec {cpu_sample_sec} "
            f"--latency-mode {shlex.quote(latency_mode)}"
        )
        if phys_iface:
            bench_cmd += f" --phys-iface {shlex.quote(phys_iface)}"
        if payload_bytes > 0:
            bench_cmd += f" --payload-bytes {payload_bytes}"
        if task_budget > 0:
            bench_cmd += f" --task-budget {task_budget}"
        bench_result = run_capture(ssh_cmd(bench_cmd), check=False)

    remote_bench_dir = None
    cluster_stdout = cluster_result.stdout
    cluster_stderr = cluster_result.stderr
    cluster_rc = cluster_result.returncode
    bench_stdout = ""
    bench_stderr = ""
    bench_rc = None
    if bench_result is not None:
        bench_stdout = bench_result.stdout
        bench_stderr = bench_result.stderr
        bench_rc = bench_result.returncode
        ls_result = run_capture(
            ssh_cmd(f"ls -td {shlex.quote(remote_root)}/bench_shards_* 2>/dev/null | head -1"),
            check=False,
        )
        remote_bench_dir = ls_result.stdout.strip() or None

    local_bench_dir, local_observability_dir = download_attempt_artifacts(
        remote_root=remote_root,
        remote_bench_dir=remote_bench_dir,
        local_attempt_root=local_attempt_root,
    )

    summary_path = local_bench_dir / "summary.json" if local_bench_dir else None
    if summary_path and summary_path.exists():
        summary = json.loads(summary_path.read_text())
    else:
        bench_error = (
            bench_stderr.strip()
            or bench_stdout.strip()
            or cluster_stderr.strip()
            or cluster_stdout.strip()
            or f"cluster_rc={cluster_rc}"
        )
        summary = build_synthetic_summary(
            size=size,
            rounds_per_shard=rounds_per_shard,
            observability=observability,
            bench_error=bench_error,
        )
        synthetic_summary_path = local_attempt_root / "synthetic_summary.json"
        synthetic_summary_path.write_text(json.dumps(summary, indent=2))
        summary_path = synthetic_summary_path

    return {
        "attempt": attempt,
        "stage": stage_name,
        "size": size,
        "remote_root": remote_root,
        "remote_bench_dir": remote_bench_dir,
        "local_root": str(local_attempt_root),
        "observability_path": str(local_observability_dir / "summary.json"),
        "summary_path": str(summary_path) if summary_path else None,
        "phys_iface": phys_iface,
        "node_args": list(node_args),
        "payload_bytes": payload_bytes,
        "task_budget": task_budget,
        "latency_mode": latency_mode,
        "launch_ok": launch_ok,
        "health_ok": health_ok,
        "cluster_rc": cluster_rc,
        "cluster_transport_warning": cluster_transport_warning,
        "cluster_stdout": cluster_stdout,
        "cluster_stderr": cluster_stderr,
        "bench_rc": bench_rc,
        "bench_stdout": bench_stdout,
        "bench_stderr": bench_stderr,
        "summary": summary,
    }


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(json.dumps(manifest, indent=2))


def finalize_conclusion(
    *,
    max_scanned_size: int,
    stable_size: int | None,
    first_cpu_limited_nodes: int | None,
    first_non_cpu_ceiling_nodes: int | None,
    first_platform_ceiling_nodes: int | None,
) -> dict[str, Any]:
    if stable_size == max_scanned_size and first_cpu_limited_nodes is None and first_non_cpu_ceiling_nodes is None and first_platform_ceiling_nodes is None:
        status = f"cpu_safe_up_to_{max_scanned_size}"
    elif first_cpu_limited_nodes is not None:
        status = "cpu_limited"
    elif first_platform_ceiling_nodes is not None:
        status = "platform_ceiling"
    elif first_non_cpu_ceiling_nodes is not None:
        status = "non_cpu_ceiling"
    else:
        status = "inconclusive"
    return {
        "status": status,
        "cpu_safe_max_nodes": stable_size,
        "first_cpu_limited_nodes": first_cpu_limited_nodes,
        "first_non_cpu_ceiling_nodes": first_non_cpu_ceiling_nodes,
        "first_platform_ceiling_nodes": first_platform_ceiling_nodes,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--output-root", default="")
    parser.add_argument("--remote-base", default="")
    parser.add_argument("--remote", default=DEFAULT_REMOTE)
    parser.add_argument("--port", default=DEFAULT_PORT)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--remote-sudo-password", default=DEFAULT_REMOTE_SUDO_PASSWORD)
    parser.add_argument("--variant-label", default="public")
    parser.add_argument("--variant", default=PUBLIC_WORST_CASE_VARIANT)
    parser.add_argument("--coarse-sizes", default="20,40,60,80,100,120,140,160,180,200")
    parser.add_argument("--guardrail-ratio", type=float, default=1.25)
    parser.add_argument("--cpu-busy-threshold", type=float, default=90.0)
    parser.add_argument("--vcpu-count", type=int, default=104)
    parser.add_argument("--coarse-rounds-per-shard", type=int, default=5)
    parser.add_argument("--fine-rounds-per-shard", type=int, default=5)
    parser.add_argument("--coarse-observe-sec", type=int, default=45)
    parser.add_argument("--fine-observe-sec", type=int, default=60)
    parser.add_argument("--coarse-stabilize-sec", type=int, default=180)
    parser.add_argument("--fine-stabilize-sec", type=int, default=180)
    parser.add_argument("--poll-sec", type=float, default=0.5)
    parser.add_argument("--cpu-sample-sec", type=float, default=1.0)
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument(
        "--latency-mode",
        choices=("global_probe", "payload_fetch"),
        default="global_probe",
    )
    parser.add_argument("--task-budget", type=int, default=0)
    parser.add_argument("--phys-iface", default="")
    parser.add_argument(
        "--node-arg",
        action="append",
        default=[],
        help="extra openagent node arg forwarded to every remote node start; repeat as needed",
    )
    parser.add_argument("--fine-repetitions", type=int, default=3)
    parser.add_argument("--retry-attempts", type=int, default=2)
    args = parser.parse_args()
    global REMOTE, PORT, PASSWORD, REMOTE_SUDO_PASSWORD
    REMOTE = args.remote
    PORT = str(args.port)
    PASSWORD = args.password
    REMOTE_SUDO_PASSWORD = args.remote_sudo_password

    repo = Path(args.repo).resolve()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    local_results_root = (
        Path(args.output_root).resolve()
        if args.output_root
        else repo / "results" / f"capacity_public_{timestamp}"
    )
    local_results_root.mkdir(parents=True, exist_ok=True)

    remote_base = args.remote_base or f"/home/ma/openagent-capacity-{timestamp}"
    upload_scripts(repo)
    run(ssh_cmd(f"mkdir -p {shlex.quote(remote_base)}"))

    coarse_sizes = parse_sizes(args.coarse_sizes)
    manifest_path = local_results_root / "capacity_manifest.json"
    report_json = local_results_root / "capacity_report.json"
    report_md = local_results_root / "capacity_report.md"
    manifest: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "remote": REMOTE,
        "port": PORT,
        "variant_label": args.variant_label,
        "variant": args.variant,
        "guardrail_ratio": args.guardrail_ratio,
        "cpu_busy_threshold": args.cpu_busy_threshold,
        "vcpu_count": args.vcpu_count,
        "phys_iface": args.phys_iface,
        "node_args": list(args.node_arg),
        "payload_bytes": args.payload_bytes,
        "latency_mode": args.latency_mode,
        "task_budget": args.task_budget,
        "remote_base": remote_base,
        "local_results_root": str(local_results_root),
        "coarse_sizes": coarse_sizes,
        "coarse_runs": [],
        "fine_runs": [],
        "conclusion": {},
    }
    write_manifest(manifest_path, manifest)

    stable_size: int | None = None
    stable_reference: dict[str, Any] | None = None
    first_coarse_failure_size: int | None = None
    first_cpu_limited_nodes: int | None = None
    first_non_cpu_ceiling_nodes: int | None = None
    first_platform_ceiling_nodes: int | None = None

    for size in coarse_sizes:
        attempts: list[dict[str, Any]] = []
        selected_attempt = None
        selected_evaluation = None

        for attempt in range(1, args.retry_attempts + 1):
            run_record = run_single_attempt(
                stage_name="coarse",
                size=size,
                attempt=attempt,
                variant_label=args.variant_label,
                variant=args.variant,
                remote_base=remote_base,
                local_results_root=local_results_root,
                rounds_per_shard=args.coarse_rounds_per_shard,
                observe_sec=args.coarse_observe_sec,
                stabilize_sec=args.coarse_stabilize_sec,
                poll_sec=args.poll_sec,
                cpu_sample_sec=args.cpu_sample_sec,
                phys_iface=args.phys_iface,
                node_args=args.node_arg,
                payload_bytes=args.payload_bytes,
                task_budget=args.task_budget,
                latency_mode=args.latency_mode,
            )
            run_record["evaluation"] = evaluate_single_run(
                run_record["summary"],
                stable_reference,
                guardrail_ratio=args.guardrail_ratio,
                cpu_busy_threshold=args.cpu_busy_threshold,
                variant=args.variant,
            )
            attempts.append(run_record)
            selected_attempt = attempt
            selected_evaluation = run_record["evaluation"]

            if selected_evaluation["passed"]:
                break
            if selected_evaluation["status"] != "platform_ceiling":
                break

        stage_row = {
            "size": size,
            "reference_before": stable_reference,
            "attempts": attempts,
            "selected_attempt": selected_attempt,
            "evaluation": selected_evaluation,
        }
        manifest["coarse_runs"].append(stage_row)
        write_manifest(manifest_path, manifest)

        if selected_evaluation is None:
            first_platform_ceiling_nodes = size
            first_coarse_failure_size = size
            break

        if selected_evaluation["passed"]:
            stable_size = size
            stable_reference = build_reference(attempts[selected_attempt - 1]["summary"])
            continue

        first_coarse_failure_size = size
        if selected_evaluation["status"] == "cpu_limited":
            first_cpu_limited_nodes = size
        elif selected_evaluation["status"] == "platform_ceiling":
            first_platform_ceiling_nodes = size
        else:
            first_non_cpu_ceiling_nodes = size
        break

    if stable_size is not None and first_coarse_failure_size is not None:
        fine_sizes = select_fine_sizes(stable_size, first_coarse_failure_size, step=20)
        manifest["fine_sizes"] = fine_sizes
        write_manifest(manifest_path, manifest)

        previous_reference = stable_reference
        fine_stable_size = stable_size
        for size in fine_sizes:
            attempts = []
            for attempt in range(1, args.fine_repetitions + 1):
                run_record = run_single_attempt(
                    stage_name="fine",
                    size=size,
                    attempt=attempt,
                    variant_label=args.variant_label,
                    variant=args.variant,
                    remote_base=remote_base,
                    local_results_root=local_results_root,
                    rounds_per_shard=args.fine_rounds_per_shard,
                    observe_sec=args.fine_observe_sec,
                    stabilize_sec=args.fine_stabilize_sec,
                    poll_sec=args.poll_sec,
                    cpu_sample_sec=args.cpu_sample_sec,
                    phys_iface=args.phys_iface,
                    node_args=args.node_arg,
                    payload_bytes=args.payload_bytes,
                    task_budget=args.task_budget,
                    latency_mode=args.latency_mode,
                )
                attempts.append(run_record)

            group_evaluation = evaluate_repetition_group(
                [attempt["summary"] for attempt in attempts],
                previous_reference,
                guardrail_ratio=args.guardrail_ratio,
                cpu_busy_threshold=args.cpu_busy_threshold,
                variant=args.variant,
            )
            for attempt, evaluation in zip(
                attempts,
                group_evaluation["evaluations"],
                strict=True,
            ):
                attempt["evaluation"] = evaluation
            selected_attempt = 1
            for attempt in attempts:
                if attempt["evaluation"]["passed"]:
                    selected_attempt = attempt["attempt"]
                    break
            stage_row = {
                "size": size,
                "reference_before": previous_reference,
                "attempts": attempts,
                "selected_attempt": selected_attempt,
                "group_evaluation": group_evaluation,
            }
            manifest["fine_runs"].append(stage_row)
            write_manifest(manifest_path, manifest)

            if group_evaluation["passed"]:
                previous_reference = group_evaluation["reference_after"]
                fine_stable_size = size
                stable_size = size
                continue

            if group_evaluation["status"] == "cpu_limited":
                first_cpu_limited_nodes = size
            elif group_evaluation["status"] == "platform_ceiling":
                first_platform_ceiling_nodes = size
            else:
                first_non_cpu_ceiling_nodes = size
            stable_size = fine_stable_size
            break

    manifest["conclusion"] = finalize_conclusion(
        max_scanned_size=coarse_sizes[-1],
        stable_size=stable_size,
        first_cpu_limited_nodes=first_cpu_limited_nodes,
        first_non_cpu_ceiling_nodes=first_non_cpu_ceiling_nodes,
        first_platform_ceiling_nodes=first_platform_ceiling_nodes,
    )
    write_manifest(manifest_path, manifest)

    run(
        [
            "python3",
            str(repo / "scripts" / "scalebench_report.py"),
            "--manifest-json",
            str(manifest_path),
            "--report-json",
            str(report_json),
            "--report-md",
            str(report_md),
        ]
    )


if __name__ == "__main__":
    main()
