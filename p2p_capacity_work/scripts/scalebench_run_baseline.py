#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shlex
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def run(cmd: list[str], *, capture: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        check=False,
        text=True,
        capture_output=capture,
        encoding="utf-8",
        errors="replace",
    )


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2))


def write_status(path: Path, status: str, **extra: Any) -> None:
    payload = {"status": status, "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}
    payload.update(extra)
    write_json(path, payload)


def scp_nodes(remote: str, port: int, password: str, sudo_password: str, remote_root: str, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    remote_cmd = (
        f"printf '{sudo_password}\\n' | sudo -S bash -lc "
        f"\"cd {shlex.quote(remote_root)} && tar -cf - node*/state/events.jsonl 2>/dev/null\""
    )
    ssh_proc = subprocess.Popen(
        [
            "sshpass", "-p", password,
            "ssh", "-o", "StrictHostKeyChecking=no", "-p", str(port), remote, remote_cmd,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    tar_proc = subprocess.Popen(
        ["tar", "-xf", "-", "-C", str(dest)],
        stdin=ssh_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert ssh_proc.stdout is not None
    ssh_proc.stdout.close()
    _, tar_stderr = tar_proc.communicate()
    ssh_stderr = b""
    if ssh_proc.stderr is not None:
        ssh_stderr = ssh_proc.stderr.read()
    ssh_returncode = ssh_proc.wait()
    if ssh_returncode != 0 or tar_proc.returncode != 0:
        ssh_msg = ssh_stderr.decode("utf-8", "replace").strip()
        tar_msg = tar_stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(f"copy node events failed: ssh={ssh_msg!r} tar={tar_msg!r}")


def remote_cleanup(remote: str, port: int, password: str, sudo_password: str) -> int:
    run(
        [
            "sshpass", "-p", password,
            "ssh", "-o", "StrictHostKeyChecking=no", "-p", str(port), remote,
            f"printf '{sudo_password}\\n' | sudo -S bash -lc 'docker ps -aq --filter name=oa-formal- | xargs -r docker rm -f >/dev/null'",
        ],
        capture=True,
    )
    verify = run(
        [
            "sshpass", "-p", password,
            "ssh", "-o", "StrictHostKeyChecking=no", "-p", str(port), remote,
            "docker ps -aq --filter name=oa-formal- | wc -l",
        ],
        capture=True,
    )
    if verify.returncode != 0:
        raise RuntimeError(f"remote cleanup verification failed: {verify.stderr.strip()}")
    try:
        return int((verify.stdout or "0").strip())
    except ValueError as exc:
        raise RuntimeError(f"unexpected cleanup verification output: {verify.stdout!r}") from exc


def single_row_csv(path: Path, row: dict[str, Any]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--config", required=True)
    parser.add_argument("--nodes", required=True, type=int)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    config = load_json(Path(args.config).resolve())
    output_root = Path(args.output_root).resolve()
    run_dir = output_root / "runs" / f"n{args.nodes:03d}_{args.run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = run_dir / "logs"
    metrics_dir = run_dir / "metrics"
    nodes_dir = run_dir / "nodes"
    logs_dir.mkdir(exist_ok=True)
    metrics_dir.mkdir(exist_ok=True)

    snapshot = dict(config)
    snapshot["nodes"] = args.nodes
    snapshot["run_id"] = args.run_id
    write_json(run_dir / "run_config.snapshot.json", snapshot)
    status_path = run_dir / "run_status.json"
    write_status(status_path, "prepared")

    label = f"n{args.nodes:03d}_{args.run_id}"
    remote_cfg = config["remote"]
    remote_base = f"{remote_cfg['base_dir'].rstrip('/')}/{label}"
    cmd = [
        "python3",
        str(repo / "scripts" / "scalebench_orchestrate.py"),
        "--repo", str(repo),
        "--output-root", str(run_dir / "orchestrate"),
        "--remote-base", remote_base,
        "--remote", remote_cfg["host"],
        "--port", str(remote_cfg["port"]),
        "--password", remote_cfg["password"],
        "--remote-sudo-password", remote_cfg["sudo_password"],
        "--variant-label", label,
        "--variant", config["variant"],
        "--coarse-sizes", str(args.nodes),
        "--coarse-rounds-per-shard", str(config.get("runtime_rounds", 5)),
        "--fine-rounds-per-shard", str(config.get("runtime_rounds", 5)),
        "--coarse-observe-sec", str(config["observe_sec"]),
        "--fine-observe-sec", str(config["observe_sec"]),
        "--coarse-stabilize-sec", str(config["stabilize_sec"]),
        "--fine-stabilize-sec", str(config["stabilize_sec"]),
        "--poll-sec", str(config["poll_sec"]),
        "--cpu-sample-sec", str(config["cpu_sample_sec"]),
        "--fine-repetitions", "1",
        "--retry-attempts", "1",
        "--payload-bytes", str(config["payload_bytes"]),
        "--latency-mode", str(config.get("latency_mode", "global_probe")),
        "--task-budget", str(config.get("message_budget", 0)),
    ]
    for item in config["node_args"]:
        cmd.append(f"--node-arg={item}")

    try:
        write_status(status_path, "cleanup_started")
        remaining = remote_cleanup(
            remote_cfg["host"],
            int(remote_cfg["port"]),
            remote_cfg["password"],
            remote_cfg["sudo_password"],
        )
        if remaining != 0:
            raise RuntimeError(f"pre-run cleanup left {remaining} oa-formal containers behind")
        write_status(status_path, "cleanup_ok", remaining_containers=remaining)

        write_status(status_path, "orchestrate_started")
        result = run(cmd, capture=True)
        (logs_dir / "run.log").write_text(result.stdout)
        (logs_dir / "stderr.log").write_text(result.stderr)
        if result.returncode != 0:
            write_status(status_path, "failed", returncode=result.returncode)
            raise SystemExit(result.returncode)

        manifest = load_json(run_dir / "orchestrate" / "capacity_manifest.json")
        attempt = manifest["coarse_runs"][0]["attempts"][0]
        summary = attempt["summary"]
        write_json(metrics_dir / "bench_summary.json", summary)
        write_status(status_path, "aggregating")
        evaluation = (manifest.get("coarse_runs", [{}])[0] or {}).get("evaluation", {})

        metrics = {
            "nodes": args.nodes,
            "run_id": args.run_id,
            "bootstrap_pass": summary.get("bootstrap_pass"),
            "bench_pass": summary.get("bench_ok"),
            "ceiling_type": evaluation.get("status"),
            "e2e_p95": (summary.get("e2e_latency_ms") or {}).get("p95"),
            "e2e_p99": (summary.get("e2e_latency_ms") or {}).get("p99"),
            "cpu_p95": (summary.get("host_cpu_busy_pct") or {}).get("p95"),
            "tcp_count": summary.get("established_tcp_count"),
            "tcp_per_node": summary.get("tcp_per_node_avg"),
            "peer_p50": summary.get("peer_per_node_p50"),
            "mesh_p50": summary.get("mesh_per_node_p50"),
            "load_max": (summary.get("load1") or {}).get("max"),
            "bootstrap_time": summary.get("bootstrap_time_sec"),
            "connection_scaling_alert": summary.get("connection_scaling_alert"),
            "status": "success" if summary.get("bench_ok") else "partial",
        }
        write_json(run_dir / "run_metrics.json", metrics)
        write_json(metrics_dir / "run_metrics.json", metrics)
        single_row_csv(run_dir / "run_metrics.csv", metrics)
        single_row_csv(metrics_dir / "run_metrics.csv", metrics)
        write_status(status_path, "aggregated", result_status=metrics["status"])
    finally:
        try:
            write_status(status_path, "cleaning")
            remaining = remote_cleanup(
                remote_cfg["host"],
                int(remote_cfg["port"]),
                remote_cfg["password"],
                remote_cfg["sudo_password"],
            )
            final_status = "success" if remaining == 0 else "partial"
            write_status(status_path, final_status, remaining_containers=remaining)
        except Exception as cleanup_exc:
            write_status(status_path, "cleanup_failed", error=str(cleanup_exc))
            raise


if __name__ == "__main__":
    main()
