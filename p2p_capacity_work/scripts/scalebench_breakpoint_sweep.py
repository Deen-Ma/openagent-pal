#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        check=False,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )


def load_config(path: Path) -> dict:
    return json.loads(path.read_text())


def load_status(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--config", required=True)
    parser.add_argument("--output-root", default="")
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    config = load_config(Path(args.config).resolve())
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(args.output_root).resolve() if args.output_root else repo / "results" / f"breakpoint_p6_{stamp}"
    output_root.mkdir(parents=True, exist_ok=True)

    run_logs = []
    for repetition in range(1, int(config["repetitions"]) + 1):
        for nodes in config["nodes"]:
            run_id = f"run{repetition:02d}"
            attempts_allowed = int(config.get("retry_budget", 0)) + 1
            for attempt_index in range(1, attempts_allowed + 1):
                cmd = [
                    "python3",
                    str(repo / "scripts" / "scalebench_run_baseline.py"),
                    "--repo", str(repo),
                    "--config", str(Path(args.config).resolve()),
                    "--nodes", str(nodes),
                    "--run-id", run_id,
                    "--output-root", str(output_root),
                ]
                result = run(cmd)
                last_returncode = result.returncode
                stdout_log = output_root / f"batch_{nodes}_{run_id}_try{attempt_index:02d}.stdout.log"
                stderr_log = output_root / f"batch_{nodes}_{run_id}_try{attempt_index:02d}.stderr.log"
                stdout_log.write_text(result.stdout)
                stderr_log.write_text(result.stderr)
                status_path = output_root / "runs" / f"n{nodes:03d}_{run_id}" / "run_status.json"
                run_status = load_status(status_path)
                run_logs.append(
                    {
                        "nodes": nodes,
                        "run_id": run_id,
                        "attempt_index": attempt_index,
                        "returncode": result.returncode,
                        "status": run_status.get("status"),
                    }
                )
                if result.returncode == 0 and run_status.get("status") == "success":
                    break
                time.sleep(5)
            time.sleep(int(config["cooldown_sec"]))

    (output_root / "batch_runs.json").write_text(json.dumps(run_logs, indent=2))
    aggregate_cmd = [
        "python3",
        str(repo / "scripts" / "scalebench_aggregate_breakpoint.py"),
        "--results-root", str(output_root),
        "--out-csv", str(output_root / "aggregate" / "breakpoint_summary.csv"),
        "--out-json", str(output_root / "aggregate" / "breakpoint_summary.json"),
        "--out-md", str(output_root / "aggregate" / "breakpoint_report.md"),
    ]
    (output_root / "aggregate").mkdir(exist_ok=True)
    aggregate = run(aggregate_cmd)
    (output_root / "aggregate" / "aggregate.stdout.log").write_text(aggregate.stdout)
    (output_root / "aggregate" / "aggregate.stderr.log").write_text(aggregate.stderr)

    plots_cmd = [
        "python3",
        str(repo / "scripts" / "scalebench_plot_breakpoint.py"),
        "--summary-csv", str(output_root / "aggregate" / "breakpoint_summary.csv"),
        "--plots-dir", str(output_root / "plots"),
    ]
    plot = run(plots_cmd)
    (output_root / "plots.stdout.log").write_text(plot.stdout)
    (output_root / "plots.stderr.log").write_text(plot.stderr)
    print((output_root / "aggregate" / "breakpoint_report.md").read_text(), end="")


if __name__ == "__main__":
    main()
