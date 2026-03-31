#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


SUMMARY_COLUMNS = [
    "nodes",
    "run_id",
    "bootstrap_pass",
    "bench_pass",
    "ceiling_type",
    "e2e_p95",
    "e2e_p99",
    "cpu_p95",
    "tcp_count",
    "tcp_per_node",
    "peer_p50",
    "mesh_p50",
    "load_max",
    "bootstrap_time",
    "connection_scaling_alert",
    "status",
]


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def metric(block: dict[str, Any], stat: str) -> Any:
    return (block or {}).get(stat)


def load_run_metrics(path: Path) -> dict[str, Any]:
    return load_json(path)


def summarize_breakpoint(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_nodes: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        by_nodes.setdefault(int(row["nodes"]), []).append(row)
    ordered_nodes = sorted(by_nodes)
    trend = []
    for nodes in ordered_nodes:
        bucket = by_nodes[nodes]
        p99_values = [float(row["e2e_p99"]) for row in bucket if row.get("e2e_p99") is not None]
        tcp_values = [float(row["tcp_per_node"]) for row in bucket if row.get("tcp_per_node") is not None]
        peer_values = [float(row["peer_p50"]) for row in bucket if row.get("peer_p50") is not None]
        mesh_values = [float(row["mesh_p50"]) for row in bucket if row.get("mesh_p50") is not None]
        cpu_values = [float(row["cpu_p95"]) for row in bucket if row.get("cpu_p95") is not None]
        trend.append(
            {
                "nodes": nodes,
                "e2e_p99_median": sorted(p99_values)[len(p99_values) // 2] if p99_values else None,
                "tcp_per_node_median": sorted(tcp_values)[len(tcp_values) // 2] if tcp_values else None,
                "peer_p50_median": sorted(peer_values)[len(peer_values) // 2] if peer_values else None,
                "mesh_p50_median": sorted(mesh_values)[len(mesh_values) // 2] if mesh_values else None,
                "cpu_p95_median": sorted(cpu_values)[len(cpu_values) // 2] if cpu_values else None,
            }
        )

    breakpoint_interval = None
    for left, right in zip(trend, trend[1:]):
        if left["e2e_p99_median"] is None or right["e2e_p99_median"] is None:
            continue
        growth = right["e2e_p99_median"] / max(1.0, left["e2e_p99_median"])
        signals = 0
        for key in ("tcp_per_node_median", "peer_p50_median", "mesh_p50_median", "cpu_p95_median"):
            if left[key] is None or right[key] is None:
                continue
            if right[key] > left[key]:
                signals += 1
        if growth >= 1.5 and signals >= 2:
            breakpoint_interval = [left["nodes"], right["nodes"]]
            break
    return {"trend": trend, "breakpoint_interval": breakpoint_interval}


def render_report(rows: list[dict[str, Any]], insight: dict[str, Any]) -> str:
    lines = [
        "# Breakpoint Baseline Report",
        "",
        "| nodes | run_id | bootstrap_pass | bench_pass | ceiling_type | e2e_p95 | e2e_p99 | cpu_p95 | tcp/node | peer_p50 | mesh_p50 | status |",
        "| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['nodes']} | {row['run_id']} | {row['bootstrap_pass']} | {row['bench_pass']} | "
            f"{row['ceiling_type']} | {row['e2e_p95']} | {row['e2e_p99']} | {row['cpu_p95']} | "
            f"{row['tcp_per_node']} | {row['peer_p50']} | {row['mesh_p50']} | {row['status']} |"
        )
    lines.extend(["", "## Trend", ""])
    for item in insight["trend"]:
        lines.append(
            f"- {item['nodes']} nodes: e2e_p99={item['e2e_p99_median']}, cpu_p95={item['cpu_p95_median']}, "
            f"tcp/node={item['tcp_per_node_median']}, peer_p50={item['peer_p50_median']}, mesh_p50={item['mesh_p50_median']}"
        )
    lines.extend(["", f"拐点区间：`{insight['breakpoint_interval']}`"])
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-root", required=True)
    parser.add_argument("--out-csv", required=True)
    parser.add_argument("--out-json", required=True)
    parser.add_argument("--out-md", required=True)
    args = parser.parse_args()

    results_root = Path(args.results_root).resolve()
    rows = []
    for metrics_path in sorted(results_root.glob("runs/*/run_metrics.json")):
        rows.append(load_run_metrics(metrics_path))
    rows = sorted(rows, key=lambda row: (int(row["nodes"]), str(row["run_id"])))

    with Path(args.out_csv).resolve().open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in SUMMARY_COLUMNS})

    insight = summarize_breakpoint(rows)
    Path(args.out_json).resolve().write_text(json.dumps({"rows": rows, "insight": insight}, indent=2))
    Path(args.out_md).resolve().write_text(render_report(rows, insight))
    print(render_report(rows, insight), end="")


if __name__ == "__main__":
    main()
