#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from scalebench_capacity_lib import metric


def fmt(value: Any) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def selected_summary(stage_row: dict[str, Any]) -> dict[str, Any] | None:
    selected_attempt = stage_row.get("selected_attempt")
    for attempt in stage_row.get("attempts", []):
        if attempt.get("attempt") == selected_attempt:
            return attempt.get("summary")
    return None


def summarize_rows(stage_rows: list[dict[str, Any]], stage_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for stage_row in stage_rows:
        summary = selected_summary(stage_row)
        evaluation = stage_row.get("evaluation") or stage_row.get("group_evaluation") or {}
        rows.append(
            {
                "stage": stage_name,
                "size": stage_row.get("size"),
                "status": evaluation.get("status"),
                "reason": evaluation.get("reason"),
                "bootstrap_pass": (summary or {}).get("bootstrap_pass"),
                "bench_pass": (summary or {}).get("bench_ok"),
                "ceiling_type": evaluation.get("status"),
                "e2e_p95": metric(summary or {}, "e2e_latency_ms", "p95"),
                "e2e_p99": metric(summary or {}, "e2e_latency_ms", "p99"),
                "host_cpu_busy_pct_p95": metric(summary or {}, "host_cpu_busy_pct", "p95"),
                "tcp_count": (summary or {}).get("established_tcp_count"),
                "tcp_per_node": (summary or {}).get("tcp_per_node_avg"),
                "peer_p50": (summary or {}).get("peer_per_node_p50"),
                "mesh_p50": (summary or {}).get("mesh_per_node_p50"),
                "load_max": metric(summary or {}, "load1", "max"),
                "bootstrap_time": (summary or {}).get("bootstrap_time_sec"),
                "connection_scaling_alert": (summary or {}).get("connection_scaling_alert"),
                "summary_path": (
                    next(
                        (
                            attempt.get("summary_path")
                            for attempt in stage_row.get("attempts", [])
                            if attempt.get("attempt") == stage_row.get("selected_attempt")
                        ),
                        None,
                    )
                ),
            }
        )
    return rows


def build_markdown(manifest: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    conclusion = manifest.get("conclusion", {})
    md: list[str] = []
    md.append("# Public Worst-Case Capacity Report")
    md.append("")
    md.append(
        f"实验对象：`{manifest['variant']}`，`{manifest['vcpu_count']} vCPU`，"
        f"guardrail=`p95 <= previous * {manifest['guardrail_ratio']:.2f}`，"
        f"CPU 判定阈值=`host_cpu_busy_pct.p95 >= {manifest['cpu_busy_threshold']:.0f}%`。"
    )
    md.append("")
    md.append("## Capacity Decision")
    md.append("")
    md.append(f"- CPU-safe 最大节点数：`{fmt(conclusion.get('cpu_safe_max_nodes'))}`")
    md.append(f"- 首个 CPU ceiling：`{fmt(conclusion.get('first_cpu_limited_nodes'))}`")
    md.append(f"- 首个非 CPU ceiling：`{fmt(conclusion.get('first_non_cpu_ceiling_nodes'))}`")
    md.append(f"- 首个平台 ceiling：`{fmt(conclusion.get('first_platform_ceiling_nodes'))}`")
    md.append(f"- 最终状态：`{fmt(conclusion.get('status'))}`")
    md.append("")
    md.append("## Stage Results")
    md.append("")
    md.append(
        "| Stage | Size | Status | bootstrap_pass | bench_pass | e2e p95 | e2e p99 | cpu p95 | tcp/node | peer p50 | mesh p50 | conn_alert | Reason |"
    )
    md.append("| --- | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |")
    for row in rows:
        md.append(
            f"| {row['stage']} | {fmt(row['size'])} | {fmt(row['status'])} | "
            f"{fmt(row['bootstrap_pass'])} | {fmt(row['bench_pass'])} | "
            f"{fmt(row['e2e_p95'])} | {fmt(row['e2e_p99'])} | {fmt(row['host_cpu_busy_pct_p95'])} | "
            f"{fmt(row['tcp_per_node'])} | {fmt(row['peer_p50'])} | {fmt(row['mesh_p50'])} | "
            f"{fmt(row['connection_scaling_alert'])} | {fmt(row['reason'])} |"
        )
    md.append("")
    md.append("## Raw Data")
    md.append("")
    for row in rows:
        if row["summary_path"]:
            md.append(f"- `{row['stage']}` size `{row['size']}`: `{row['summary_path']}`")
    return "\n".join(md) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest-json", required=True)
    parser.add_argument("--report-json", required=True)
    parser.add_argument("--report-md", required=True)
    args = parser.parse_args()

    manifest = json.loads(Path(args.manifest_json).read_text())
    rows = summarize_rows(manifest.get("coarse_runs", []), "coarse")
    rows.extend(summarize_rows(manifest.get("fine_runs", []), "fine"))

    report = {
        "manifest_json": str(Path(args.manifest_json).resolve()),
        "variant": manifest["variant"],
        "vcpu_count": manifest["vcpu_count"],
        "guardrail_ratio": manifest["guardrail_ratio"],
        "cpu_busy_threshold": manifest["cpu_busy_threshold"],
        "conclusion": manifest.get("conclusion", {}),
        "rows": rows,
    }
    Path(args.report_json).write_text(json.dumps(report, indent=2))
    Path(args.report_md).write_text(build_markdown(manifest, rows))


if __name__ == "__main__":
    main()
