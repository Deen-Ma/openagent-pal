#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def summary_metric(summary: dict[str, Any], key: str, stat: str = "p95") -> float | None:
    block = summary.get(key) or {}
    if not isinstance(block, dict):
        return None
    value = block.get(stat)
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    return None


def summarize_weak_nodes(result_dir: Path) -> tuple[int, int]:
    sample_topology = result_dir / "sample_topology"
    sample_pubsub = result_dir / "sample_pubsub"
    if not sample_topology.exists() or not sample_pubsub.exists():
        return (0, 0)

    weak_nodes = 0
    weak_topics = 0
    for path in sorted(sample_topology.glob("final_*.json")):
        pubsub_path = sample_pubsub / path.name
        if not pubsub_path.exists():
            continue
        topology = json.loads(path.read_text())
        pubsub = json.loads(pubsub_path.read_text())
        mesh_by_topic = pubsub.get("topic_mesh") or {}
        same_topic = topology.get("same_topic_connected_counts") or {}
        mesh_min = int(topology.get("same_topic_mesh_min") or 0)
        node_weak_topics = 0
        for topic in same_topic:
            if len(mesh_by_topic.get(topic) or []) < mesh_min:
                node_weak_topics += 1
        if node_weak_topics:
            weak_nodes += 1
            weak_topics += node_weak_topics
    return (weak_nodes, weak_topics)


def selected_attempt(manifest: dict[str, Any], size: int) -> dict[str, Any]:
    for row in manifest.get("coarse_runs") or []:
        if int(row.get("size") or 0) != size:
            continue
        attempt_index = int(row.get("selected_attempt") or 0)
        attempts = row.get("attempts") or []
        if 1 <= attempt_index <= len(attempts):
            return attempts[attempt_index - 1]
        if attempts:
            return attempts[-1]
    raise ValueError(f"size {size} not found in manifest")


def build_profile_row(manifest_path: Path, size: int) -> dict[str, Any]:
    manifest = json.loads(manifest_path.read_text())
    attempt = selected_attempt(manifest, size)
    summary = attempt.get("summary") or {}
    evaluation = attempt.get("evaluation") or {}
    summary_path = Path(attempt.get("summary_path") or "")
    if summary_path.exists():
        result_dir = summary_path.parent
    else:
        result_dir = Path(summary.get("result_dir") or "")
        if not result_dir.is_absolute():
            local_root = Path(attempt.get("local_root") or manifest_path.parent)
            result_dir = local_root / result_dir
    weak_nodes, weak_topics = summarize_weak_nodes(result_dir)
    return {
        "profile": manifest.get("variant_label") or manifest_path.parent.name,
        "manifest_path": str(manifest_path),
        "summary_path": str(summary_path) if summary_path else attempt.get("summary_path"),
        "result_dir": str(result_dir),
        "node_args": manifest.get("node_args") or [],
        "functional_ok": bool(evaluation.get("functional_ok")),
        "bench_ok": bool(summary.get("bench_ok")),
        "passed": bool(evaluation.get("passed")),
        "status": evaluation.get("status"),
        "reason": evaluation.get("reason"),
        "target_cover100_ms_p95": summary_metric(summary, "target_cover100_ms"),
        "global_cover100_ms_p95": summary_metric(summary, "global_cover100_ms"),
        "seen_ratio_global_median": summary_metric(summary, "seen_ratio_global", "median"),
        "host_cpu_busy_pct_p95": summary_metric(summary, "host_cpu_busy_pct"),
        "weak_nodes": weak_nodes,
        "weak_topics": weak_topics,
    }


def ranking_key(row: dict[str, Any]) -> tuple[Any, ...]:
    inf = float("inf")
    return (
        0 if row["functional_ok"] else 1,
        0 if row["bench_ok"] else 1,
        0 if row["passed"] else 1,
        row["target_cover100_ms_p95"] if row["target_cover100_ms_p95"] is not None else inf,
        row["global_cover100_ms_p95"] if row["global_cover100_ms_p95"] is not None else inf,
        row["weak_nodes"],
        row["weak_topics"],
        row["host_cpu_busy_pct_p95"] if row["host_cpu_busy_pct_p95"] is not None else inf,
        row["profile"],
    )


def collect_rows(manifests: list[Path], size: int) -> list[dict[str, Any]]:
    rows = [build_profile_row(path, size) for path in manifests]
    return sorted(rows, key=ranking_key)


def render_markdown(rows: list[dict[str, Any]]) -> str:
    header = [
        "| profile | functional_ok | bench_ok | passed | target_p95_ms | global_p95_ms | seen_ratio | weak_nodes | weak_topics | cpu_p95 | status |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    body = []
    for row in rows:
        body.append(
            "| {profile} | {functional_ok} | {bench_ok} | {passed} | {target} | {global_} | {seen} | {weak_nodes} | {weak_topics} | {cpu} | {status} |".format(
                profile=row["profile"],
                functional_ok="yes" if row["functional_ok"] else "no",
                bench_ok="yes" if row["bench_ok"] else "no",
                passed="yes" if row["passed"] else "no",
                target=row["target_cover100_ms_p95"] if row["target_cover100_ms_p95"] is not None else "-",
                global_=row["global_cover100_ms_p95"] if row["global_cover100_ms_p95"] is not None else "-",
                seen=row["seen_ratio_global_median"] if row["seen_ratio_global_median"] is not None else "-",
                weak_nodes=row["weak_nodes"],
                weak_topics=row["weak_topics"],
                cpu=row["host_cpu_busy_pct_p95"] if row["host_cpu_busy_pct_p95"] is not None else "-",
                status=row["status"] or "-",
            )
        )
    return "\n".join(header + body) + "\n"


def render_csv(rows: list[dict[str, Any]]) -> str:
    keys = [
        "profile",
        "functional_ok",
        "bench_ok",
        "passed",
        "target_cover100_ms_p95",
        "global_cover100_ms_p95",
        "seen_ratio_global_median",
        "weak_nodes",
        "weak_topics",
        "host_cpu_busy_pct_p95",
        "status",
        "reason",
        "summary_path",
        "manifest_path",
    ]
    lines = [",".join(keys)]
    for row in rows:
        values = []
        for key in keys:
            value = row.get(key)
            text = "" if value is None else str(value)
            if "," in text or "\"" in text:
                text = "\"" + text.replace("\"", "\"\"") + "\""
            values.append(text)
        lines.append(",".join(values))
    return "\n".join(lines) + "\n"


def resolve_manifests(items: list[str]) -> list[Path]:
    manifests: list[Path] = []
    for item in items:
        path = Path(item).resolve()
        if path.is_file():
            manifests.append(path)
            continue
        candidate = path / "capacity_manifest.json"
        if candidate.exists():
            manifests.append(candidate)
            continue
        raise FileNotFoundError(f"missing manifest under {path}")
    return manifests


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="+", help="capacity result roots or manifest paths")
    parser.add_argument("--size", type=int, default=40)
    parser.add_argument("--json-out", default="")
    parser.add_argument("--markdown-out", default="")
    parser.add_argument("--csv-out", default="")
    args = parser.parse_args()

    manifests = resolve_manifests(args.inputs)
    rows = collect_rows(manifests, args.size)
    payload = {"size": args.size, "rows": rows}
    markdown = render_markdown(rows)
    csv_text = render_csv(rows)

    if args.json_out:
        Path(args.json_out).resolve().write_text(json.dumps(payload, indent=2))
    if args.markdown_out:
        Path(args.markdown_out).resolve().write_text(markdown)
    if args.csv_out:
        Path(args.csv_out).resolve().write_text(csv_text)

    print(markdown, end="")


if __name__ == "__main__":
    main()
