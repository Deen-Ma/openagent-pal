#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from discovery_workload_lib import (
    COARSE_MATCH_TRUE,
    DECLARATION_RECEIVED,
    ELIGIBILITY_TRUE,
    FULL_MATCH_TRUE,
    MANDATORY_EVENT_TYPES,
    PULL_SENT,
    RESPONSE_RECEIVED_BY_REQUESTER,
    DEFAULT_CAPABILITY_TAGS,
    dump_json,
    ensure_dir,
    load_json,
    now_iso,
    read_jsonl,
    stats,
)

TWO_STAGE_MODE = "deterministic_two_stage"


def safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    if not rows:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([])
        return
    all_keys: Set[str] = set()
    for row in rows:
        all_keys.update(row.keys())
    fieldnames = sorted(all_keys)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def read_requests(
    *,
    run_root: Path,
) -> Tuple[
    Dict[str, Dict[str, Any]],
    Dict[str, Dict[str, Any]],
    Dict[str, Dict[str, Any]],
    Dict[str, Any],
]:
    results_dir = run_root / "results"
    legacy_requests_path = results_dir / "requests.jsonl"
    declaration_path = results_dir / "requests_declaration.jsonl"
    payload_path = results_dir / "requests_payload.jsonl"

    combined: Dict[str, Dict[str, Any]] = {}
    declaration_map: Dict[str, Dict[str, Any]] = {}
    payload_map: Dict[str, Dict[str, Any]] = {}
    for row in read_jsonl(legacy_requests_path):
        request_id = str(row.get("request_id") or "")
        if not request_id:
            continue
        combined[request_id] = dict(row)

    for row in read_jsonl(declaration_path):
        request_id = str(row.get("request_id") or "")
        if not request_id:
            continue
        declaration_map[request_id] = dict(row)
        merged = combined.setdefault(request_id, {})
        merged.update(row)

    for row in read_jsonl(payload_path):
        request_id = str(row.get("request_id") or "")
        if not request_id:
            continue
        payload_map[request_id] = dict(row)
        merged = combined.setdefault(request_id, {})
        merged.update(row)

    meta = {
        "requests_path": str(legacy_requests_path),
        "requests_declaration_path": str(declaration_path),
        "requests_payload_path": str(payload_path),
        "has_two_stage_requests": bool(declaration_map or payload_map),
    }
    return combined, declaration_map, payload_map, meta


def build_event_index(events_path: Path) -> Dict[str, Dict[str, Any]]:
    request_events: Dict[str, Dict[str, Any]] = {}
    for row in read_jsonl(events_path):
        request_id = str(row.get("request_id") or "")
        event_type = str(row.get("event_type") or "")
        if not request_id or event_type not in MANDATORY_EVENT_TYPES:
            continue
        event_ts_raw = row.get("event_ts")
        if not isinstance(event_ts_raw, (int, float)):
            continue
        event_ts = float(event_ts_raw)
        node_id_raw = row.get("node_id")
        if not isinstance(node_id_raw, int):
            continue
        node_id = int(node_id_raw)
        bucket = request_events.setdefault(
            request_id,
            {
                "visible_nodes": set(),
                "coarse_nodes": set(),
                "full_nodes": set(),
                "pulling_nodes": set(),
                "responding_nodes": set(),
                "response_event_count_total": 0,
                "first_visible_ts": None,
                "last_visible_ts": None,
                "first_pull_ts": None,
                "first_response_ts": None,
            },
        )

        if event_type == DECLARATION_RECEIVED:
            cast = bucket["visible_nodes"]
            assert isinstance(cast, set)
            cast.add(node_id)
            first_visible = bucket["first_visible_ts"]
            last_visible = bucket["last_visible_ts"]
            if first_visible is None or event_ts < float(first_visible):
                bucket["first_visible_ts"] = event_ts
            if last_visible is None or event_ts > float(last_visible):
                bucket["last_visible_ts"] = event_ts
        elif event_type in {ELIGIBILITY_TRUE, COARSE_MATCH_TRUE}:
            cast = bucket["coarse_nodes"]
            assert isinstance(cast, set)
            cast.add(node_id)
        elif event_type == FULL_MATCH_TRUE:
            cast = bucket["full_nodes"]
            assert isinstance(cast, set)
            cast.add(node_id)
        elif event_type == PULL_SENT:
            cast = bucket["pulling_nodes"]
            assert isinstance(cast, set)
            cast.add(node_id)
            first_pull = bucket["first_pull_ts"]
            if first_pull is None or event_ts < float(first_pull):
                bucket["first_pull_ts"] = event_ts
        elif event_type == RESPONSE_RECEIVED_BY_REQUESTER:
            cast = bucket["responding_nodes"]
            assert isinstance(cast, set)
            cast.add(node_id)
            bucket["response_event_count_total"] = int(bucket["response_event_count_total"]) + 1
            first_response = bucket["first_response_ts"]
            if first_response is None or event_ts < float(first_response):
                bucket["first_response_ts"] = event_ts
    return request_events


def aggregate_request_label_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        label = str(
            row.get("required_family")
            or row.get("required_capability")
            or row.get("required_label")
            or "unknown"
        )
        buckets[label].append(row)

    out: List[Dict[str, Any]] = []
    for label in sorted(buckets):
        entries = buckets[label]
        out.append(
            {
                "required_label": label,
                "request_count": len(entries),
                "visible_mean": round(sum(float(entry["visible_count"]) for entry in entries) / max(1, len(entries)), 6),
                "coarse_mean": round(
                    sum(float(entry["coarse_match_count"]) for entry in entries) / max(1, len(entries)),
                    6,
                ),
                "full_mean": round(
                    sum(float(entry["full_match_count"]) for entry in entries) / max(1, len(entries)),
                    6,
                ),
                "actual_pull_mean": round(
                    sum(float(entry["actual_pull_count"]) for entry in entries) / max(1, len(entries)),
                    6,
                ),
                "actual_response_mean": round(
                    sum(float(entry["actual_response_count"]) for entry in entries) / max(1, len(entries)),
                    6,
                ),
                "p_coarse_mean": round(
                    sum(float(entry["p_coarse"]) for entry in entries) / max(1, len(entries)),
                    6,
                ),
                "p_full_mean": round(sum(float(entry["p_full"]) for entry in entries) / max(1, len(entries)), 6),
                "p_pull_mean": round(sum(float(entry["p_pull"]) for entry in entries) / max(1, len(entries)), 6),
                "p_resp_mean": round(sum(float(entry["p_resp"]) for entry in entries) / max(1, len(entries)), 6),
                "coarse_to_full_mean": round(
                    sum(float(entry["coarse_to_full"]) for entry in entries) / max(1, len(entries)),
                    6,
                ),
                "pull_efficiency_mean": round(
                    sum(float(entry["pull_efficiency"]) for entry in entries) / max(1, len(entries)),
                    6,
                ),
            }
        )
    return out


def distribution_rows(values: List[int]) -> List[Dict[str, Any]]:
    counts: Dict[int, int] = defaultdict(int)
    for value in values:
        counts[int(value)] += 1
    total = max(1, len(values))
    cumulative = 0
    rows: List[Dict[str, Any]] = []
    for key in sorted(counts):
        freq = int(counts[key])
        cumulative += freq
        rows.append(
            {
                "value": key,
                "frequency": freq,
                "ratio": round(float(freq) / float(total), 6),
                "cdf": round(float(cumulative) / float(total), 6),
            }
        )
    return rows


def build_summary_markdown(summary: Dict[str, Any]) -> str:
    config = summary.get("config") or {}
    metrics = summary.get("metrics") or {}
    funnel = summary.get("funnel_overall") or {}
    rules = summary.get("matching_rules") or {}
    partial_reasons = summary.get("partial_reasons") or []
    request_distribution = summary.get("request_label_distribution") or {}
    node_distribution = summary.get("node_label_distribution") or {}

    lines = [
        "# Discovery Workload Summary",
        "",
        "## Experiment Config",
        "",
        f"- generated_at: `{summary.get('generated_at', '')}`",
        f"- matching_mode: `{config.get('matching_mode', '')}`",
        f"- constraint_mode: `{config.get('constraint_mode', '')}`",
        f"- variant: `{config.get('variant', '')}`",
        f"- topic: `{config.get('global_topic', '')}`",
        f"- node_count: `{summary.get('node_count', '')}`",
        f"- request_count: `{summary.get('request_count', '')}`",
        f"- repetitions: `{summary.get('repetitions', '')}`",
        f"- random_seed: `{summary.get('random_seed', '')}`",
        f"- pull_policy: `{config.get('pull_policy', '')}`",
        f"- pull_probability_compat: `{config.get('pull_probability', '')}`",
        "",
        "## Two-Stage Matching Rules",
        "",
        f"- coarse_match: `{rules.get('coarse_match', '')}`",
        f"- full_match: `{rules.get('full_match', '')}`",
        f"- specialization_rule: `{rules.get('specialization_rule', '')}`",
        f"- include_requester_in_match_counts: `{rules.get('include_requester_in_match_counts', '')}`",
        "",
        "## Funnel (5 Layers)",
        "",
        f"- visible_total: `{funnel.get('visible_total', 0)}`",
        f"- coarse_match_total: `{funnel.get('coarse_match_total', 0)}`",
        f"- full_match_total: `{funnel.get('full_match_total', 0)}`",
        f"- actual_pull_total: `{funnel.get('actual_pull_total', 0)}`",
        f"- actual_response_total: `{funnel.get('actual_response_total', 0)}`",
        "",
        "## Core Metrics",
        "",
        f"- visible_count mean/median/p95: `{metrics.get('visible_count', {}).get('avg', 0)}` / `{metrics.get('visible_count', {}).get('median', 0)}` / `{metrics.get('visible_count', {}).get('p95', 0)}`",
        f"- coarse_match_count mean/median/p95: `{metrics.get('coarse_match_count', {}).get('avg', 0)}` / `{metrics.get('coarse_match_count', {}).get('median', 0)}` / `{metrics.get('coarse_match_count', {}).get('p95', 0)}`",
        f"- full_match_count mean/median/p95: `{metrics.get('full_match_count', {}).get('avg', 0)}` / `{metrics.get('full_match_count', {}).get('median', 0)}` / `{metrics.get('full_match_count', {}).get('p95', 0)}`",
        f"- actual_pull_count mean/median/p95: `{metrics.get('actual_pull_count', {}).get('avg', 0)}` / `{metrics.get('actual_pull_count', {}).get('median', 0)}` / `{metrics.get('actual_pull_count', {}).get('p95', 0)}`",
        f"- actual_response_count mean/median/p95: `{metrics.get('actual_response_count', {}).get('avg', 0)}` / `{metrics.get('actual_response_count', {}).get('median', 0)}` / `{metrics.get('actual_response_count', {}).get('p95', 0)}`",
        f"- p_coarse mean/median/p95: `{metrics.get('p_coarse', {}).get('avg', 0)}` / `{metrics.get('p_coarse', {}).get('median', 0)}` / `{metrics.get('p_coarse', {}).get('p95', 0)}`",
        f"- p_full mean/median/p95: `{metrics.get('p_full', {}).get('avg', 0)}` / `{metrics.get('p_full', {}).get('median', 0)}` / `{metrics.get('p_full', {}).get('p95', 0)}`",
        f"- p_pull mean/median/p95: `{metrics.get('p_pull', {}).get('avg', 0)}` / `{metrics.get('p_pull', {}).get('median', 0)}` / `{metrics.get('p_pull', {}).get('p95', 0)}`",
        f"- p_resp mean/median/p95: `{metrics.get('p_resp', {}).get('avg', 0)}` / `{metrics.get('p_resp', {}).get('median', 0)}` / `{metrics.get('p_resp', {}).get('p95', 0)}`",
        f"- coarse_to_full mean/median/p95: `{metrics.get('coarse_to_full', {}).get('avg', 0)}` / `{metrics.get('coarse_to_full', {}).get('median', 0)}` / `{metrics.get('coarse_to_full', {}).get('p95', 0)}`",
        f"- pull_efficiency mean/median/p95: `{metrics.get('pull_efficiency', {}).get('avg', 0)}` / `{metrics.get('pull_efficiency', {}).get('median', 0)}` / `{metrics.get('pull_efficiency', {}).get('p95', 0)}`",
        f"- full_match_available_rate: `{summary.get('full_match_available_rate', 0)}`",
        f"- zero_full_match_rate: `{summary.get('zero_full_match_rate', 0)}`",
        f"- zero_responder_rate: `{summary.get('zero_responder_rate', 0)}`",
        f"- first_response_available_rate: `{summary.get('first_response_available_rate', 0)}`",
        "",
        "## Label Distribution",
        "",
        "| label | node_slots | request_count |",
        "| --- | --- | --- |",
    ]
    labels = sorted(set(node_distribution.keys()) | set(request_distribution.keys()))
    for label in labels:
        lines.append(f"| {label} | {node_distribution.get(label, 0)} | {request_distribution.get(label, 0)} |")
    lines.append("")
    if partial_reasons:
        lines.append("## Partial / Failure Notes")
        lines.append("")
        for reason in partial_reasons:
            lines.append(f"- {reason}")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", required=True)
    args = parser.parse_args()

    run_root = Path(args.run_root).expanduser()
    logs_path = run_root / "logs" / "events.jsonl"
    timeline_path = run_root / "results" / "request_timeline.jsonl"
    node_caps_path = run_root / "results" / "node_capabilities.json"
    runtime_summary_path = run_root / "summaries" / "runtime_summary.json"
    config_snapshot_path = run_root / "configs" / "runtime_config.json"

    per_request_csv = run_root / "results" / "per_request_metrics.csv"
    label_funnel_csv = run_root / "results" / "funnel_by_request_capability.csv"
    plot_ready_dir = ensure_dir(run_root / "results" / "plot_ready")
    responding_distribution_csv = plot_ready_dir / "responding_count_distribution.csv"
    full_match_distribution_csv = plot_ready_dir / "full_match_count_distribution.csv"
    summary_json_path = run_root / "summaries" / "summary.json"
    summary_md_path = run_root / "summaries" / "summary.md"

    request_map, declaration_map, payload_map, request_meta = read_requests(run_root=run_root)
    timeline_map = {str(row.get("request_id") or ""): row for row in read_jsonl(timeline_path)}
    event_index = build_event_index(logs_path)
    node_caps_payload = load_json(node_caps_path) if node_caps_path.exists() else {}
    runtime_summary = load_json(runtime_summary_path) if runtime_summary_path.exists() else {}
    config_snapshot = load_json(config_snapshot_path) if config_snapshot_path.exists() else {}

    partial_reasons: List[str] = []
    if not request_map:
        partial_reasons.append("requests_missing_or_empty")
    if not logs_path.exists():
        partial_reasons.append("events_missing")
    if not node_caps_path.exists():
        partial_reasons.append("node_capabilities_missing")

    all_request_ids = sorted(set(request_map.keys()) | set(timeline_map.keys()) | set(event_index.keys()))
    per_request_rows: List[Dict[str, Any]] = []
    for request_id in all_request_ids:
        request_row = request_map.get(request_id, {})
        payload_row = payload_map.get(request_id, {})
        event_row = event_index.get(request_id, {})
        timeline_row = timeline_map.get(request_id, {})

        visible_nodes = event_row.get("visible_nodes", set())
        coarse_nodes = event_row.get("coarse_nodes", set())
        full_nodes = event_row.get("full_nodes", set())
        pulling_nodes = event_row.get("pulling_nodes", set())
        responding_nodes = event_row.get("responding_nodes", set())
        if not isinstance(visible_nodes, set):
            visible_nodes = set()
        if not isinstance(coarse_nodes, set):
            coarse_nodes = set()
        if not isinstance(full_nodes, set):
            full_nodes = set()
        if not isinstance(pulling_nodes, set):
            pulling_nodes = set()
        if not isinstance(responding_nodes, set):
            responding_nodes = set()

        visible_count = len(visible_nodes)
        coarse_match_count = len(coarse_nodes)
        full_match_count = len(full_nodes) if full_nodes else int(timeline_row.get("full_match_count", 0))
        actual_pull_count = len(pulling_nodes)
        actual_response_count = len(responding_nodes)
        if full_match_count == 0 and actual_response_count > 0:
            # Backward compatibility for old runs with no full_match event.
            full_match_count = actual_response_count

        first_visible_ts = event_row.get("first_visible_ts", timeline_row.get("first_visible_ts"))
        last_visible_ts = event_row.get("last_visible_ts", timeline_row.get("last_visible_ts"))
        first_pull_ts = event_row.get("first_pull_ts", timeline_row.get("first_pull_ts"))
        first_response_ts = event_row.get("first_response_ts", timeline_row.get("first_response_ts"))
        declaration_sent_ts = timeline_row.get("declaration_sent_ts", request_row.get("created_ts"))

        response_count_total = int(event_row.get("response_event_count_total", 0))
        if response_count_total == 0 and isinstance(timeline_row.get("response_count_total"), int):
            response_count_total = int(timeline_row["response_count_total"])

        p_coarse = safe_ratio(coarse_match_count, visible_count)
        p_full = safe_ratio(full_match_count, visible_count)
        p_pull = safe_ratio(actual_pull_count, visible_count)
        p_resp = safe_ratio(actual_response_count, visible_count)
        coarse_to_full = safe_ratio(full_match_count, max(1, coarse_match_count))
        pull_efficiency = safe_ratio(actual_response_count, max(1, actual_pull_count))

        per_request_rows.append(
            {
                "request_id": request_id,
                "repetition": request_row.get("repetition", timeline_row.get("repetition")),
                "requester_id": request_row.get("requester_id", timeline_row.get("requester_id")),
                "required_family": request_row.get("required_family"),
                "coarse_domain": request_row.get("coarse_domain"),
                "required_capability": request_row.get("required_capability"),
                "required_tools": payload_row.get("required_tools", request_row.get("required_tools")),
                "required_trust_domain": payload_row.get(
                    "required_trust_domain",
                    request_row.get("required_trust_domain"),
                ),
                "max_latency_tier": payload_row.get("max_latency_tier", request_row.get("max_latency_tier")),
                "optional_specialization_preference": payload_row.get(
                    "optional_specialization_preference",
                    request_row.get("optional_specialization_preference"),
                ),
                "payload_id": request_row.get("payload_id", request_row.get("payload_ref")),
                "payload_ref": request_row.get("payload_ref", request_row.get("payload_id")),
                "payload_bytes": request_row.get("payload_bytes", timeline_row.get("payload_bytes")),
                "created_ts": request_row.get("created_ts"),
                "declaration_sent_ts": declaration_sent_ts,
                "first_visible_ts": first_visible_ts,
                "last_visible_ts": last_visible_ts,
                "first_pull_ts": first_pull_ts,
                "first_response_ts": first_response_ts,
                "response_count_total": response_count_total,
                "visible_count": visible_count,
                "coarse_match_count": coarse_match_count,
                "full_match_count": full_match_count,
                "actual_pull_count": actual_pull_count,
                "actual_response_count": actual_response_count,
                "eligible_count": coarse_match_count,
                "pulling_count": actual_pull_count,
                "responding_count": actual_response_count,
                "p_coarse": round(p_coarse, 6),
                "p_full": round(p_full, 6),
                "p_pull": round(p_pull, 6),
                "p_resp": round(p_resp, 6),
                "coarse_to_full": round(coarse_to_full, 6),
                "pull_efficiency": round(pull_efficiency, 6),
                "p_elig": round(p_coarse, 6),
                "has_full_match": full_match_count > 0,
                "has_response": actual_response_count > 0,
                "request_fail_reason": timeline_row.get("request_fail_reason", ""),
            }
        )

    write_csv(per_request_csv, per_request_rows)
    write_csv(label_funnel_csv, aggregate_request_label_rows(per_request_rows))

    visible_values = [int(row["visible_count"]) for row in per_request_rows]
    coarse_values = [int(row["coarse_match_count"]) for row in per_request_rows]
    full_values = [int(row["full_match_count"]) for row in per_request_rows]
    actual_pull_values = [int(row["actual_pull_count"]) for row in per_request_rows]
    actual_response_values = [int(row["actual_response_count"]) for row in per_request_rows]
    p_coarse_values = [float(row["p_coarse"]) for row in per_request_rows]
    p_full_values = [float(row["p_full"]) for row in per_request_rows]
    p_pull_values = [float(row["p_pull"]) for row in per_request_rows]
    p_resp_values = [float(row["p_resp"]) for row in per_request_rows]
    coarse_to_full_values = [float(row["coarse_to_full"]) for row in per_request_rows]
    pull_eff_values = [float(row["pull_efficiency"]) for row in per_request_rows]

    write_csv(responding_distribution_csv, distribution_rows(actual_response_values))
    write_csv(full_match_distribution_csv, distribution_rows(full_values))

    request_label_distribution: Dict[str, int] = {}
    for row in per_request_rows:
        label = str(row.get("required_family") or row.get("required_capability") or "unknown")
        request_label_distribution[label] = request_label_distribution.get(label, 0) + 1
    request_label_distribution = dict(sorted(request_label_distribution.items()))

    node_label_distribution: Dict[str, int] = {}
    node_caps_map = node_caps_payload.get("node_capabilities", {})
    if isinstance(node_caps_map, dict):
        for value in node_caps_map.values():
            if isinstance(value, list):
                for token in value:
                    label = str(token)
                    node_label_distribution[label] = node_label_distribution.get(label, 0) + 1
            elif isinstance(value, dict):
                families = value.get("families", [])
                if isinstance(families, list):
                    for token in families:
                        label = str(token)
                        node_label_distribution[label] = node_label_distribution.get(label, 0) + 1
    node_label_distribution = dict(sorted(node_label_distribution.items()))

    matching_mode = str(
        config_snapshot.get("matching_mode")
        or runtime_summary.get("matching_mode")
        or (TWO_STAGE_MODE if request_meta.get("has_two_stage_requests") else "legacy_simple")
    )
    specialization_hard = bool(config_snapshot.get("specialization_hard", True))
    include_requester = not bool(config_snapshot.get("exclude_requester_from_match_count", True))

    summary = {
        "generated_at": now_iso(),
        "run_root": str(run_root),
        "node_count": config_snapshot.get("node_count", runtime_summary.get("node_count")),
        "request_count": len(per_request_rows),
        "repetitions": config_snapshot.get("repetitions", runtime_summary.get("repetitions")),
        "random_seed": config_snapshot.get("random_seed", runtime_summary.get("random_seed")),
        "matching_mode": matching_mode,
        "config": {
            "variant": config_snapshot.get("variant", runtime_summary.get("variant")),
            "matching_mode": matching_mode,
            "global_topic": config_snapshot.get("global_topic", runtime_summary.get("global_topic")),
            "pull_policy": config_snapshot.get("pull_policy", runtime_summary.get("pull_policy")),
            "pull_probability": config_snapshot.get("pull_probability", runtime_summary.get("pull_probability")),
            "constraint_mode": config_snapshot.get("constraint_mode", runtime_summary.get("constraint_mode", "medium")),
            "specialization_hard": specialization_hard,
            "exclude_requester_from_match_count": bool(
                config_snapshot.get("exclude_requester_from_match_count", True)
            ),
        },
        "matching_rules": {
            "coarse_match": "required_family in node.families AND coarse_domain in node.domains",
            "full_match": "required_tools subset of node.tools AND trust_domain equal AND node.latency_tier <= max_latency_tier",
            "specialization_rule": "hard_constraint" if specialization_hard else "soft_flag",
            "include_requester_in_match_counts": include_requester,
        },
        "metrics": {
            "visible_count": stats(visible_values),
            "coarse_match_count": stats(coarse_values),
            "full_match_count": stats(full_values),
            "actual_pull_count": stats(actual_pull_values),
            "actual_response_count": stats(actual_response_values),
            "eligible_count": stats(coarse_values),
            "pulling_count": stats(actual_pull_values),
            "responding_count": stats(actual_response_values),
            "p_coarse": stats(p_coarse_values),
            "p_full": stats(p_full_values),
            "p_pull": stats(p_pull_values),
            "p_resp": stats(p_resp_values),
            "coarse_to_full": stats(coarse_to_full_values),
            "pull_efficiency": stats(pull_eff_values),
            "p_elig": stats(p_coarse_values),
        },
        "funnel_overall": {
            "visible_total": int(sum(visible_values)),
            "coarse_match_total": int(sum(coarse_values)),
            "full_match_total": int(sum(full_values)),
            "actual_pull_total": int(sum(actual_pull_values)),
            "actual_response_total": int(sum(actual_response_values)),
            "eligible_total": int(sum(coarse_values)),
            "pulling_total": int(sum(actual_pull_values)),
            "responding_total": int(sum(actual_response_values)),
            "coarse_over_visible": round(safe_ratio(int(sum(coarse_values)), int(sum(visible_values))), 6),
            "full_over_visible": round(safe_ratio(int(sum(full_values)), int(sum(visible_values))), 6),
            "actual_pull_over_visible": round(
                safe_ratio(int(sum(actual_pull_values)), int(sum(visible_values))),
                6,
            ),
            "actual_response_over_visible": round(
                safe_ratio(int(sum(actual_response_values)), int(sum(visible_values))),
                6,
            ),
            "eligible_over_visible": round(safe_ratio(int(sum(coarse_values)), int(sum(visible_values))), 6),
            "pulling_over_visible": round(
                safe_ratio(int(sum(actual_pull_values)), int(sum(visible_values))),
                6,
            ),
            "responding_over_visible": round(
                safe_ratio(int(sum(actual_response_values)), int(sum(visible_values))),
                6,
            ),
        },
        "zero_responder_rate": round(
            safe_ratio(sum(1 for value in actual_response_values if value == 0), len(actual_response_values)),
            6,
        ),
        "full_match_available_rate": round(
            safe_ratio(sum(1 for value in full_values if value > 0), len(full_values)),
            6,
        ),
        "zero_full_match_rate": round(
            safe_ratio(sum(1 for value in full_values if value == 0), len(full_values)),
            6,
        ),
        "first_response_available_rate": round(
            safe_ratio(sum(1 for value in actual_response_values if value > 0), len(actual_response_values)),
            6,
        ),
        "node_label_distribution": node_label_distribution,
        "request_label_distribution": request_label_distribution,
        "runtime_summary_ref": runtime_summary,
        "partial_reasons": partial_reasons,
        "outputs": {
            "node_capabilities_json": str(node_caps_path),
            "requests_jsonl": request_meta["requests_path"],
            "requests_declaration_jsonl": request_meta["requests_declaration_path"],
            "requests_payload_jsonl": request_meta["requests_payload_path"],
            "events_jsonl": str(logs_path),
            "per_request_metrics_csv": str(per_request_csv),
            "summary_json": str(summary_json_path),
            "summary_md": str(summary_md_path),
            "funnel_by_request_capability_csv": str(label_funnel_csv),
            "responding_count_distribution_csv": str(responding_distribution_csv),
            "full_match_count_distribution_csv": str(full_match_distribution_csv),
        },
    }

    dump_json(summary_json_path, summary)
    summary_md_path.write_text(build_summary_markdown(summary) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
