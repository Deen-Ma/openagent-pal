#!/usr/bin/env python3
from __future__ import annotations

import statistics
from typing import Any


PUBLIC_WORST_CASE_VARIANT = "ablation_no_semantic_public_topic"
GLOBAL_TOPIC = "openagent/v1/general"


def seed_count_for_nodes(node_count: int) -> int:
    if node_count <= 80:
        return 3
    if node_count <= 160:
        return 4
    return 5


def metric(summary: dict[str, Any], key: str, stat: str = "median") -> float | int | None:
    block = summary.get(key) or {}
    if not isinstance(block, dict):
        return None
    value = block.get(stat)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value
    return None


def median_or_none(values: list[float | int | None], digits: int = 2) -> float | None:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return None
    return round(float(statistics.median(cleaned)), digits)


def e2e_p95(summary: dict[str, Any]) -> float | int | None:
    return (
        metric(summary, "e2e_latency_ms", "p95")
        or metric(summary, "e2e_cover95_ms", "p95")
        or metric(summary, "global_cover100_ms", "p95")
    )


def build_reference(summary: dict[str, Any]) -> dict[str, float | int | None]:
    return {
        "size": summary.get("node_count"),
        "e2e_latency_ms_p95": e2e_p95(summary),
        "host_cpu_busy_pct_p95": metric(summary, "host_cpu_busy_pct", "p95"),
        "tcp_per_node_avg": summary.get("tcp_per_node_avg"),
        "peer_per_node_p50": summary.get("peer_per_node_p50") or metric(summary, "peer_count", "p50"),
        "mesh_per_node_p50": summary.get("mesh_per_node_p50")
        or metric(summary, "general_topic_mesh_size", "p50"),
    }


def merge_references(summaries: list[dict[str, Any]]) -> dict[str, float | int | None]:
    return {
        "size": summaries[0].get("node_count") if summaries else None,
        "e2e_latency_ms_p95": median_or_none([e2e_p95(summary) for summary in summaries]),
        "host_cpu_busy_pct_p95": median_or_none(
            [metric(summary, "host_cpu_busy_pct", "p95") for summary in summaries]
        ),
        "tcp_per_node_avg": median_or_none([summary.get("tcp_per_node_avg") for summary in summaries]),
        "peer_per_node_p50": median_or_none(
            [summary.get("peer_per_node_p50") or metric(summary, "peer_count", "p50") for summary in summaries]
        ),
        "mesh_per_node_p50": median_or_none(
            [
                summary.get("mesh_per_node_p50")
                or metric(summary, "general_topic_mesh_size", "p50")
                for summary in summaries
            ]
        ),
    }


def functional_success(
    summary: dict[str, Any],
    variant: str = PUBLIC_WORST_CASE_VARIANT,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []

    if summary.get("launch_ok") is False:
        reasons.append("launch_not_ok")
    if summary.get("health_ok") is False:
        reasons.append("health_not_ok")
    if summary.get("bench_ok") is False:
        reasons.append("bench_not_ok")

    round_count = int(summary.get("round_count") or 0)
    expected_round_count = int(summary.get("expected_round_count") or round_count)
    if round_count != expected_round_count:
        reasons.append("round_count_mismatch")

    e2e_round_count = metric(summary, "e2e_cover95_ms", "count") or metric(summary, "e2e_cover99_ms", "count")
    if e2e_round_count is not None and int(e2e_round_count) != round_count:
        reasons.append("e2e_latency_incomplete")
    elif int(metric(summary, "e2e_latency_ms", "count") or 0) <= 0:
        reasons.append("e2e_latency_incomplete")

    if variant == PUBLIC_WORST_CASE_VARIANT:
        seen_ratio_global = metric(summary, "seen_ratio_global", "median") or 1.0
        if float(seen_ratio_global) != 1.0:
            reasons.append("seen_ratio_global_not_full")

    return (not reasons, reasons)


def evaluate_single_run(
    summary: dict[str, Any],
    previous_reference: dict[str, Any] | None,
    *,
    guardrail_ratio: float = 1.25,
    cpu_busy_threshold: float = 90.0,
    variant: str = PUBLIC_WORST_CASE_VARIANT,
) -> dict[str, Any]:
    current_e2e_p95 = e2e_p95(summary)
    current_cpu_p95 = metric(summary, "host_cpu_busy_pct", "p95")
    current_tcp_per_node = summary.get("tcp_per_node_avg")
    current_peer_p50 = summary.get("peer_per_node_p50") or metric(summary, "peer_count", "p50")
    current_mesh_p50 = summary.get("mesh_per_node_p50") or metric(summary, "general_topic_mesh_size", "p50")

    functional_ok, functional_reasons = functional_success(summary, variant=variant)
    connection_reasons: list[str] = []
    evaluation = {
        "passed": False,
        "status": "unknown",
        "reason": "",
        "functional_ok": functional_ok,
        "functional_reasons": functional_reasons,
        "guardrail_ok": False,
        "latency_guardrail_failures": [],
        "cpu_limited": False,
        "non_cpu_ceiling": False,
        "platform_ceiling": False,
        "connection_scaling_alert": False,
        "connection_scaling_reasons": connection_reasons,
        "current_e2e_latency_ms_p95": current_e2e_p95,
        "current_host_cpu_busy_pct_p95": current_cpu_p95,
        "current_tcp_per_node_avg": current_tcp_per_node,
        "current_peer_per_node_p50": current_peer_p50,
        "current_mesh_per_node_p50": current_mesh_p50,
        "reference": previous_reference,
    }

    if not functional_ok:
        evaluation.update(
            {
                "status": "platform_ceiling",
                "reason": ",".join(functional_reasons) or "functional_failure",
                "platform_ceiling": True,
            }
        )
        return evaluation

    peer_limit = None
    if summary.get("neighbor_hard_cap") is not None and summary.get("bootstrap_connect_count") is not None:
        peer_limit = float(summary["neighbor_hard_cap"]) + float(summary["bootstrap_connect_count"]) + 2.0
        if current_peer_p50 is not None and float(current_peer_p50) > peer_limit:
            connection_reasons.append("peer_density_above_limit")

    if previous_reference is not None:
        previous_tcp_per_node = previous_reference.get("tcp_per_node_avg")
        if previous_tcp_per_node is not None and current_tcp_per_node is not None:
            if float(current_tcp_per_node) > float(previous_tcp_per_node) * 1.5:
                connection_reasons.append("tcp_per_node_growth_over_50pct")

    if connection_reasons:
        evaluation["connection_scaling_alert"] = True

    if previous_reference is None:
        evaluation.update(
            {
                "passed": True,
                "status": "baseline_pass",
                "reason": "baseline_without_prior_guardrail",
                "guardrail_ok": True,
            }
        )
        return evaluation

    failures: list[str] = []
    previous_e2e_p95 = previous_reference.get("e2e_latency_ms_p95")
    if previous_e2e_p95 is None or current_e2e_p95 is None:
        failures.append("e2e_latency_ms_p95_missing")
    elif float(current_e2e_p95) > float(previous_e2e_p95) * guardrail_ratio:
        failures.append("e2e_latency_ms_p95_guardrail")

    if not failures:
        evaluation.update(
            {
                "passed": True,
                "status": "pass",
                "reason": "guardrail_pass",
                "guardrail_ok": True,
            }
        )
        return evaluation

    cpu_limited = current_cpu_p95 is not None and float(current_cpu_p95) >= cpu_busy_threshold
    evaluation.update(
        {
            "status": "cpu_limited" if cpu_limited else "non_cpu_ceiling",
            "reason": ",".join(failures),
            "guardrail_ok": False,
            "latency_guardrail_failures": failures,
            "cpu_limited": cpu_limited,
            "non_cpu_ceiling": not cpu_limited,
        }
    )
    return evaluation


def evaluate_repetition_group(
    summaries: list[dict[str, Any]],
    previous_reference: dict[str, Any] | None,
    *,
    guardrail_ratio: float = 1.25,
    cpu_busy_threshold: float = 90.0,
    pass_quorum: int = 2,
    cpu_quorum: int = 2,
    variant: str = PUBLIC_WORST_CASE_VARIANT,
) -> dict[str, Any]:
    evaluations = [
        evaluate_single_run(
            summary,
            previous_reference,
            guardrail_ratio=guardrail_ratio,
            cpu_busy_threshold=cpu_busy_threshold,
            variant=variant,
        )
        for summary in summaries
    ]
    pass_count = sum(1 for evaluation in evaluations if evaluation["passed"])
    cpu_limited_votes = sum(1 for evaluation in evaluations if evaluation["cpu_limited"])
    connection_alert_votes = sum(1 for evaluation in evaluations if evaluation["connection_scaling_alert"])
    if len(summaries) != len(evaluations):
        raise ValueError("summaries and evaluations length mismatch")
    passed_summaries = [
        summary
        for summary, evaluation in zip(summaries, evaluations)
        if evaluation["passed"]
    ]
    reference = merge_references(passed_summaries or summaries)

    group_pass = pass_count >= pass_quorum
    if group_pass:
        status = "pass"
        reason = f"pass_quorum_{pass_count}_of_{len(summaries)}"
    elif cpu_limited_votes >= cpu_quorum:
        status = "cpu_limited"
        reason = f"cpu_quorum_{cpu_limited_votes}_of_{len(summaries)}"
    else:
        platform_votes = sum(1 for evaluation in evaluations if evaluation["platform_ceiling"])
        if platform_votes:
            status = "platform_ceiling"
            reason = f"platform_failure_{platform_votes}_of_{len(summaries)}"
        else:
            status = "non_cpu_ceiling"
            reason = f"guardrail_failure_{len(summaries) - pass_count}_of_{len(summaries)}"

    return {
        "passed": group_pass,
        "status": status,
        "reason": reason,
        "pass_count": pass_count,
        "cpu_limited_votes": cpu_limited_votes,
        "connection_alert_votes": connection_alert_votes,
        "platform_failure_votes": sum(1 for evaluation in evaluations if evaluation["platform_ceiling"]),
        "evaluations": evaluations,
        "reference_after": reference,
    }


def select_fine_sizes(stable_size: int | None, failing_size: int | None, *, step: int = 20) -> list[int]:
    if failing_size is None:
        return []
    if stable_size is None:
        return [failing_size]
    if failing_size <= stable_size:
        return [failing_size]
    return list(range(stable_size + step, failing_size + 1, step))
