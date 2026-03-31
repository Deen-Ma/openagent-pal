from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "scalebench_capacity_lib.py"
SPEC = importlib.util.spec_from_file_location("scalebench_capacity_lib", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def make_summary(
    *,
    node_count: int = 200,
    round_count: int = 5,
    e2e_p95: float = 1500.0,
    cpu_p95: float = 75.0,
    seen_ratio_global: float = 1.0,
    launch_ok: bool = True,
    health_ok: bool = True,
    bench_ok: bool = True,
    tcp_per_node_avg: float = 20.0,
    peer_per_node_p50: float = 8.0,
    mesh_per_node_p50: float = 4.0,
) -> dict:
    return {
        "node_count": node_count,
        "round_count": round_count,
        "expected_round_count": round_count,
        "launch_ok": launch_ok,
        "health_ok": health_ok,
        "bench_ok": bench_ok,
        "e2e_latency_ms": {"count": round_count, "p95": e2e_p95},
        "seen_ratio_global": {"count": round_count, "median": seen_ratio_global},
        "host_cpu_busy_pct": {"count": round_count, "p95": cpu_p95},
        "tcp_per_node_avg": tcp_per_node_avg,
        "peer_per_node_p50": peer_per_node_p50,
        "mesh_per_node_p50": mesh_per_node_p50,
        "neighbor_hard_cap": 8,
        "bootstrap_connect_count": 2,
    }


def test_evaluate_single_run_passes_within_guardrail() -> None:
    previous = MODULE.build_reference(make_summary(e2e_p95=1200.0))
    current = make_summary(e2e_p95=1499.0, cpu_p95=82.0)
    result = MODULE.evaluate_single_run(current, previous)
    assert result["passed"] is True
    assert result["status"] == "pass"


def test_evaluate_single_run_marks_cpu_limited_when_guardrail_breaks_under_high_cpu() -> None:
    previous = MODULE.build_reference(make_summary(e2e_p95=1200.0))
    current = make_summary(e2e_p95=1700.0, cpu_p95=93.0)
    result = MODULE.evaluate_single_run(current, previous)
    assert result["passed"] is False
    assert result["status"] == "cpu_limited"
    assert result["cpu_limited"] is True


def test_evaluate_single_run_marks_platform_ceiling_for_incomplete_global_coverage() -> None:
    previous = MODULE.build_reference(make_summary(e2e_p95=1200.0))
    current = make_summary(e2e_p95=1600.0, seen_ratio_global=0.95)
    result = MODULE.evaluate_single_run(current, previous)
    assert result["passed"] is False
    assert result["status"] == "platform_ceiling"
    assert "seen_ratio_global_not_full" in result["functional_reasons"]


def test_evaluate_repetition_group_requires_two_of_three_passes() -> None:
    previous = MODULE.build_reference(make_summary(e2e_p95=1200.0))
    summaries = [
        make_summary(e2e_p95=1400.0, cpu_p95=80.0),
        make_summary(e2e_p95=1410.0, cpu_p95=81.0),
        make_summary(e2e_p95=1700.0, cpu_p95=88.0),
    ]
    result = MODULE.evaluate_repetition_group(summaries, previous)
    assert result["passed"] is True
    assert result["status"] == "pass"
    assert result["pass_count"] == 2


def test_select_fine_sizes_returns_full_twenty_step_window() -> None:
    assert MODULE.select_fine_sizes(500, 600) == [520, 540, 560, 580, 600]


def test_seed_count_table() -> None:
    assert MODULE.seed_count_for_nodes(20) == 3
    assert MODULE.seed_count_for_nodes(40) == 3
    assert MODULE.seed_count_for_nodes(60) == 3
    assert MODULE.seed_count_for_nodes(100) == 4
    assert MODULE.seed_count_for_nodes(180) == 5
    assert MODULE.seed_count_for_nodes(200) == 5


def test_connection_scaling_alert_triggered_by_peer_density() -> None:
    previous = MODULE.build_reference(make_summary(e2e_p95=1200.0, tcp_per_node_avg=10.0, peer_per_node_p50=8.0))
    current = make_summary(e2e_p95=1400.0, tcp_per_node_avg=16.0, peer_per_node_p50=14.0)
    result = MODULE.evaluate_single_run(current, previous)
    assert result["connection_scaling_alert"] is True
    assert "peer_density_above_limit" in result["connection_scaling_reasons"]
