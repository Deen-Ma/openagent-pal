from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, rel_path: str):
    module_path = ROOT / rel_path
    spec = importlib.util.spec_from_file_location(name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


MATRIX = load_module("scalebench_matrix", "scripts/scalebench_matrix.py")
COMPARE = load_module("scalebench_compare", "scripts/scalebench_compare.py")


def test_build_node_args_renders_profile_b_shape() -> None:
    args = MATRIX.build_node_args(
        {
            "neighbor_target": 10,
            "neighbor_hard_cap": 16,
            "same_topic_neighbor_min": 6,
            "same_topic_mesh_min": 6,
            "topic_replenish_batch": 2,
            "bootstrap_connect_count": 4,
        }
    )
    assert args == [
        "--neighbor-target", "10",
        "--neighbor-hard-cap", "16",
        "--same-topic-neighbor-min", "6",
        "--same-topic-mesh-min", "6",
        "--topic-replenish-batch", "2",
        "--bootstrap-connect-count", "4",
    ]


def test_ranking_prefers_functional_and_lower_latency(tmp_path: Path) -> None:
    result_dir = tmp_path / "bench"
    (result_dir / "sample_topology").mkdir(parents=True)
    (result_dir / "sample_pubsub").mkdir(parents=True)
    (result_dir / "sample_topology" / "final_8500.json").write_text(
        json.dumps(
            {
                "same_topic_connected_counts": {"openagent/v1/bench/shard-001": 12},
                "same_topic_mesh_min": 6,
            }
        )
    )
    (result_dir / "sample_pubsub" / "final_8500.json").write_text(
        json.dumps({"topic_mesh": {"openagent/v1/bench/shard-001": ["p1"] * 12}})
    )

    faster = {
        "profile": "fast",
        "functional_ok": True,
        "bench_ok": True,
        "passed": True,
        "target_cover100_ms_p95": 100.0,
        "global_cover100_ms_p95": 110.0,
        "weak_nodes": 0,
        "weak_topics": 0,
        "host_cpu_busy_pct_p95": 30.0,
    }
    slower = dict(faster)
    slower["profile"] = "slow"
    slower["target_cover100_ms_p95"] = 150.0
    broken = dict(faster)
    broken["profile"] = "broken"
    broken["functional_ok"] = False
    rows = sorted([slower, broken, faster], key=COMPARE.ranking_key)
    assert [row["profile"] for row in rows] == ["fast", "slow", "broken"]


def test_summarize_weak_nodes_counts_mesh_deficits(tmp_path: Path) -> None:
    result_dir = tmp_path / "bench"
    topo = result_dir / "sample_topology"
    pubsub = result_dir / "sample_pubsub"
    topo.mkdir(parents=True)
    pubsub.mkdir(parents=True)
    (topo / "final_8500.json").write_text(
        json.dumps(
            {
                "same_topic_connected_counts": {
                    "openagent/v1/bench/shard-001": 9,
                    "openagent/v1/bench/shard-002": 9,
                },
                "same_topic_mesh_min": 6,
            }
        )
    )
    (pubsub / "final_8500.json").write_text(
        json.dumps(
            {
                "topic_mesh": {
                    "openagent/v1/bench/shard-001": ["p1"] * 5,
                    "openagent/v1/bench/shard-002": ["p1"] * 6,
                }
            }
        )
    )
    assert COMPARE.summarize_weak_nodes(result_dir) == (1, 1)
