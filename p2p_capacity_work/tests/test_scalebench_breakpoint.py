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


COLLECT = load_module("scalebench_collect_events", "scripts/scalebench_collect_events.py")
AGGREGATE = load_module("scalebench_aggregate_breakpoint", "scripts/scalebench_aggregate_breakpoint.py")


def test_collect_events_aggregates_protocol_and_queue_metrics(tmp_path: Path) -> None:
    node_dir = tmp_path / "node8500" / "state"
    node_dir.mkdir(parents=True)
    events = [
        {"type": "task_received", "applied": True},
        {"type": "task_received", "applied": True},
        {"type": "sae_duplicate"},
        {"type": "fetch_request_end"},
        {"type": "fetch_response_end", "applied": True},
        {"type": "fetch_response_end", "applied": False, "reason": "timeout waiting for peer"},
        {"type": "fetch_retry_scheduled"},
        {"type": "queue_enqueue", "queue_depth": 3},
        {"type": "queue_dequeue", "queue_depth": 1, "queue_wait_ms": 12},
        {"type": "heartbeat_round_end", "duration_ms": 8},
        {"type": "fetch_response_end", "applied": False, "error_kind": "network"},
    ]
    (node_dir / "events.jsonl").write_text("\n".join(json.dumps(item) for item in events))

    payload = COLLECT.aggregate(tmp_path)
    assert payload["node_count"] == 1
    assert payload["header_recv_avg"] == 2.0
    assert payload["duplicate_header_avg"] == 1.0
    assert payload["pull_req_avg"] == 1.0
    assert payload["pull_success_rate"] == 1.0
    assert payload["timeout_cnt"] == 1
    assert payload["retry_cnt"] == 1
    assert payload["queue_avg"] == 2.0
    assert payload["queue_max"] == 3
    assert payload["heartbeat_p95"] == 8.0
    assert payload["error_event_cnt"] == 1


def test_summarize_breakpoint_flags_growth_interval() -> None:
    rows = [
        {"nodes": 40, "run_id": "run01", "e2e_p99": 100.0, "tcp_per_node": 10.0, "peer_p50": 7.0, "mesh_p50": 4.0, "cpu_p95": 20.0},
        {"nodes": 40, "run_id": "run02", "e2e_p99": 110.0, "tcp_per_node": 11.0, "peer_p50": 7.0, "mesh_p50": 4.0, "cpu_p95": 22.0},
        {"nodes": 60, "run_id": "run01", "e2e_p99": 220.0, "tcp_per_node": 18.0, "peer_p50": 12.0, "mesh_p50": 8.0, "cpu_p95": 40.0},
        {"nodes": 60, "run_id": "run02", "e2e_p99": 240.0, "tcp_per_node": 19.0, "peer_p50": 13.0, "mesh_p50": 8.0, "cpu_p95": 42.0},
    ]
    payload = AGGREGATE.summarize_breakpoint(rows)
    assert payload["breakpoint_interval"] == [40, 60]
    assert payload["trend"][0]["nodes"] == 40
    assert payload["trend"][1]["nodes"] == 60
