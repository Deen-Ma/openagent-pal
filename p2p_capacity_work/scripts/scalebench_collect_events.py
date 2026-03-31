#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_events(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    if not path.exists():
        return events
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def stats(values: list[float | int]) -> dict[str, float | int] | None:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    idx95 = max(0, min(len(ordered) - 1, int(len(ordered) * 0.95) - 1))
    return {
        "count": len(ordered),
        "avg": round(sum(ordered) / len(ordered), 2),
        "max": round(ordered[-1], 2),
        "p95": round(ordered[idx95], 2),
    }


def aggregate_node(path: Path) -> dict[str, Any]:
    events = load_events(path / "state" / "events.jsonl")
    queue_depths = [int(event.get("queue_depth") or 0) for event in events if event.get("type") in {"queue_enqueue", "queue_dequeue"}]
    queue_waits = [int(event.get("queue_wait_ms") or 0) for event in events if event.get("type") == "queue_dequeue"]
    heartbeat_durations = [int(event.get("duration_ms") or 0) for event in events if event.get("type") == "heartbeat_round_end"]
    fetch_requests = [event for event in events if event.get("type") == "fetch_request_end"]
    fetch_responses = [event for event in events if event.get("type") == "fetch_response_end"]
    fetch_success = [event for event in fetch_responses if event.get("applied") is True]
    fetch_timeout = [
        event for event in fetch_responses
        if "timeout" in str(event.get("reason") or "").lower()
    ]
    return {
        "node": path.name,
        "header_recv": sum(1 for event in events if event.get("type") == "task_received" and event.get("applied") is True),
        "duplicate_header": sum(1 for event in events if event.get("type") == "sae_duplicate"),
        "pull_requests": len(fetch_requests),
        "pull_success": len(fetch_success),
        "timeout_count": len(fetch_timeout),
        "retry_count": sum(1 for event in events if event.get("type") == "fetch_retry_scheduled"),
        "queue_avg": round(sum(queue_depths) / len(queue_depths), 2) if queue_depths else 0.0,
        "queue_max": max(queue_depths) if queue_depths else 0,
        "queue_wait_p95": (stats(queue_waits) or {}).get("p95"),
        "heartbeat_p95": (stats(heartbeat_durations) or {}).get("p95"),
        "error_event_count": sum(1 for event in events if event.get("applied") is False and event.get("error_kind")),
    }


def aggregate(nodes_dir: Path) -> dict[str, Any]:
    node_rows = []
    for path in sorted(nodes_dir.glob("node*")):
        if path.is_dir():
            node_rows.append(aggregate_node(path))
    if not node_rows:
        return {"node_count": 0, "nodes": []}

    pull_requests = sum(int(row["pull_requests"]) for row in node_rows)
    pull_success = sum(int(row["pull_success"]) for row in node_rows)
    return {
        "node_count": len(node_rows),
        "nodes": node_rows,
        "header_recv_avg": round(sum(float(row["header_recv"]) for row in node_rows) / len(node_rows), 2),
        "duplicate_header_avg": round(sum(float(row["duplicate_header"]) for row in node_rows) / len(node_rows), 2),
        "pull_req_avg": round(sum(float(row["pull_requests"]) for row in node_rows) / len(node_rows), 2),
        "pull_success_rate": round(pull_success / max(1, pull_requests), 4),
        "timeout_cnt": sum(int(row["timeout_count"]) for row in node_rows),
        "retry_cnt": sum(int(row["retry_count"]) for row in node_rows),
        "queue_avg": round(sum(float(row["queue_avg"]) for row in node_rows) / len(node_rows), 2),
        "queue_max": max(int(row["queue_max"]) for row in node_rows),
        "heartbeat_p95": (stats([row["heartbeat_p95"] for row in node_rows if row["heartbeat_p95"] is not None]) or {}).get("p95"),
        "error_event_cnt": sum(int(row["error_event_count"]) for row in node_rows),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes-dir", required=True)
    parser.add_argument("--out-json", required=True)
    args = parser.parse_args()

    payload = aggregate(Path(args.nodes_dir).resolve())
    Path(args.out_json).resolve().write_text(json.dumps(payload, indent=2))
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
