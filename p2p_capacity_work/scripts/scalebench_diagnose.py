#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def stats(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    if not ordered:
        return {}
    idx = max(0, min(len(ordered) - 1, int(len(ordered) * 0.95) - 1))
    return {
        "p50": round(statistics.median(ordered), 2),
        "p95": round(ordered[idx], 2),
        "max": round(ordered[-1], 2),
    }


def summarize_first_seen(path: Path) -> list[dict[str, Any]]:
    rows = list(csv.DictReader(path.open()))
    by_round: dict[int, list[dict[str, str]]] = {}
    for row in rows:
        by_round.setdefault(int(row["round_index"]), []).append(row)

    summaries: list[dict[str, Any]] = []
    for round_idx, items in sorted(by_round.items()):
        target_lat = [
            float(row["first_seen_ms"])
            for row in items
            if row["target"] == "True" and row["seen"] == "True" and row["first_seen_ms"]
        ]
        spill_lat = [
            float(row["first_seen_ms"])
            for row in items
            if row["target"] == "False" and row["seen"] == "True" and row["first_seen_ms"]
        ]
        summaries.append(
            {
                "round_index": round_idx,
                "target_seen": sum(1 for row in items if row["target"] == "True" and row["seen"] == "True"),
                "spill_seen": sum(1 for row in items if row["target"] == "False" and row["seen"] == "True"),
                "target_latency": stats(target_lat),
                "spill_latency": stats(spill_lat),
            }
        )
    return summaries


def summarize_topology(sample_dir: Path, pubsub_dir: Path) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for path in sorted(sample_dir.glob("final_*.json")):
        payload = load_json(path)
        pubsub_payload = load_json(pubsub_dir / path.name) if (pubsub_dir / path.name).exists() else {}
        same_topic = payload.get("same_topic_connected_counts") or {}
        by_topic = payload.get("active_neighbors_by_topic") or {}
        topic_mesh = pubsub_payload.get("topic_mesh") or {}
        pubsub_topics = list(by_topic)
        weak_topics = []
        for topic, count in same_topic.items():
            mesh_count = len(topic_mesh.get(topic) or [])
            if mesh_count < int(payload.get("same_topic_mesh_min") or 0):
                weak_topics.append(topic)
        summaries.append(
            {
                "node": path.stem.replace("final_", ""),
                "active_neighbors": len(payload.get("active_neighbors") or []),
                "active_neighbor_target": payload.get("active_neighbor_target"),
                "active_neighbor_hard_cap": payload.get("active_neighbor_hard_cap"),
                "same_topic_connected_counts": same_topic,
                "topic_neighbor_counts": {topic: len(peers or []) for topic, peers in by_topic.items()},
                "topic_mesh_counts": {topic: len(topic_mesh.get(topic) or []) for topic in topic_mesh},
                "topics": pubsub_topics,
                "weak_topics": weak_topics,
            }
        )
    return summaries


def summarize_pubsub(sample_dir: Path) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for path in sorted(sample_dir.glob("final_*.json")):
        payload = load_json(path)
        topic_peers = payload.get("topic_peers") or {}
        topic_mesh = payload.get("topic_mesh") or {}
        summaries.append(
            {
                "node": path.stem.replace("final_", ""),
                "topics": {
                    topic: {
                        "peers": len(peers or []),
                        "mesh": len(topic_mesh.get(topic) or []),
                    }
                    for topic, peers in topic_peers.items()
                },
            }
        )
    return summaries


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-dir", required=True, help="bench_shards_* directory")
    args = parser.parse_args()

    result_dir = Path(args.result_dir).resolve()
    summary = load_json(result_dir / "summary.json")
    first_seen = summarize_first_seen(result_dir / "first_seen_matrix.csv")
    topology = summarize_topology(result_dir / "sample_topology", result_dir / "sample_pubsub")
    pubsub = summarize_pubsub(result_dir / "sample_pubsub")

    weak_nodes = [row for row in topology if row["weak_topics"]]
    print("# Scalebench Diagnosis")
    print()
    print(json.dumps(
        {
            "node_count": summary["node_count"],
            "target_cover100_ms_p95": summary["target_cover100_ms"].get("p95"),
            "global_cover100_ms_p95": summary["global_cover100_ms"].get("p95"),
            "host_cpu_busy_pct_p95": summary["host_cpu_busy_pct"].get("p95"),
            "docker_cpu_pct_sum_p95": summary["docker_cpu_pct_sum"].get("p95"),
            "load1_p95": summary["load1"].get("p95"),
        },
        ensure_ascii=False,
        indent=2,
    ))
    print()
    print("## Per Round")
    for row in first_seen:
        print(json.dumps(row, ensure_ascii=False))
    print()
    print("## Final Topology")
    for row in topology:
        print(json.dumps(row, ensure_ascii=False))
    print()
    print("## Final PubSub")
    for row in pubsub:
        print(json.dumps(row, ensure_ascii=False))
    print()
    print("## Weak Nodes")
    for row in weak_nodes:
        print(json.dumps(row, ensure_ascii=False))


if __name__ == "__main__":
    main()
