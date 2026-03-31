#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open() as handle:
        return list(csv.DictReader(handle))


def median_by_nodes(rows: list[dict[str, str]], key: str) -> tuple[list[int], list[float]]:
    grouped: dict[int, list[float]] = {}
    for row in rows:
        raw = row.get(key)
        if raw in (None, "", "None"):
            continue
        grouped.setdefault(int(row["nodes"]), []).append(float(raw))
    xs = sorted(grouped)
    ys = [sorted(grouped[x])[len(grouped[x]) // 2] for x in xs]
    return xs, ys


def plot_metric(rows: list[dict[str, str]], key: str, title: str, out_path: Path) -> None:
    xs, ys = median_by_nodes(rows, key)
    if not xs:
        return
    plt.figure(figsize=(7, 4))
    plt.plot(xs, ys, marker="o")
    plt.title(title)
    plt.xlabel("nodes")
    plt.ylabel(key)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary-csv", required=True)
    parser.add_argument("--plots-dir", required=True)
    args = parser.parse_args()

    rows = load_rows(Path(args.summary_csv).resolve())
    plots_dir = Path(args.plots_dir).resolve()
    plots_dir.mkdir(parents=True, exist_ok=True)

    plot_metric(rows, "header_p99", "Header P99 vs Nodes", plots_dir / "nodes_vs_header_p99.png")
    plot_metric(rows, "payload_p99", "Payload P99 vs Nodes", plots_dir / "nodes_vs_payload_p99.png")
    plot_metric(rows, "e2e_p99", "E2E P99 vs Nodes", plots_dir / "nodes_vs_e2e_p99.png")
    plot_metric(rows, "cpu_peak", "CPU Peak vs Nodes", plots_dir / "nodes_vs_cpu_peak.png")
    plot_metric(rows, "queue_max", "Queue Max vs Nodes", plots_dir / "nodes_vs_queue_max.png")
    plot_metric(rows, "timeout_cnt", "Timeout Count vs Nodes", plots_dir / "nodes_vs_timeout_retry.png")


if __name__ == "__main__":
    main()
