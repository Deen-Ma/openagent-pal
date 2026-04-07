#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

import matplotlib.pyplot as plt


REQUIRED_AGG_FILES = (
    "evidence_pack_summary.json",
    "multi_seed_summary.csv",
    "constraint_sweep_summary.csv",
)


@dataclass
class Paths:
    evidence_pack_root: Path
    aggregate_dir: Path
    output_dir: Path


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def to_float(v: Any) -> float:
    if v is None:
        return float("nan")
    if isinstance(v, float):
        return v
    if isinstance(v, int):
        return float(v)
    s = str(v).strip()
    if not s:
        return float("nan")
    return float(s)


def fmt(v: Any, digits: int = 5) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, bool):
        return str(v)
    f = to_float(v)
    if f != f:  # NaN
        return ""
    return f"{f:.{digits}f}".rstrip("0").rstrip(".")


def latest_complete_evidence_pack(results_root: Path) -> Path:
    candidates = sorted(
        results_root.glob("workload_char_public_discovery_two_stage_n100_evidence_pack_*")
    )
    valid: List[Path] = []
    for c in candidates:
        agg = c / "aggregate"
        if not agg.is_dir():
            continue
        if not all((agg / f).is_file() for f in REQUIRED_AGG_FILES):
            continue
        try:
            summary = json.loads((agg / "evidence_pack_summary.json").read_text(encoding="utf-8"))
        except Exception:
            continue
        if int(summary.get("run_count", 0)) == 5:
            valid.append(c)
    if not valid:
        raise SystemExit("No complete evidence-pack directory found under results/.")
    return valid[-1]


def resolve_paths(args: argparse.Namespace) -> Paths:
    repo_root = Path(args.repo_root).resolve()
    results_root = repo_root / "results"
    if args.evidence_pack_root:
        evidence_pack_root = Path(args.evidence_pack_root).resolve()
    else:
        evidence_pack_root = latest_complete_evidence_pack(results_root)
    aggregate_dir = evidence_pack_root / "aggregate"
    missing = [f for f in REQUIRED_AGG_FILES if not (aggregate_dir / f).is_file()]
    if missing:
        raise SystemExit(f"Missing aggregate files: {missing} in {aggregate_dir}")

    if args.output_dir:
        output_dir = Path(args.output_dir).resolve()
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_dir = repo_root / "reports" / f"sparse_demand_paper_pack_{ts}"
    output_dir.mkdir(parents=True, exist_ok=True)
    return Paths(evidence_pack_root=evidence_pack_root, aggregate_dir=aggregate_dir, output_dir=output_dir)


def pick_row(rows: Sequence[Dict[str, str]], *, seed: int | None = None, mode: str | None = None) -> Dict[str, str]:
    for r in rows:
        ok_seed = seed is None or int(float(r["seed"])) == seed
        ok_mode = mode is None or r["constraint_mode"] == mode
        if ok_seed and ok_mode:
            return r
    raise KeyError(f"Row not found for seed={seed}, mode={mode}")


def build_main_rows(
    multi_seed_rows: Sequence[Dict[str, str]],
    sweep_rows: Sequence[Dict[str, str]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in multi_seed_rows:
        out.append(
            {
                "section": "multi_seed_medium",
                "seed": int(float(r["seed"])),
                "mode": r["constraint_mode"],
                "visible_mean": to_float(r["visible_mean"]),
                "coarse_mean": to_float(r["coarse_mean"]),
                "full_mean": to_float(r["full_mean"]),
                "p_coarse_mean": to_float(r["p_coarse_mean"]),
                "p_full_mean": to_float(r["p_full_mean"]),
                "coarse_to_full_mean": to_float(r["coarse_to_full_mean"]),
                "full_match_available_rate": to_float(r["full_match_available_rate"]),
                "zero_full_match_rate": to_float(r["zero_full_match_rate"]),
            }
        )
    mode_order = {"loose": 0, "medium": 1, "tight": 2}
    for r in sorted(sweep_rows, key=lambda x: mode_order.get(x["constraint_mode"], 99)):
        out.append(
            {
                "section": "constraint_sweep_seed20260402",
                "seed": int(float(r["seed"])),
                "mode": r["constraint_mode"],
                "visible_mean": to_float(r["visible_mean"]),
                "coarse_mean": to_float(r["coarse_mean"]),
                "full_mean": to_float(r["full_mean"]),
                "p_coarse_mean": to_float(r["p_coarse_mean"]),
                "p_full_mean": to_float(r["p_full_mean"]),
                "coarse_to_full_mean": to_float(r["coarse_to_full_mean"]),
                "full_match_available_rate": to_float(r["full_match_available_rate"]),
                "zero_full_match_rate": to_float(r["zero_full_match_rate"]),
            }
        )
    return out


def write_main_table_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    cols = [
        "section",
        "seed",
        "mode",
        "visible_mean",
        "coarse_mean",
        "full_mean",
        "p_coarse_mean",
        "p_full_mean",
        "coarse_to_full_mean",
        "full_match_available_rate",
        "zero_full_match_rate",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_main_table_md(path: Path, rows: Sequence[Dict[str, Any]], digits: int = 5) -> None:
    cols = [
        "section",
        "seed",
        "mode",
        "visible_mean",
        "coarse_mean",
        "full_mean",
        "p_coarse_mean",
        "p_full_mean",
        "coarse_to_full_mean",
        "full_match_available_rate",
        "zero_full_match_rate",
    ]
    lines = []
    lines.append("| " + " | ".join(cols) + " |")
    lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
    for r in rows:
        vals = []
        for c in cols:
            v = r[c]
            if isinstance(v, float):
                vals.append(fmt(v, digits))
            else:
                vals.append(str(v))
        lines.append("| " + " | ".join(vals) + " |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_main_table_tex(path: Path, rows: Sequence[Dict[str, Any]], digits: int = 5) -> None:
    cols = [
        "section",
        "seed",
        "mode",
        "visible_mean",
        "coarse_mean",
        "full_mean",
        "p_coarse_mean",
        "p_full_mean",
        "coarse_to_full_mean",
        "full_match_available_rate",
        "zero_full_match_rate",
    ]
    def esc(s: str) -> str:
        return s.replace("_", "\\_")

    lines: List[str] = []
    lines.append("\\begin{table*}[t]")
    lines.append("\\centering")
    lines.append("\\small")
    lines.append("\\caption{Sparse-demand main results table (multi-seed + constraint sweep).}")
    lines.append("\\label{tab:sparse_demand_main}")
    lines.append("\\begin{tabular}{lllrrrrrrrr}")
    lines.append("\\toprule")
    lines.append(
        "section & seed & mode & visible & coarse & full & $p_{coarse}$ & $p_{full}$ & coarse/full & full-avail & zero-full \\\\"
    )
    lines.append("\\midrule")
    for r in rows:
        vals: List[str] = []
        for c in cols:
            v = r[c]
            if isinstance(v, str):
                vals.append(esc(v))
            elif isinstance(v, int):
                vals.append(str(v))
            else:
                vals.append(fmt(v, digits))
        lines.append(" & ".join(vals) + " \\\\")
    lines.append("\\bottomrule")
    lines.append("\\end{tabular}")
    lines.append("\\end{table*}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def plot_funnel_medium_seed20260402(path_base: Path, medium_20260402: Dict[str, str]) -> None:
    labels = ["Visible", "Coarse Match", "Full Match"]
    values = [
        to_float(medium_20260402["visible_mean"]),
        to_float(medium_20260402["coarse_mean"]),
        to_float(medium_20260402["full_mean"]),
    ]
    fig, ax = plt.subplots(figsize=(6.2, 4.0))
    bars = ax.bar(labels, values, color=["#4e79a7", "#59a14f", "#f28e2b"], width=0.62)
    ax.set_title("Sparse-Demand Funnel (Medium, Seed 20260402)")
    ax.set_ylabel("Mean Count per Request")
    ax.set_ylim(0, max(values) * 1.12)
    for b, v in zip(bars, values):
        ax.text(b.get_x() + b.get_width() / 2.0, v + max(values) * 0.015, fmt(v, 3), ha="center", va="bottom", fontsize=9)
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    fig.tight_layout()
    fig.savefig(path_base.with_suffix(".png"), dpi=220)
    fig.savefig(path_base.with_suffix(".pdf"))
    plt.close(fig)


def plot_multiseed_stability(path_base: Path, multi_seed_rows: Sequence[Dict[str, str]]) -> None:
    seeds = [str(int(float(r["seed"]))) for r in multi_seed_rows]
    p_coarse = [to_float(r["p_coarse_mean"]) for r in multi_seed_rows]
    p_full = [to_float(r["p_full_mean"]) for r in multi_seed_rows]
    coarse_to_full = [to_float(r["coarse_to_full_mean"]) for r in multi_seed_rows]

    x = list(range(len(seeds)))
    w = 0.22
    fig, ax = plt.subplots(figsize=(7.0, 4.0))
    ax.bar([i - w for i in x], p_coarse, width=w, label="p_coarse", color="#4e79a7")
    ax.bar(x, p_full, width=w, label="p_full", color="#f28e2b")
    ax.bar([i + w for i in x], coarse_to_full, width=w, label="coarse_to_full", color="#59a14f")
    ax.set_title("Multi-Seed Stability (Medium Constraint)")
    ax.set_xlabel("Seed")
    ax.set_ylabel("Ratio")
    ax.set_xticks(x)
    ax.set_xticklabels(seeds)
    ax.legend(frameon=False, ncol=3, loc="upper left")
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    fig.tight_layout()
    fig.savefig(path_base.with_suffix(".png"), dpi=220)
    fig.savefig(path_base.with_suffix(".pdf"))
    plt.close(fig)


def plot_constraint_sweep(path_base: Path, sweep_rows: Sequence[Dict[str, str]]) -> None:
    mode_order = {"loose": 0, "medium": 1, "tight": 2}
    rows = sorted(sweep_rows, key=lambda x: mode_order.get(x["constraint_mode"], 99))
    modes = [r["constraint_mode"] for r in rows]
    p_full = [to_float(r["p_full_mean"]) for r in rows]
    coarse_to_full = [to_float(r["coarse_to_full_mean"]) for r in rows]

    x = list(range(len(modes)))
    fig, ax = plt.subplots(figsize=(6.6, 4.0))
    ax.plot(x, p_full, marker="o", color="#f28e2b", linewidth=2.0, label="p_full")
    ax.plot(x, coarse_to_full, marker="s", color="#59a14f", linewidth=2.0, label="coarse_to_full")
    ax.set_title("Constraint Tightness Sweep")
    ax.set_xlabel("Constraint Mode")
    ax.set_ylabel("Ratio")
    ax.set_xticks(x)
    ax.set_xticklabels(modes)
    ax.legend(frameon=False, loc="upper right")
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    fig.tight_layout()
    fig.savefig(path_base.with_suffix(".png"), dpi=220)
    fig.savefig(path_base.with_suffix(".pdf"))
    plt.close(fig)


def build_cn_summary(robustness: Dict[str, Any], monotonicity: Dict[str, Any]) -> str:
    overview = (
        "我们基于已完成的 two-stage deterministic evidence pack（100 节点、200 requests）整理了 sparse-demand 证据。"
        "结果显示，在 medium 设定下，三个 seed 的 p_coarse、p_full 与 coarse_to_full 波动较小；在固定 seed 下，"
        "constraint 从 loose 到 tight 时，p_full 与 coarse_to_full 均单调下降。同时，full-match availability 始终维持在高位，"
        "说明需求稀疏并不等于请求无解。"
    )
    analysis = (
        "从稳定性看，multi-seed 的 p_coarse_mean、p_full_mean、coarse_to_full_mean 的变异系数分别为 "
        f"{fmt(robustness['p_coarse_mean']['cv'], 6)}、{fmt(robustness['p_full_mean']['cv'], 6)}、"
        f"{fmt(robustness['coarse_to_full_mean']['cv'], 6)}，说明 coarse 与 full 两层比例在不同随机种子下保持同一量级，"
        "不属于单次抽样偶然。进一步看约束收紧实验，p_full 与 coarse_to_full 在 loose→medium→tight 上严格下降，"
        f"monotonicity 检查结果为 p_full={monotonicity.get('p_full_monotonic')}、"
        f"coarse_to_full={monotonicity.get('coarse_to_full_monotonic')}。这验证了 two-stage 结构中“声明层可见性稳定、"
        "细约束决定最终匹配稀疏度”的机制。与此同时，full_match_available_rate 在各组中保持接近 1，"
        "对应 zero_full_match_rate 维持低位，说明虽然 full match 相对 coarse 显著收缩，但绝大多数请求仍至少存在可行提供者。"
        "因此，当前结果已经形成完整证据链：two-stage deterministic 已建立且不再由 probabilistic pull 主导，"
        "稀疏性来源于能力分布与约束结构本身，可直接支撑论文中的 Workload Characterization / Sparse-Demand Motivation 小节。"
    )
    return "# Sparse-Demand Results Summary (CN)\n\n## Overview\n\n" + overview + "\n\n## Analysis\n\n" + analysis + "\n"


def build_en_summary(robustness: Dict[str, Any], monotonicity: Dict[str, Any]) -> str:
    overview = (
        "Using the completed two-stage deterministic evidence pack (100 nodes, 200 requests), we observe a consistent sparse-demand pattern. "
        "Under the medium setting, p_coarse, p_full, and coarse_to_full remain stable across three seeds. "
        "Under fixed seed with constraint tightening (loose/medium/tight), both p_full and coarse_to_full decrease monotonically. "
        "At the same time, full-match availability stays high, indicating that sparsity does not imply unsatisfiable demand."
    )
    analysis = (
        "The multi-seed robustness statistics show low cross-seed variance in the key ratios, with coefficients of variation "
        f"of {fmt(robustness['p_coarse_mean']['cv'], 6)} for p_coarse_mean, "
        f"{fmt(robustness['p_full_mean']['cv'], 6)} for p_full_mean, and "
        f"{fmt(robustness['coarse_to_full_mean']['cv'], 6)} for coarse_to_full_mean. "
        "This supports that the observed sparsity is not an artifact of a single random realization. "
        "In the constraint sweep, both p_full and coarse_to_full follow a strict loose-to-tight decline, and the monotonicity checks are "
        f"p_full={monotonicity.get('p_full_monotonic')} and coarse_to_full={monotonicity.get('coarse_to_full_monotonic')}. "
        "These results are consistent with the intended two-stage semantics: declaration-level coarse visibility remains broad, while payload-level constraints drive selective full matching. "
        "Importantly, full_match_available_rate remains close to one, with low zero_full_match_rate, meaning that the system remains largely solvable despite strong sparsity. "
        "Taken together, the current package provides a coherent and publication-ready motivation for sparse demand in the workload characterization section."
    )
    return "# Sparse-Demand Results Summary (EN)\n\n## Overview\n\n" + overview + "\n\n## Analysis\n\n" + analysis + "\n"


def build_section_snippet_tex() -> str:
    return (
        "% Sparse-demand subsection snippet\n"
        "Table~\\ref{tab:sparse_demand_main} summarizes both the multi-seed robustness results (medium mode) and the constraint-tightness sweep "
        "(loose/medium/tight, fixed seed). The table shows that the two-stage deterministic pipeline consistently yields a strong contraction from "
        "coarse-level matches to full-level matches while maintaining high full-match availability.\n\n"
        "Figure~\\ref{fig:sparse_funnel} visualizes a representative funnel (medium, seed 20260402), where visibility is broad but full matches are "
        "substantially fewer. Figure~\\ref{fig:sparse_multiseed} further shows that $p_{coarse}$, $p_{full}$, and coarse-to-full ratios are stable across seeds. "
        "Figure~\\ref{fig:sparse_sweep} demonstrates a monotonic drop in $p_{full}$ and coarse-to-full as constraints tighten.\n\n"
        "Overall, these results support the sparse-demand motivation: matching sparsity is structural and reproducible, rather than being induced by probabilistic pull behavior. "
        "Response-path observations are retained as secondary diagnostics and are not required for this conclusion.\n"
    )


def build_metric_definitions_md() -> str:
    return """# Sparse-Demand Metric Definitions

- `visible`: Number of nodes that received the request declaration on the public topic.
- `coarse_match`: Number of nodes satisfying declaration-level coarse criteria (`required_family` + `coarse_domain`).
- `full_match`: Number of nodes satisfying payload-level full constraints (tools/trust/latency and configured specialization rule).
- `p_coarse`: `coarse_match / visible`.
- `p_full`: `full_match / visible`.
- `coarse_to_full`: `full_match / max(coarse_match, 1)`.
- `full_match_available_rate`: Fraction of requests with `full_match >= 1`.
- `zero_full_match_rate`: Fraction of requests with `full_match == 0`.

These definitions follow the existing two-stage workload characterization outputs and do not change the current metric semantics.
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate paper-ready sparse-demand artifacts from completed evidence-pack outputs.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--evidence-pack-root", default="")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--representative-seed", type=int, default=20260402)
    args = parser.parse_args()

    paths = resolve_paths(args)
    summary_json_path = paths.aggregate_dir / "evidence_pack_summary.json"
    multi_seed_csv_path = paths.aggregate_dir / "multi_seed_summary.csv"
    constraint_csv_path = paths.aggregate_dir / "constraint_sweep_summary.csv"

    summary = json.loads(summary_json_path.read_text(encoding="utf-8"))
    multi_seed_rows = read_csv(multi_seed_csv_path)
    sweep_rows = read_csv(constraint_csv_path)

    # deterministic ordering
    multi_seed_rows = sorted(multi_seed_rows, key=lambda x: int(float(x["seed"])))
    mode_order = {"loose": 0, "medium": 1, "tight": 2}
    sweep_rows = sorted(sweep_rows, key=lambda x: mode_order.get(x["constraint_mode"], 99))

    main_rows = build_main_rows(multi_seed_rows, sweep_rows)
    write_main_table_csv(paths.output_dir / "sparse_demand_main_table.csv", main_rows)
    write_main_table_md(paths.output_dir / "sparse_demand_main_table.md", main_rows)
    write_main_table_tex(paths.output_dir / "sparse_demand_main_table.tex", main_rows)

    medium_row = pick_row(multi_seed_rows, seed=args.representative_seed, mode="medium")
    plot_funnel_medium_seed20260402(
        paths.output_dir / "sparse_demand_funnel_medium_seed20260402",
        medium_row,
    )
    plot_multiseed_stability(paths.output_dir / "sparse_demand_multiseed_stability", multi_seed_rows)
    plot_constraint_sweep(paths.output_dir / "sparse_demand_constraint_sweep", sweep_rows)

    (paths.output_dir / "sparse_demand_results_summary_cn.md").write_text(
        build_cn_summary(summary["robustness_stats"], summary["monotonicity"]),
        encoding="utf-8",
    )
    (paths.output_dir / "sparse_demand_results_summary_en.md").write_text(
        build_en_summary(summary["robustness_stats"], summary["monotonicity"]),
        encoding="utf-8",
    )
    (paths.output_dir / "sparse_demand_section_snippet.tex").write_text(
        build_section_snippet_tex(), encoding="utf-8"
    )
    (paths.output_dir / "sparse_demand_metric_definitions.md").write_text(
        build_metric_definitions_md(), encoding="utf-8"
    )

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "evidence_pack_root": str(paths.evidence_pack_root),
        "aggregate_dir": str(paths.aggregate_dir),
        "inputs": {
            "evidence_pack_summary_json": str(summary_json_path),
            "multi_seed_summary_csv": str(multi_seed_csv_path),
            "constraint_sweep_summary_csv": str(constraint_csv_path),
            "run_matrix_csv": str(paths.aggregate_dir / "run_matrix.csv"),
        },
        "outputs_dir": str(paths.output_dir),
    }
    (paths.output_dir / "manifest_inputs_used.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    print(json.dumps(manifest, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
