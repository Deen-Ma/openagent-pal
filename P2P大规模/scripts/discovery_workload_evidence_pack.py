#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import statistics
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from discovery_workload_lib import dump_json, ensure_dir, now_iso


DEFAULT_PROFILE_NAME = "workload_char_public_discovery_two_stage_n100_evidence_pack"
DEFAULT_SEEDS = [20260402, 20260412, 20260422]
DEFAULT_SWEEP_SEED = 20260402
DEFAULT_SWEEP_MODES = ["loose", "medium", "tight"]
DEFAULT_BASE_CONFIG = "configs/workload_char_public_discovery_two_stage_n100.yaml"


def parse_int_csv(raw: Any) -> List[int]:
    if isinstance(raw, list):
        tokens = [str(item) for item in raw]
    else:
        tokens = str(raw).split(",")
    out: List[int] = []
    seen = set()
    for token in tokens:
        value = token.strip()
        if not value:
            continue
        number = int(value)
        if number in seen:
            continue
        seen.add(number)
        out.append(number)
    return out


def parse_mode_csv(raw: Any) -> List[str]:
    if isinstance(raw, list):
        tokens = [str(item) for item in raw]
    else:
        tokens = str(raw).split(",")
    out: List[str] = []
    seen = set()
    for token in tokens:
        value = token.strip().lower()
        if not value:
            continue
        if value not in {"loose", "medium", "tight"}:
            raise ValueError(f"unsupported mode: {value}")
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    if not rows:
        with path.open("w", newline="", encoding="utf-8") as handle:
            csv.writer(handle).writerow([])
        return
    keys = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def read_yaml(path: Path) -> Dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return {}


def get_nested(payload: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    cursor: Any = payload
    for key in keys:
        if not isinstance(cursor, dict):
            return default
        if key not in cursor:
            return default
        cursor = cursor[key]
    return cursor


def safe_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def aggregate_series(rows: List[Dict[str, Any]], key: str) -> Dict[str, Any]:
    values = [safe_float(row.get(key)) for row in rows]
    cleaned = [value for value in values if value is not None]
    if not cleaned:
        return {"count": 0}
    mean = float(sum(cleaned) / len(cleaned))
    std = float(statistics.pstdev(cleaned)) if len(cleaned) > 1 else 0.0
    cv = std / mean if abs(mean) > 1e-12 else 0.0
    return {
        "count": len(cleaned),
        "mean": round(mean, 6),
        "std": round(std, 6),
        "min": round(min(cleaned), 6),
        "max": round(max(cleaned), 6),
        "cv": round(cv, 6),
    }


def metrics_avg(summary: Dict[str, Any], key: str) -> float | None:
    metrics = summary.get("metrics")
    if not isinstance(metrics, dict):
        return None
    bucket = metrics.get(key)
    if not isinstance(bucket, dict):
        return None
    value = bucket.get("avg")
    if isinstance(value, (int, float)):
        return float(value)
    return None


def ordered_unique_specs(specs: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    seen = set()
    for seed, mode in specs:
        key = (int(seed), str(mode))
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def run_spec(
    *,
    python_exec: str,
    repo: Path,
    base_config: Path,
    output_root: Path,
    profile_name: str,
    seed: int,
    mode: str,
) -> Dict[str, Any]:
    run_id = f"seed{seed}_{mode}"
    run_root = output_root / "runs" / run_id
    ensure_dir(run_root)
    cmd = [
        python_exec,
        str(repo / "scripts" / "discovery_workload_char_probe.py"),
        "--repo",
        str(repo),
        "--config",
        str(base_config),
        "--output-root",
        str(run_root),
        "--profile-name",
        profile_name,
        "--repetitions",
        "1",
        "--random-seed",
        str(seed),
        "--constraint-mode",
        mode,
    ]
    proc = subprocess.run(cmd, check=False, text=True, capture_output=True)
    (run_root / "driver_stdout.log").write_text(proc.stdout, encoding="utf-8")
    (run_root / "driver_stderr.log").write_text(proc.stderr, encoding="utf-8")

    one_run_root = run_root / "results" / "run01"
    summary_path = one_run_root / "summaries" / "summary.json"
    runtime_summary_path = one_run_root / "summaries" / "runtime_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
    runtime_summary = (
        json.loads(runtime_summary_path.read_text(encoding="utf-8")) if runtime_summary_path.exists() else {}
    )
    status = "ok" if summary else "failed"
    note = ""
    if proc.returncode != 0:
        note = f"probe_rc={proc.returncode}"
    elif not summary:
        note = "summary_missing"
    elif not bool(runtime_summary.get("bootstrap_pass", True)):
        note = str(runtime_summary.get("bench_fail_reason") or runtime_summary.get("bootstrap_fail_reason") or "")

    row = {
        "run_id": run_id,
        "seed": seed,
        "constraint_mode": mode,
        "status": status,
        "note": note,
        "probe_rc": proc.returncode,
        "bootstrap_pass": bool(runtime_summary.get("bootstrap_pass")),
        "bench_pass": bool(runtime_summary.get("bench_ok")),
        "request_count": summary.get("request_count"),
        "visible_mean": metrics_avg(summary, "visible_count"),
        "coarse_mean": metrics_avg(summary, "coarse_match_count"),
        "full_mean": metrics_avg(summary, "full_match_count"),
        "actual_pull_mean": metrics_avg(summary, "actual_pull_count"),
        "actual_response_mean": metrics_avg(summary, "actual_response_count"),
        "p_coarse_mean": metrics_avg(summary, "p_coarse"),
        "p_full_mean": metrics_avg(summary, "p_full"),
        "coarse_to_full_mean": metrics_avg(summary, "coarse_to_full"),
        "full_match_available_rate": safe_float(summary.get("full_match_available_rate")),
        "zero_full_match_rate": safe_float(summary.get("zero_full_match_rate")),
        "zero_responder_rate": safe_float(summary.get("zero_responder_rate")),
        "first_response_available_rate": safe_float(summary.get("first_response_available_rate")),
        "run_root": str(run_root),
        "one_run_root": str(one_run_root),
        "summary_path": str(summary_path),
    }
    return {"row": row, "summary": summary, "runtime_summary": runtime_summary}


def build_md(
    *,
    profile_name: str,
    matrix_rows: List[Dict[str, Any]],
    multi_seed_rows: List[Dict[str, Any]],
    constraint_rows: List[Dict[str, Any]],
    robustness_stats: Dict[str, Any],
    monotonicity: Dict[str, Any],
    output_root: Path,
) -> str:
    lines = [
        "# Evidence Pack Summary",
        "",
        "## 1) 实验目的",
        "",
        "- 补充 two-stage sparse-demand characterization 的稳健性证据。",
        "- 本次主分析聚焦 `visible -> coarse_match -> full_match`；`actual_response` 仅作为次要观察。",
        "",
        "## 2) 运行矩阵（seed x mode）",
        "",
        "| run_id | seed | mode | status | note |",
        "| --- | --- | --- | --- | --- |",
    ]
    for row in matrix_rows:
        lines.append(
            f"| {row['run_id']} | {row['seed']} | {row['constraint_mode']} | {row['status']} | {row.get('note','')} |"
        )

    lines.extend(
        [
            "",
            "## 3) Multi-Seed Robustness（mode=medium）",
            "",
            "| seed | visible_mean | coarse_mean | full_mean | p_coarse_mean | p_full_mean | coarse_to_full_mean | full_match_available_rate | zero_full_match_rate |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in multi_seed_rows:
        lines.append(
            f"| {row['seed']} | {row.get('visible_mean')} | {row.get('coarse_mean')} | {row.get('full_mean')} | {row.get('p_coarse_mean')} | {row.get('p_full_mean')} | {row.get('coarse_to_full_mean')} | {row.get('full_match_available_rate')} | {row.get('zero_full_match_rate')} |"
        )
    lines.extend(
        [
            "",
            f"- p_coarse_mean 跨 seed 统计: `{json.dumps(robustness_stats.get('p_coarse_mean', {}), ensure_ascii=True)}`",
            f"- p_full_mean 跨 seed 统计: `{json.dumps(robustness_stats.get('p_full_mean', {}), ensure_ascii=True)}`",
            f"- coarse_to_full_mean 跨 seed 统计: `{json.dumps(robustness_stats.get('coarse_to_full_mean', {}), ensure_ascii=True)}`",
            f"- full_match_available_rate 跨 seed 统计: `{json.dumps(robustness_stats.get('full_match_available_rate', {}), ensure_ascii=True)}`",
            "",
            "## 4) Constraint Sweep（seed=20260402）",
            "",
            "| mode | visible_mean | coarse_mean | full_mean | p_coarse_mean | p_full_mean | coarse_to_full_mean | full_match_available_rate | zero_full_match_rate |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for row in constraint_rows:
        lines.append(
            f"| {row['constraint_mode']} | {row.get('visible_mean')} | {row.get('coarse_mean')} | {row.get('full_mean')} | {row.get('p_coarse_mean')} | {row.get('p_full_mean')} | {row.get('coarse_to_full_mean')} | {row.get('full_match_available_rate')} | {row.get('zero_full_match_rate')} |"
        )
    lines.extend(
        [
            "",
            f"- monotonicity.p_full: `{monotonicity.get('p_full_monotonic')}`",
            f"- monotonicity.coarse_to_full: `{monotonicity.get('coarse_to_full_monotonic')}`",
            f"- monotonicity.note: `{monotonicity.get('note', '')}`",
            "",
            "## 5) 一句话结论",
            "",
            "- 这组补实验用于验证 sparse-demand 在多 seed 下是否稳定、并验证在约束收紧时 full-match 比例是否系统下降。",
            "",
            "## 6) 输出目录",
            "",
            f"- `{output_root}`",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--config", default=DEFAULT_BASE_CONFIG)
    parser.add_argument("--output-root", default="")
    parser.add_argument("--profile-name", default=DEFAULT_PROFILE_NAME)
    parser.add_argument("--seeds", default=",".join(str(seed) for seed in DEFAULT_SEEDS))
    parser.add_argument("--sweep-seed", type=int, default=DEFAULT_SWEEP_SEED)
    parser.add_argument("--sweep-modes", default=",".join(DEFAULT_SWEEP_MODES))
    parser.add_argument("--python", default="python3")
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    base_config = Path(args.config).expanduser()
    if not base_config.is_absolute():
        base_config = (repo / base_config).resolve()

    config_payload = read_yaml(base_config) if base_config.exists() else {}
    evidence_name = get_nested(config_payload, "evidence_pack", "name", default=args.profile_name)
    seeds = parse_int_csv(get_nested(config_payload, "evidence_pack", "multi_seed", default=args.seeds))
    sweep_seed = int(get_nested(config_payload, "evidence_pack", "sweep_seed", default=args.sweep_seed))
    sweep_modes = parse_mode_csv(get_nested(config_payload, "evidence_pack", "sweep_modes", default=args.sweep_modes))
    if not seeds:
        seeds = list(DEFAULT_SEEDS)
    if not sweep_modes:
        sweep_modes = list(DEFAULT_SWEEP_MODES)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_root = (
        Path(args.output_root).resolve()
        if args.output_root
        else repo / "results" / f"{evidence_name}_{timestamp}"
    )
    aggregate_dir = ensure_dir(output_root / "aggregate")
    ensure_dir(output_root / "runs")
    ensure_dir(output_root / "logs")
    ensure_dir(output_root / "summaries")

    requested_specs: List[Tuple[int, str]] = []
    requested_specs.extend((seed, "medium") for seed in seeds)
    requested_specs.extend((sweep_seed, mode) for mode in sweep_modes)
    logical_specs = ordered_unique_specs(requested_specs)

    run_records: List[Dict[str, Any]] = []
    for seed, mode in logical_specs:
        record = run_spec(
            python_exec=args.python,
            repo=repo,
            base_config=base_config,
            output_root=output_root,
            profile_name=evidence_name,
            seed=seed,
            mode=mode,
        )
        run_records.append(record["row"])

    run_matrix_csv = aggregate_dir / "run_matrix.csv"
    write_csv(run_matrix_csv, run_records)

    medium_rows = [row for row in run_records if int(row["seed"]) in seeds and row["constraint_mode"] == "medium"]
    medium_rows = sorted(medium_rows, key=lambda row: int(row["seed"]))
    constraint_rows = [row for row in run_records if int(row["seed"]) == sweep_seed and row["constraint_mode"] in sweep_modes]
    mode_rank = {mode: idx for idx, mode in enumerate(sweep_modes)}
    constraint_rows = sorted(constraint_rows, key=lambda row: mode_rank.get(str(row["constraint_mode"]), 999))

    multi_seed_csv = aggregate_dir / "multi_seed_summary.csv"
    constraint_csv = aggregate_dir / "constraint_sweep_summary.csv"
    write_csv(multi_seed_csv, medium_rows)
    write_csv(constraint_csv, constraint_rows)

    robustness_stats = {
        "p_coarse_mean": aggregate_series(medium_rows, "p_coarse_mean"),
        "p_full_mean": aggregate_series(medium_rows, "p_full_mean"),
        "coarse_to_full_mean": aggregate_series(medium_rows, "coarse_to_full_mean"),
        "full_match_available_rate": aggregate_series(medium_rows, "full_match_available_rate"),
    }

    by_mode: Dict[str, Dict[str, Any]] = {str(row["constraint_mode"]): row for row in constraint_rows}
    monotonic_note = ""
    p_full_ok = False
    c2f_ok = False
    if all(mode in by_mode for mode in ("loose", "medium", "tight")):
        loose_full = safe_float(by_mode["loose"].get("p_full_mean"))
        medium_full = safe_float(by_mode["medium"].get("p_full_mean"))
        tight_full = safe_float(by_mode["tight"].get("p_full_mean"))
        loose_c2f = safe_float(by_mode["loose"].get("coarse_to_full_mean"))
        medium_c2f = safe_float(by_mode["medium"].get("coarse_to_full_mean"))
        tight_c2f = safe_float(by_mode["tight"].get("coarse_to_full_mean"))
        if None not in {loose_full, medium_full, tight_full, loose_c2f, medium_c2f, tight_c2f}:
            p_full_ok = bool(loose_full >= medium_full >= tight_full)
            c2f_ok = bool(loose_c2f >= medium_c2f >= tight_c2f)
            if not p_full_ok or not c2f_ok:
                monotonic_note = "observed local non-monotonicity, inspect per-request discrete constraint effects"
        else:
            monotonic_note = "missing p_full/coarse_to_full values"
    else:
        monotonic_note = "missing loose/medium/tight rows"

    monotonicity = {
        "p_full_monotonic": p_full_ok,
        "coarse_to_full_monotonic": c2f_ok,
        "note": monotonic_note,
    }

    summary_json = {
        "generated_at": now_iso(),
        "profile_name": evidence_name,
        "base_config": str(base_config),
        "output_root": str(output_root),
        "logical_runs": logical_specs,
        "run_count": len(run_records),
        "run_records": run_records,
        "multi_seed_rows": medium_rows,
        "constraint_rows": constraint_rows,
        "robustness_stats": robustness_stats,
        "monotonicity": monotonicity,
        "outputs": {
            "run_matrix_csv": str(run_matrix_csv),
            "multi_seed_summary_csv": str(multi_seed_csv),
            "constraint_sweep_summary_csv": str(constraint_csv),
            "evidence_pack_summary_json": str(aggregate_dir / "evidence_pack_summary.json"),
            "evidence_pack_summary_md": str(aggregate_dir / "evidence_pack_summary.md"),
        },
    }
    dump_json(aggregate_dir / "evidence_pack_summary.json", summary_json)
    summary_md = build_md(
        profile_name=evidence_name,
        matrix_rows=run_records,
        multi_seed_rows=medium_rows,
        constraint_rows=constraint_rows,
        robustness_stats=robustness_stats,
        monotonicity=monotonicity,
        output_root=output_root,
    )
    (aggregate_dir / "evidence_pack_summary.md").write_text(summary_md + "\n", encoding="utf-8")
    print(json.dumps({"output_root": str(output_root), "run_count": len(run_records)}, indent=2))


if __name__ == "__main__":
    main()
