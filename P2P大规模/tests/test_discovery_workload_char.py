from __future__ import annotations

import csv
import importlib.util
import json
import random
import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

LIB_SPEC = importlib.util.spec_from_file_location(
    "discovery_workload_lib",
    SCRIPTS_DIR / "discovery_workload_lib.py",
)
assert LIB_SPEC is not None and LIB_SPEC.loader is not None
LIB = importlib.util.module_from_spec(LIB_SPEC)
LIB_SPEC.loader.exec_module(LIB)

REMOTE_BENCH_SPEC = importlib.util.spec_from_file_location(
    "discovery_workload_remote_bench",
    SCRIPTS_DIR / "discovery_workload_remote_bench.py",
)
assert REMOTE_BENCH_SPEC is not None and REMOTE_BENCH_SPEC.loader is not None
REMOTE_BENCH = importlib.util.module_from_spec(REMOTE_BENCH_SPEC)
REMOTE_BENCH_SPEC.loader.exec_module(REMOTE_BENCH)

AGG_SPEC = importlib.util.spec_from_file_location(
    "discovery_workload_aggregate",
    SCRIPTS_DIR / "discovery_workload_aggregate.py",
)
assert AGG_SPEC is not None and AGG_SPEC.loader is not None
AGG = importlib.util.module_from_spec(AGG_SPEC)
AGG_SPEC.loader.exec_module(AGG)


def test_assign_node_capabilities_is_seed_reproducible() -> None:
    rng_a = random.Random(1234)
    rng_b = random.Random(1234)
    caps_a = LIB.assign_node_capabilities(
        node_count=6,
        capability_tags=LIB.DEFAULT_CAPABILITY_TAGS,
        capabilities_per_node=2,
        rng=rng_a,
    )
    caps_b = LIB.assign_node_capabilities(
        node_count=6,
        capability_tags=LIB.DEFAULT_CAPABILITY_TAGS,
        capabilities_per_node=2,
        rng=rng_b,
    )
    assert caps_a == caps_b
    assert all(len(value) == 2 for value in caps_a.values())


def test_choose_pullers_probabilistic_is_seed_reproducible() -> None:
    rng_a = random.Random(2026)
    rng_b = random.Random(2026)
    selected_a = REMOTE_BENCH.choose_pullers(
        policy="probabilistic",
        candidates=[2, 3, 4, 5, 6, 7],
        rng=rng_a,
        pull_probability=0.30,
        pull_topk=5,
    )
    selected_b = REMOTE_BENCH.choose_pullers(
        policy="probabilistic",
        candidates=[2, 3, 4, 5, 6, 7],
        rng=rng_b,
        pull_probability=0.30,
        pull_topk=5,
    )
    assert selected_a == selected_b


def test_choose_pullers_deterministic_coarse_match_returns_all_candidates() -> None:
    selected = REMOTE_BENCH.choose_pullers(
        policy="deterministic_coarse_match",
        candidates=[9, 2, 7, 4],
        rng=random.Random(1),
        pull_probability=0.3,
        pull_topk=2,
    )
    assert selected == [2, 4, 7, 9]


def test_two_stage_matching_rules_are_deterministic() -> None:
    profile = {
        "families": ["planner", "coder"],
        "domains": ["research"],
        "tools": ["python", "web"],
        "trust_domain": "internal",
        "latency_tier": "medium",
        "specialization_tags": ["fast"],
    }
    declaration = {"required_family": "planner", "coarse_domain": "research"}
    payload = {
        "required_tools": ["python"],
        "required_trust_domain": "internal",
        "max_latency_tier": "medium",
        "optional_specialization_preference": "fast",
    }
    assert LIB.coarse_match(profile, declaration)
    matched, evidence = LIB.full_match(
        profile,
        payload,
        latency_tiers=LIB.DEFAULT_LATENCY_TIERS,
        specialization_hard=True,
    )
    assert matched is True
    assert evidence["tools_ok"] is True
    assert evidence["trust_ok"] is True
    assert evidence["latency_ok"] is True
    assert evidence["specialization_hit"] is True

    payload_tight = dict(payload, max_latency_tier="low")
    matched_tight, _ = LIB.full_match(
        profile,
        payload_tight,
        latency_tiers=LIB.DEFAULT_LATENCY_TIERS,
        specialization_hard=True,
    )
    assert matched_tight is False


def test_two_stage_declaration_stable_across_constraint_modes() -> None:
    node_profiles = LIB.assign_node_profiles_two_stage(
        node_count=30,
        families=LIB.DEFAULT_FAMILIES,
        domains=LIB.DEFAULT_DOMAINS,
        tools=LIB.DEFAULT_TOOLS,
        trust_domains=LIB.DEFAULT_TRUST_DOMAINS,
        latency_tiers=LIB.DEFAULT_LATENCY_TIERS,
        specialization_tags=LIB.DEFAULT_SPECIALIZATION_TAGS,
        rng=random.Random(20260401),
    )
    declarations_by_mode = {}
    for mode in ("loose", "medium", "tight"):
        decl_rows, _, _ = LIB.generate_two_stage_requests(
            node_profiles=node_profiles,
            request_count=50,
            payload_bytes=32768,
            latency_tiers=LIB.DEFAULT_LATENCY_TIERS,
            rng=random.Random(20260402),
            constraint_mode=mode,
        )
        declarations_by_mode[mode] = [
            (
                row["request_id"],
                row["requester_id"],
                row["required_family"],
                row["coarse_domain"],
            )
            for row in decl_rows
        ]
    assert declarations_by_mode["loose"] == declarations_by_mode["medium"]
    assert declarations_by_mode["medium"] == declarations_by_mode["tight"]


def test_two_stage_full_match_monotonic_with_tightening() -> None:
    node_profiles = LIB.assign_node_profiles_two_stage(
        node_count=50,
        families=LIB.DEFAULT_FAMILIES,
        domains=LIB.DEFAULT_DOMAINS,
        tools=LIB.DEFAULT_TOOLS,
        trust_domains=LIB.DEFAULT_TRUST_DOMAINS,
        latency_tiers=LIB.DEFAULT_LATENCY_TIERS,
        specialization_tags=LIB.DEFAULT_SPECIALIZATION_TAGS,
        rng=random.Random(20260411),
    )
    payloads_by_mode: dict[str, dict[str, dict[str, object]]] = {}
    declarations_ref = None
    for mode in ("loose", "medium", "tight"):
        decl_rows, payload_rows, _ = LIB.generate_two_stage_requests(
            node_profiles=node_profiles,
            request_count=80,
            payload_bytes=32768,
            latency_tiers=LIB.DEFAULT_LATENCY_TIERS,
            rng=random.Random(20260412),
            constraint_mode=mode,
        )
        if declarations_ref is None:
            declarations_ref = {row["request_id"]: row for row in decl_rows}
        else:
            current_decl = {row["request_id"]: row for row in decl_rows}
            for request_id, row in current_decl.items():
                base_row = declarations_ref[request_id]
                assert row["request_id"] == base_row["request_id"]
                assert row["requester_id"] == base_row["requester_id"]
                assert row["required_family"] == base_row["required_family"]
                assert row["coarse_domain"] == base_row["coarse_domain"]
                assert row["payload_ref"] == base_row["payload_ref"]
                assert row["payload_id"] == base_row["payload_id"]
        payloads_by_mode[mode] = {row["request_id"]: row for row in payload_rows}

    assert declarations_ref is not None
    for request_id, declaration in declarations_ref.items():
        coarse_count = 0
        full_counts = {"loose": 0, "medium": 0, "tight": 0}
        for node_profile in node_profiles.values():
            if not LIB.coarse_match(node_profile, declaration):
                continue
            coarse_count += 1
            for mode in ("loose", "medium", "tight"):
                payload_row = payloads_by_mode[mode][request_id]
                matched, _ = LIB.full_match(
                    node_profile,
                    payload_row,
                    latency_tiers=LIB.DEFAULT_LATENCY_TIERS,
                    specialization_hard=bool(payload_row.get("specialization_hard", False)),
                )
                if matched:
                    full_counts[mode] += 1
        assert full_counts["loose"] >= full_counts["medium"] >= full_counts["tight"]
        assert coarse_count >= full_counts["loose"]


def test_aggregate_builds_expected_funnel(tmp_path: Path) -> None:
    run_root = tmp_path / "run"
    for subdir in ("configs", "logs", "metrics", "results", "summaries"):
        (run_root / subdir).mkdir(parents=True, exist_ok=True)

    (run_root / "results" / "requests.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "request_id": "rep01-req-00001",
                        "repetition": 1,
                        "requester_id": 1,
                        "required_capability": "coder",
                        "payload_id": "payload-rep01-req-00001",
                        "payload_bytes": 32768,
                        "created_ts": 1.0,
                    }
                ),
                json.dumps(
                    {
                        "request_id": "rep01-req-00002",
                        "repetition": 1,
                        "requester_id": 2,
                        "required_capability": "planner",
                        "payload_id": "payload-rep01-req-00002",
                        "payload_bytes": 32768,
                        "created_ts": 2.0,
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    (run_root / "results" / "node_capabilities.json").write_text(
        json.dumps(
            {
                "random_seed": 20260402,
                "capability_tags": LIB.DEFAULT_CAPABILITY_TAGS,
                "capabilities_per_node": 2,
                "node_capabilities": {
                    "1": ["coder", "planner"],
                    "2": ["planner", "memory"],
                    "3": ["coder", "vision"],
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    events = [
        {
            "request_id": "rep01-req-00001",
            "node_id": 1,
            "event_type": LIB.DECLARATION_RECEIVED,
            "event_ts": 10.0,
        },
        {
            "request_id": "rep01-req-00001",
            "node_id": 2,
            "event_type": LIB.DECLARATION_RECEIVED,
            "event_ts": 11.0,
        },
        {
            "request_id": "rep01-req-00001",
            "node_id": 3,
            "event_type": LIB.DECLARATION_RECEIVED,
            "event_ts": 12.0,
        },
        {
            "request_id": "rep01-req-00001",
            "node_id": 1,
            "event_type": LIB.ELIGIBILITY_TRUE,
            "event_ts": 10.1,
        },
        {
            "request_id": "rep01-req-00001",
            "node_id": 3,
            "event_type": LIB.ELIGIBILITY_TRUE,
            "event_ts": 12.1,
        },
        {
            "request_id": "rep01-req-00001",
            "node_id": 3,
            "event_type": LIB.PULL_SENT,
            "event_ts": 12.2,
        },
        {
            "request_id": "rep01-req-00001",
            "node_id": 3,
            "event_type": LIB.RESPONSE_RECEIVED_BY_REQUESTER,
            "event_ts": 13.0,
        },
        {
            "request_id": "rep01-req-00002",
            "node_id": 1,
            "event_type": LIB.DECLARATION_RECEIVED,
            "event_ts": 20.0,
        },
        {
            "request_id": "rep01-req-00002",
            "node_id": 2,
            "event_type": LIB.DECLARATION_RECEIVED,
            "event_ts": 20.1,
        },
        {
            "request_id": "rep01-req-00002",
            "node_id": 2,
            "event_type": LIB.ELIGIBILITY_TRUE,
            "event_ts": 20.2,
        },
    ]
    (run_root / "logs" / "events.jsonl").write_text(
        "\n".join(json.dumps(row) for row in events) + "\n",
        encoding="utf-8",
    )
    (run_root / "results" / "request_timeline.jsonl").write_text("", encoding="utf-8")
    (run_root / "configs" / "runtime_config.json").write_text(
        json.dumps(
            {
                "node_count": 3,
                "repetitions": 1,
                "random_seed": 20260402,
                "pull_policy": "probabilistic",
                "pull_probability": 0.3,
                "capability_tags": LIB.DEFAULT_CAPABILITY_TAGS,
                "capabilities_per_node": 2,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (run_root / "summaries" / "runtime_summary.json").write_text(
        json.dumps({"bench_ok": True}) + "\n",
        encoding="utf-8",
    )

    import subprocess

    subprocess.run(
        [
            "python3",
            str(SCRIPTS_DIR / "discovery_workload_aggregate.py"),
            "--run-root",
            str(run_root),
        ],
        check=True,
    )

    per_request_csv = run_root / "results" / "per_request_metrics.csv"
    assert per_request_csv.exists()
    with per_request_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2

    req1 = next(row for row in rows if row["request_id"] == "rep01-req-00001")
    assert int(req1["visible_count"]) == 3
    assert int(req1["eligible_count"]) == 2
    assert int(req1["pulling_count"]) == 1
    assert int(req1["responding_count"]) == 1
    assert req1["has_full_match"] in {"True", "true", "1"}
    assert req1["has_response"] in {"True", "true", "1"}

    summary_json = json.loads((run_root / "summaries" / "summary.json").read_text(encoding="utf-8"))
    assert summary_json["metrics"]["visible_count"]["count"] == 2
    assert summary_json["funnel_overall"]["visible_total"] == 5
    assert "full_match_available_rate" in summary_json
    assert "zero_full_match_rate" in summary_json
