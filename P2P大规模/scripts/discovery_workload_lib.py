#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import random
import statistics
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


DEFAULT_CAPABILITY_TAGS = [
    "planner",
    "retriever",
    "tool-user",
    "coder",
    "memory",
    "vision",
    "verifier",
    "coordinator",
]
DEFAULT_FAMILIES = list(DEFAULT_CAPABILITY_TAGS)
DEFAULT_DOMAINS = ["travel", "finance", "ops", "research", "dev"]
DEFAULT_TOOLS = ["python", "sql", "ocr", "web", "maps", "shell"]
DEFAULT_TRUST_DOMAINS = ["public", "internal"]
DEFAULT_LATENCY_TIERS = ["low", "medium", "high"]
DEFAULT_SPECIALIZATION_TAGS = ["fast", "reliable", "multimodal", "structured-output"]
DEFAULT_DEADLINE_CLASSES = ["loose", "normal", "tight"]
DEFAULT_CONSTRAINT_MODE = "medium"
CONSTRAINT_MODES = ["loose", "medium", "tight"]

DECLARATION_RECEIVED = "declaration_received"
ELIGIBILITY_TRUE = "eligibility_true"
COARSE_MATCH_TRUE = "coarse_match_true"
FULL_MATCH_TRUE = "full_match_true"
PULL_SENT = "pull_sent"
PAYLOAD_RECEIVED = "payload_received"
RESPONSE_SENT = "response_sent"
RESPONSE_RECEIVED_BY_REQUESTER = "response_received_by_requester"

MANDATORY_EVENT_TYPES = {
    DECLARATION_RECEIVED,
    ELIGIBILITY_TRUE,
    COARSE_MATCH_TRUE,
    FULL_MATCH_TRUE,
    PULL_SENT,
    PAYLOAD_RECEIVED,
    RESPONSE_SENT,
    RESPONSE_RECEIVED_BY_REQUESTER,
}


def now_ts() -> float:
    return round(time.time(), 6)


def now_iso(ts: float | None = None) -> str:
    value = ts if ts is not None else time.time()
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(value))


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def dump_json(path: Path, payload: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        token = line.strip()
        if not token:
            continue
        rows.append(json.loads(token))
    return rows


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def normalize_string_list(raw_values: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for raw in raw_values:
        token = str(raw).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        normalized.append(token)
    return normalized


def stats(values: Iterable[float | int | None]) -> Dict[str, Any]:
    cleaned = sorted(float(value) for value in values if value is not None)
    if not cleaned:
        return {"count": 0}
    count = len(cleaned)
    p50_index = max(0, math.ceil(count * 0.50) - 1)
    p95_index = max(0, math.ceil(count * 0.95) - 1)
    p99_index = max(0, math.ceil(count * 0.99) - 1)
    return {
        "count": count,
        "min": round(cleaned[0], 6),
        "p50": round(cleaned[p50_index], 6),
        "median": round(float(statistics.median(cleaned)), 6),
        "p95": round(cleaned[p95_index], 6),
        "p99": round(cleaned[p99_index], 6),
        "max": round(cleaned[-1], 6),
        "avg": round(sum(cleaned) / count, 6),
    }


def assign_node_capabilities(
    *,
    node_count: int,
    capability_tags: List[str],
    capabilities_per_node: int,
    rng,
) -> Dict[int, List[str]]:
    if node_count < 1:
        raise ValueError("node_count must be >= 1")
    if capabilities_per_node < 1:
        raise ValueError("capabilities_per_node must be >= 1")
    if capabilities_per_node > len(capability_tags):
        raise ValueError("capabilities_per_node exceeds capability_tags size")
    node_caps: Dict[int, List[str]] = {}
    for node_id in range(1, node_count + 1):
        node_caps[node_id] = sorted(rng.sample(capability_tags, k=capabilities_per_node))
    return node_caps


def assign_node_profiles_two_stage(
    *,
    node_count: int,
    families: List[str],
    domains: List[str],
    tools: List[str],
    trust_domains: List[str],
    latency_tiers: List[str],
    specialization_tags: List[str],
    rng,
    families_per_node: int = 2,
    domain_min: int = 1,
    domain_max: int = 2,
    tool_min: int = 1,
    tool_max: int = 3,
    specialization_max: int = 2,
) -> Dict[int, Dict[str, Any]]:
    if node_count < 1:
        raise ValueError("node_count must be >= 1")
    if families_per_node < 1 or families_per_node > len(families):
        raise ValueError("families_per_node out of range")
    if domain_min < 1 or domain_min > domain_max or domain_max > len(domains):
        raise ValueError("domain range invalid")
    if tool_min < 1 or tool_min > tool_max or tool_max > len(tools):
        raise ValueError("tool range invalid")
    if specialization_max < 0:
        raise ValueError("specialization_max must be >= 0")
    if not trust_domains:
        raise ValueError("trust_domains must not be empty")
    if not latency_tiers:
        raise ValueError("latency_tiers must not be empty")

    profiles: Dict[int, Dict[str, Any]] = {}
    for node_id in range(1, node_count + 1):
        domain_count = int(rng.randint(domain_min, domain_max))
        tool_count = int(rng.randint(tool_min, tool_max))
        spec_upper = min(specialization_max, len(specialization_tags))
        spec_count = int(rng.randint(0, spec_upper)) if spec_upper > 0 else 0
        profile = {
            "node_id": node_id,
            "families": sorted(rng.sample(families, k=families_per_node)),
            "domains": sorted(rng.sample(domains, k=domain_count)),
            "tools": sorted(rng.sample(tools, k=tool_count)),
            "trust_domain": str(rng.choice(trust_domains)),
            "latency_tier": str(rng.choice(latency_tiers)),
            "specialization_tags": sorted(rng.sample(specialization_tags, k=spec_count)) if spec_count > 0 else [],
        }
        profiles[node_id] = profile
    return profiles


def generate_requests(
    *,
    node_count: int,
    request_count: int,
    capability_tags: List[str],
    payload_bytes: int,
    rng,
) -> List[Dict[str, Any]]:
    if request_count < 1:
        raise ValueError("request_count must be >= 1")
    requests: List[Dict[str, Any]] = []
    for idx in range(1, request_count + 1):
        request_id = f"req-{idx:05d}"
        requester_id = int(rng.randint(1, node_count))
        required_capability = str(rng.choice(capability_tags))
        created_ts = now_ts()
        requests.append(
            {
                "request_id": request_id,
                "requester_id": requester_id,
                "required_capability": required_capability,
                "payload_id": f"payload-{request_id}",
                "payload_bytes": int(payload_bytes),
                "created_ts": created_ts,
                "created_ts_iso": now_iso(created_ts),
            }
        )
    return requests


def generate_two_stage_requests(
    *,
    node_profiles: Dict[int, Dict[str, Any]],
    request_count: int,
    payload_bytes: int,
    latency_tiers: List[str],
    rng,
    required_tools_min: int = 1,
    required_tools_max: int = 2,
    specialization_preference_prob: float = 0.55,
    deadline_classes: List[str] | None = None,
    constraint_mode: str = DEFAULT_CONSTRAINT_MODE,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    if request_count < 1:
        raise ValueError("request_count must be >= 1")
    if payload_bytes < 1:
        raise ValueError("payload_bytes must be >= 1")
    if not node_profiles:
        raise ValueError("node_profiles must not be empty")
    if not latency_tiers:
        raise ValueError("latency_tiers must not be empty")
    if required_tools_min < 1 or required_tools_min > required_tools_max:
        raise ValueError("required_tools range invalid")
    if constraint_mode not in CONSTRAINT_MODES:
        raise ValueError(f"constraint_mode must be one of {CONSTRAINT_MODES}")

    deadline_tokens = deadline_classes or DEFAULT_DEADLINE_CLASSES
    node_ids = sorted(node_profiles.keys())
    declarations: List[Dict[str, Any]] = []
    payloads: List[Dict[str, Any]] = []
    combined: List[Dict[str, Any]] = []
    declaration_seeds = [int(rng.randrange(0, 2**63 - 1)) for _ in range(request_count)]
    payload_seeds = [int(rng.randrange(0, 2**63 - 1)) for _ in range(request_count)]

    for idx in range(1, request_count + 1):
        request_id = f"req-{idx:05d}"
        declaration_rng = random.Random(declaration_seeds[idx - 1])
        payload_rng = random.Random(payload_seeds[idx - 1])
        requester_id = int(declaration_rng.choice(node_ids))
        anchor_id = int(declaration_rng.choice(node_ids))
        anchor = node_profiles[anchor_id]

        anchor_families = [str(token) for token in anchor.get("families", []) if str(token).strip()]
        anchor_domains = [str(token) for token in anchor.get("domains", []) if str(token).strip()]
        anchor_tools = [str(token) for token in anchor.get("tools", []) if str(token).strip()]
        anchor_trust = str(anchor.get("trust_domain") or "public")
        anchor_latency = str(anchor.get("latency_tier") or "medium")
        anchor_specs = [str(token) for token in anchor.get("specialization_tags", []) if str(token).strip()]
        if not anchor_families or not anchor_domains or not anchor_tools:
            raise ValueError(f"anchor node profile incomplete for node {anchor_id}")

        required_family = str(declaration_rng.choice(anchor_families))
        coarse_domain = str(declaration_rng.choice(anchor_domains))

        if anchor_latency not in latency_tiers:
            raise ValueError(f"unknown anchor latency tier: {anchor_latency}")
        anchor_latency_idx = latency_tiers.index(anchor_latency)
        ordered_tools = [str(token) for token in payload_rng.sample(anchor_tools, k=len(anchor_tools))]
        primary_tool = ordered_tools[0]
        tight_tool_count = min(2, len(ordered_tools))
        tight_tools = sorted(ordered_tools[:tight_tool_count])
        if "medium" in latency_tiers:
            medium_idx = latency_tiers.index("medium")
        else:
            medium_idx = max(0, len(latency_tiers) // 2)

        if constraint_mode == "loose":
            max_latency_tier = str(latency_tiers[-1])
            required_tools = []
            if anchor_tools and payload_rng.random() >= 0.5:
                required_tools = [primary_tool]
            required_trust_domain: str | None = None
            optional_specialization_preference = None
            specialization_hard = False
        elif constraint_mode == "medium":
            max_latency_idx = max(anchor_latency_idx, medium_idx)
            max_latency_tier = str(latency_tiers[max_latency_idx])
            required_tools = [primary_tool]
            required_trust_domain = anchor_trust
            optional_specialization_preference = None
            if anchor_specs and payload_rng.random() <= specialization_preference_prob:
                optional_specialization_preference = str(payload_rng.choice(anchor_specs))
            specialization_hard = False
        else:
            max_latency_tier = str(latency_tiers[anchor_latency_idx])
            required_tools = tight_tools
            required_trust_domain = anchor_trust
            if anchor_specs:
                optional_specialization_preference = str(payload_rng.choice(anchor_specs))
                specialization_hard = True
            else:
                optional_specialization_preference = None
                specialization_hard = False

        created_ts = now_ts()
        payload_id = f"payload-{request_id}"
        declaration_row = {
            "request_id": request_id,
            "requester_id": requester_id,
            "required_family": required_family,
            "coarse_domain": coarse_domain,
            "payload_ref": payload_id,
            "payload_id": payload_id,
            "payload_bytes": int(payload_bytes),
            "created_ts": created_ts,
            "created_ts_iso": now_iso(created_ts),
        }
        payload_row = {
            "request_id": request_id,
            "required_tools": required_tools,
            "required_trust_domain": required_trust_domain,
            "max_latency_tier": max_latency_tier,
            "optional_specialization_preference": optional_specialization_preference,
            "specialization_hard": specialization_hard,
            "constraint_mode": constraint_mode,
            "maybe_deadline_class": str(payload_rng.choice(deadline_tokens)),
        }
        combined_row = {
            **declaration_row,
            **payload_row,
            "anchor_provider_id": anchor_id,
        }
        declarations.append(declaration_row)
        payloads.append(payload_row)
        combined.append(combined_row)

    return declarations, payloads, combined


def coarse_match(node_profile: Dict[str, Any], declaration: Dict[str, Any]) -> bool:
    required_family = str(declaration.get("required_family") or "")
    coarse_domain = str(declaration.get("coarse_domain") or "")
    node_families = [str(token) for token in node_profile.get("families", [])]
    node_domains = [str(token) for token in node_profile.get("domains", [])]
    return required_family in node_families and coarse_domain in node_domains


def latency_tier_allowed(node_tier: str, max_tier: str, latency_tiers: List[str]) -> bool:
    if node_tier not in latency_tiers or max_tier not in latency_tiers:
        return False
    return latency_tiers.index(node_tier) <= latency_tiers.index(max_tier)


def full_match(
    node_profile: Dict[str, Any],
    payload: Dict[str, Any],
    *,
    latency_tiers: List[str],
    specialization_hard: bool,
) -> Tuple[bool, Dict[str, Any]]:
    required_tools = [str(token) for token in payload.get("required_tools", [])]
    raw_trust_domain = payload.get("required_trust_domain")
    required_trust_domain = str(raw_trust_domain or "")
    max_latency_tier = str(payload.get("max_latency_tier") or "")
    optional_specialization_preference = payload.get("optional_specialization_preference")
    specialization_required = isinstance(optional_specialization_preference, str) and bool(
        optional_specialization_preference
    )

    node_tools = {str(token) for token in node_profile.get("tools", [])}
    node_trust_domain = str(node_profile.get("trust_domain") or "")
    node_latency_tier = str(node_profile.get("latency_tier") or "")
    node_specializations = {str(token) for token in node_profile.get("specialization_tags", [])}

    tools_ok = set(required_tools).issubset(node_tools)
    trust_required = bool(required_trust_domain) and required_trust_domain not in {"*", "any", "ANY"}
    trust_ok = (not trust_required) or (required_trust_domain == node_trust_domain)
    latency_ok = latency_tier_allowed(node_latency_tier, max_latency_tier, latency_tiers)
    specialization_hit = (not specialization_required) or (
        str(optional_specialization_preference) in node_specializations
    )
    specialization_ok = specialization_hit if specialization_hard else True

    matched = tools_ok and trust_ok and latency_ok and specialization_ok
    evidence = {
        "tools_ok": tools_ok,
        "trust_ok": trust_ok,
        "trust_required": trust_required,
        "latency_ok": latency_ok,
        "specialization_required": specialization_required,
        "specialization_hit": specialization_hit,
        "specialization_hard": specialization_hard,
    }
    return matched, evidence


def append_event(
    *,
    path: Path,
    request_id: str,
    node_id: int,
    event_type: str,
    required_capability: str | None = None,
    node_capabilities: Any | None = None,
    payload_bytes: int | None = None,
    metadata: Dict[str, Any] | None = None,
) -> float:
    ts = now_ts()
    row: Dict[str, Any] = {
        "request_id": request_id,
        "node_id": int(node_id),
        "event_type": event_type,
        "event_ts": ts,
        "event_ts_iso": now_iso(ts),
    }
    if required_capability is not None:
        row["required_capability"] = required_capability
    if node_capabilities is not None:
        if isinstance(node_capabilities, list):
            row["node_capabilities"] = list(node_capabilities)
        else:
            row["node_capabilities"] = node_capabilities
    if payload_bytes is not None:
        row["payload_bytes"] = int(payload_bytes)
    if metadata:
        row.update(metadata)
    append_jsonl(path, row)
    return ts


def capability_distribution(node_capabilities: Dict[int, List[str]]) -> Dict[str, int]:
    distribution: Dict[str, int] = {}
    for caps in node_capabilities.values():
        for capability in caps:
            distribution[capability] = distribution.get(capability, 0) + 1
    return dict(sorted(distribution.items()))
