#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from discovery_workload_lib import (
    COARSE_MATCH_TRUE,
    CONSTRAINT_MODES,
    DECLARATION_RECEIVED,
    ELIGIBILITY_TRUE,
    FULL_MATCH_TRUE,
    MANDATORY_EVENT_TYPES,
    PAYLOAD_RECEIVED,
    PULL_SENT,
    RESPONSE_RECEIVED_BY_REQUESTER,
    RESPONSE_SENT,
    DEFAULT_CAPABILITY_TAGS,
    DEFAULT_CONSTRAINT_MODE,
    DEFAULT_DEADLINE_CLASSES,
    DEFAULT_DOMAINS,
    DEFAULT_FAMILIES,
    DEFAULT_LATENCY_TIERS,
    DEFAULT_SPECIALIZATION_TAGS,
    DEFAULT_TOOLS,
    DEFAULT_TRUST_DOMAINS,
    append_event,
    assign_node_capabilities,
    assign_node_profiles_two_stage,
    coarse_match,
    dump_json,
    ensure_dir,
    full_match,
    generate_requests,
    generate_two_stage_requests,
    normalize_string_list,
    now_iso,
    now_ts,
    stats,
    write_jsonl,
)


LEGACY_MODE = "legacy_simple"
TWO_STAGE_MODE = "deterministic_two_stage"
MATCHING_MODES = [LEGACY_MODE, TWO_STAGE_MODE]
PULL_POLICIES = ["all", "probabilistic", "topk", "deterministic_coarse_match"]


def get_json(port: int, path: str, timeout_sec: float = 5.0) -> Any:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=timeout_sec) as response:
        return json.loads(response.read().decode())


def post_json(port: int, path: str, body: Dict[str, Any], timeout_sec: float = 10.0) -> Any:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        return json.loads(response.read().decode())


def extract_task_id(row: Dict[str, Any]) -> str | None:
    direct = row.get("task_id")
    if isinstance(direct, str) and direct:
        return direct
    envelope = row.get("envelope")
    if isinstance(envelope, dict):
        nested = envelope.get("task_id")
        if isinstance(nested, str) and nested:
            return nested
    return None


def extract_detail_ref(row: Dict[str, Any]) -> str | None:
    envelope = row.get("envelope")
    if not isinstance(envelope, dict):
        return None
    detail_ref = envelope.get("detail_ref")
    if isinstance(detail_ref, str) and detail_ref:
        return detail_ref
    return None


def find_task(rows: Any, task_id: str) -> Dict[str, Any] | None:
    if not isinstance(rows, list):
        return None
    for row in rows:
        if not isinstance(row, dict):
            continue
        if extract_task_id(row) == task_id:
            return row
    return None


def fetch_tasks(port: int) -> List[Dict[str, Any]]:
    try:
        rows = get_json(port, "/v1/tasks")
    except Exception:
        return []
    if not isinstance(rows, list):
        return []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(row)
    return out


def health_ok_count(node_ports: Dict[int, int]) -> int:
    count = 0
    for port in node_ports.values():
        try:
            get_json(port, "/healthz", timeout_sec=2.0)
            count += 1
        except Exception:
            continue
    return count


def parse_csv_list(raw_value: str, arg_name: str) -> List[str]:
    tokens = normalize_string_list(chunk.strip() for chunk in str(raw_value).split(","))
    if not tokens:
        raise ValueError(f"{arg_name} must not be empty")
    return tokens


def choose_pullers(
    *,
    policy: str,
    candidates: List[int],
    rng: random.Random,
    pull_probability: float,
    pull_topk: int,
) -> List[int]:
    if policy in {"all", "deterministic_coarse_match"}:
        return sorted(candidates)
    if policy == "probabilistic":
        selected = [node_id for node_id in candidates if rng.random() <= pull_probability]
        return sorted(selected)
    if policy == "topk":
        if pull_topk <= 0:
            return []
        if len(candidates) <= pull_topk:
            return sorted(candidates)
        return sorted(rng.sample(candidates, k=pull_topk))
    raise ValueError(f"unsupported pull policy: {policy}")


def synthetic_summary(
    *,
    args: argparse.Namespace,
    reason: str,
    launch_ok: bool,
    health_ok: bool,
    bootstrap_pass: bool,
    request_count: int = 0,
) -> Dict[str, Any]:
    empty = {"count": 0}
    return {
        "label": args.label,
        "variant": args.variant,
        "matching_mode": args.matching_mode,
        "node_count": args.node_count,
        "global_topic": args.global_topic,
        "request_count": request_count,
        "repetitions": args.repetitions,
        "random_seed": args.random_seed,
        "constraint_mode": args.constraint_mode,
        "pull_policy": args.pull_policy,
        "pull_probability": args.pull_probability,
        "launch_ok": launch_ok,
        "health_ok": health_ok,
        "bootstrap_pass": bootstrap_pass,
        "bench_ok": False,
        "bench_fail_reason": reason,
        "visible_count": dict(empty),
        "eligible_count": dict(empty),
        "pulling_count": dict(empty),
        "responding_count": dict(empty),
        "coarse_match_count": dict(empty),
        "full_match_count": dict(empty),
        "actual_pull_count": dict(empty),
        "actual_response_count": dict(empty),
    }


def node_profile_summary(profile: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "families": list(profile.get("families", [])),
        "domains": list(profile.get("domains", [])),
        "tools": list(profile.get("tools", [])),
        "trust_domain": profile.get("trust_domain"),
        "latency_tier": profile.get("latency_tier"),
        "specialization_tags": list(profile.get("specialization_tags", [])),
    }


def collect_visibility(
    *,
    request_id: str,
    declaration_task_id: str,
    requester_id: int,
    node_ports: Dict[int, int],
    declaration_observe_sec: float,
    poll_sec: float,
    event_path: Path,
    node_payload_for_event: Dict[int, Any],
    common_metadata: Dict[str, Any],
) -> Tuple[Dict[int, float], Optional[str]]:
    visible_nodes: Dict[int, float] = {}
    detail_ref: Optional[str] = None
    declaration_deadline = time.time() + declaration_observe_sec
    last_new_visible_at = time.time()
    while time.time() < declaration_deadline:
        for node_id, port in node_ports.items():
            if node_id in visible_nodes:
                continue
            rows = fetch_tasks(port)
            task_row = find_task(rows, declaration_task_id)
            if task_row is None:
                continue
            seen_ts = now_ts()
            visible_nodes[node_id] = seen_ts
            append_event(
                path=event_path,
                request_id=request_id,
                node_id=node_id,
                event_type=DECLARATION_RECEIVED,
                node_capabilities=node_payload_for_event.get(node_id),
                metadata={
                    **common_metadata,
                    "requester_id": requester_id,
                    "declaration_task_id": declaration_task_id,
                },
            )
            if detail_ref is None:
                candidate = extract_detail_ref(task_row)
                if candidate:
                    detail_ref = candidate
            last_new_visible_at = time.time()
        now = time.time()
        if len(visible_nodes) == len(node_ports) and (now - last_new_visible_at) >= 1.0:
            break
        time.sleep(poll_sec)
    return visible_nodes, detail_ref


def run_legacy_mode(
    *,
    args: argparse.Namespace,
    rng: random.Random,
    event_path: Path,
    requests_path: Path,
    timeline_path: Path,
    node_caps_path: Path,
    node_ports: Dict[int, int],
) -> List[Dict[str, Any]]:
    capability_tags = parse_csv_list(args.capability_tags, "capability-tags")
    node_capabilities = assign_node_capabilities(
        node_count=args.node_count,
        capability_tags=capability_tags,
        capabilities_per_node=args.capabilities_per_node,
        rng=rng,
    )
    node_caps_payload = {
        "matching_mode": LEGACY_MODE,
        "random_seed": args.random_seed,
        "capability_tags": capability_tags,
        "capabilities_per_node": args.capabilities_per_node,
        "node_capabilities": {str(node_id): caps for node_id, caps in sorted(node_capabilities.items())},
    }
    dump_json(node_caps_path, node_caps_payload)

    requests: List[Dict[str, Any]] = []
    for repetition in range(1, args.repetitions + 1):
        batch = generate_requests(
            node_count=args.node_count,
            request_count=args.requests,
            capability_tags=capability_tags,
            payload_bytes=args.payload_bytes,
            rng=rng,
        )
        for row in batch:
            row["request_id"] = f"rep{repetition:02d}-{row['request_id']}"
            row["repetition"] = repetition
            requests.append(row)
    write_jsonl(requests_path, requests)

    request_timelines: List[Dict[str, Any]] = []
    for request_row in requests:
        request_id = str(request_row["request_id"])
        requester_id = int(request_row["requester_id"])
        requester_port = node_ports[requester_id]
        required_capability = str(request_row["required_capability"])
        payload_bytes = int(request_row["payload_bytes"])
        declaration_task_id = f"dw-decl-{request_id}"
        declaration_body = {
            "task_id": declaration_task_id,
            "taxonomy": "ioa.discovery.declaration",
            "topics": [args.global_topic],
            "summary": f"discovery declaration {request_id}",
            "detail": {
                "kind": "discovery_declaration",
                "request_id": request_id,
                "requester_id": requester_id,
                "required_capability": required_capability,
                "payload_id": request_row["payload_id"],
                "payload_bytes": payload_bytes,
            },
            "conf": 900,
            "ttl_sec": 3600,
        }

        declaration_sent_ts = now_ts()
        try:
            publish_result = post_json(requester_port, "/v1/tasks/publish", declaration_body)
        except Exception as exc:
            request_timelines.append(
                {
                    "request_id": request_id,
                    "requester_id": requester_id,
                    "required_capability": required_capability,
                    "declaration_task_id": declaration_task_id,
                    "declaration_sent_ts": declaration_sent_ts,
                    "first_visible_ts": None,
                    "last_visible_ts": None,
                    "first_pull_ts": None,
                    "first_response_ts": None,
                    "response_count_total": 0,
                    "visible_count": 0,
                    "eligible_count": 0,
                    "pulling_count": 0,
                    "responding_count": 0,
                    "coarse_match_count": 0,
                    "full_match_count": 0,
                    "actual_pull_count": 0,
                    "actual_response_count": 0,
                    "request_fail_reason": f"publish_failed:{type(exc).__name__}",
                }
            )
            continue

        detail_ref = None
        if isinstance(publish_result, dict):
            raw_ref = publish_result.get("detail_ref")
            if isinstance(raw_ref, str) and raw_ref:
                detail_ref = raw_ref

        visible_nodes, visible_detail_ref = collect_visibility(
            request_id=request_id,
            declaration_task_id=declaration_task_id,
            requester_id=requester_id,
            node_ports=node_ports,
            declaration_observe_sec=float(args.declaration_observe_sec),
            poll_sec=float(args.poll_sec),
            event_path=event_path,
            node_payload_for_event=node_capabilities,
            common_metadata={
                "required_capability": required_capability,
                "payload_bytes": payload_bytes,
            },
        )
        if detail_ref is None:
            detail_ref = visible_detail_ref

        eligible_nodes: Set[int] = set()
        for node_id in visible_nodes:
            if required_capability in node_capabilities[node_id]:
                eligible_nodes.add(node_id)
                append_event(
                    path=event_path,
                    request_id=request_id,
                    node_id=node_id,
                    event_type=ELIGIBILITY_TRUE,
                    required_capability=required_capability,
                    node_capabilities=node_capabilities[node_id],
                    payload_bytes=payload_bytes,
                    metadata={
                        "requester_id": requester_id,
                        "declaration_task_id": declaration_task_id,
                    },
                )

        pull_candidates = [node_id for node_id in sorted(eligible_nodes) if node_id != requester_id]
        selected_pullers = choose_pullers(
            policy=args.pull_policy,
            candidates=pull_candidates,
            rng=rng,
            pull_probability=float(args.pull_probability),
            pull_topk=args.pull_topk,
        )

        pulling_nodes: Set[int] = set()
        responding_nodes: Set[int] = set()
        first_pull_ts: Optional[float] = None
        first_response_ts: Optional[float] = None
        response_task_ids: Dict[int, str] = {}
        request_fail_reason = ""

        if detail_ref is None:
            request_fail_reason = "detail_ref_missing"
        else:
            for node_id in selected_pullers:
                pull_ts = append_event(
                    path=event_path,
                    request_id=request_id,
                    node_id=node_id,
                    event_type=PULL_SENT,
                    required_capability=required_capability,
                    node_capabilities=node_capabilities[node_id],
                    payload_bytes=payload_bytes,
                    metadata={
                        "requester_id": requester_id,
                        "detail_ref": detail_ref,
                        "declaration_task_id": declaration_task_id,
                    },
                )
                pulling_nodes.add(node_id)
                if first_pull_ts is None or pull_ts < first_pull_ts:
                    first_pull_ts = pull_ts

                try:
                    post_json(node_ports[node_id], "/v1/fetch", {"detail_ref": detail_ref})
                except Exception:
                    continue

                append_event(
                    path=event_path,
                    request_id=request_id,
                    node_id=node_id,
                    event_type=PAYLOAD_RECEIVED,
                    required_capability=required_capability,
                    node_capabilities=node_capabilities[node_id],
                    payload_bytes=payload_bytes,
                    metadata={
                        "requester_id": requester_id,
                        "detail_ref": detail_ref,
                        "declaration_task_id": declaration_task_id,
                    },
                )

                response_task_id = f"dw-resp-{request_id}-n{node_id:03d}"
                response_body = {
                    "task_id": response_task_id,
                    "taxonomy": "ioa.discovery.response",
                    "topics": [args.global_topic],
                    "summary": f"discovery response {request_id} from node {node_id}",
                    "detail": {
                        "kind": "discovery_response",
                        "request_id": request_id,
                        "requester_id": requester_id,
                        "responder_node_id": node_id,
                        "required_capability": required_capability,
                    },
                    "conf": 900,
                    "ttl_sec": 3600,
                }
                try:
                    post_json(node_ports[node_id], "/v1/tasks/publish", response_body)
                except Exception:
                    continue

                append_event(
                    path=event_path,
                    request_id=request_id,
                    node_id=node_id,
                    event_type=RESPONSE_SENT,
                    required_capability=required_capability,
                    node_capabilities=node_capabilities[node_id],
                    payload_bytes=payload_bytes,
                    metadata={
                        "requester_id": requester_id,
                        "response_task_id": response_task_id,
                    },
                )
                response_task_ids[node_id] = response_task_id

        received_response_nodes: Set[int] = set()
        response_deadline = time.time() + float(args.response_observe_sec)
        while time.time() < response_deadline and response_task_ids:
            requester_rows = fetch_tasks(requester_port)
            unresolved = [node_id for node_id in response_task_ids if node_id not in received_response_nodes]
            for responder_id in unresolved:
                response_task_id = response_task_ids[responder_id]
                if find_task(requester_rows, response_task_id) is None:
                    continue
                recv_ts = append_event(
                    path=event_path,
                    request_id=request_id,
                    node_id=responder_id,
                    event_type=RESPONSE_RECEIVED_BY_REQUESTER,
                    required_capability=required_capability,
                    node_capabilities=node_capabilities[responder_id],
                    payload_bytes=payload_bytes,
                    metadata={
                        "requester_id": requester_id,
                        "response_task_id": response_task_id,
                    },
                )
                received_response_nodes.add(responder_id)
                responding_nodes.add(responder_id)
                if first_response_ts is None or recv_ts < first_response_ts:
                    first_response_ts = recv_ts
            if len(received_response_nodes) == len(response_task_ids):
                break
            time.sleep(float(args.poll_sec))

        visible_ts = list(visible_nodes.values())
        request_timelines.append(
            {
                "request_id": request_id,
                "requester_id": requester_id,
                "required_capability": required_capability,
                "declaration_task_id": declaration_task_id,
                "declaration_sent_ts": declaration_sent_ts,
                "first_visible_ts": min(visible_ts) if visible_ts else None,
                "last_visible_ts": max(visible_ts) if visible_ts else None,
                "first_pull_ts": first_pull_ts,
                "first_response_ts": first_response_ts,
                "response_count_total": len(received_response_nodes),
                "visible_count": len(visible_nodes),
                "eligible_count": len(eligible_nodes),
                "pulling_count": len(pulling_nodes),
                "responding_count": len(responding_nodes),
                "coarse_match_count": len(eligible_nodes),
                "full_match_count": len(responding_nodes),
                "actual_pull_count": len(pulling_nodes),
                "actual_response_count": len(responding_nodes),
                "request_fail_reason": request_fail_reason,
                "detail_ref": detail_ref,
                "payload_bytes": payload_bytes,
            }
        )
        time.sleep(float(args.request_gap_sec))

    write_jsonl(timeline_path, request_timelines)
    return request_timelines


def run_two_stage_mode(
    *,
    args: argparse.Namespace,
    rng: random.Random,
    event_path: Path,
    requests_path: Path,
    declaration_requests_path: Path,
    payload_requests_path: Path,
    timeline_path: Path,
    node_caps_path: Path,
    node_ports: Dict[int, int],
) -> List[Dict[str, Any]]:
    if args.pull_policy != "deterministic_coarse_match":
        raise SystemExit("two-stage mode requires pull-policy=deterministic_coarse_match")

    families = parse_csv_list(args.families, "families")
    domains = parse_csv_list(args.domains, "domains")
    tools = parse_csv_list(args.tools, "tools")
    trust_domains = parse_csv_list(args.trust_domains, "trust-domains")
    latency_tiers = parse_csv_list(args.latency_tiers, "latency-tiers")
    specialization_tags = parse_csv_list(args.specialization_tags, "specialization-tags")
    deadline_classes = parse_csv_list(args.deadline_classes, "deadline-classes")
    specialization_hard_default = bool(int(args.specialization_hard))

    node_profiles = assign_node_profiles_two_stage(
        node_count=args.node_count,
        families=families,
        domains=domains,
        tools=tools,
        trust_domains=trust_domains,
        latency_tiers=latency_tiers,
        specialization_tags=specialization_tags,
        rng=rng,
        families_per_node=args.families_per_node,
        domain_min=args.domains_min,
        domain_max=args.domains_max,
        tool_min=args.tools_min,
        tool_max=args.tools_max,
        specialization_max=args.specialization_max,
    )
    node_payload_for_event = {node_id: node_profile_summary(profile) for node_id, profile in node_profiles.items()}
    dump_json(
        node_caps_path,
        {
            "matching_mode": TWO_STAGE_MODE,
            "random_seed": args.random_seed,
            "families": families,
            "domains": domains,
            "tools": tools,
            "trust_domains": trust_domains,
            "latency_tiers": latency_tiers,
            "specialization_tags": specialization_tags,
            "families_per_node": args.families_per_node,
            "domains_min": args.domains_min,
            "domains_max": args.domains_max,
            "tools_min": args.tools_min,
            "tools_max": args.tools_max,
            "specialization_max": args.specialization_max,
            "constraint_mode": str(args.constraint_mode),
            "node_capabilities": {str(node_id): node_payload_for_event[node_id] for node_id in sorted(node_profiles)},
        },
    )

    declarations: List[Dict[str, Any]] = []
    payloads: List[Dict[str, Any]] = []
    requests: List[Dict[str, Any]] = []
    for repetition in range(1, args.repetitions + 1):
        batch_decl, batch_payload, batch_combined = generate_two_stage_requests(
            node_profiles=node_profiles,
            request_count=args.requests,
            payload_bytes=args.payload_bytes,
            latency_tiers=latency_tiers,
            rng=rng,
            required_tools_min=args.required_tools_min,
            required_tools_max=args.required_tools_max,
            specialization_preference_prob=float(args.specialization_preference_prob),
            deadline_classes=deadline_classes,
            constraint_mode=str(args.constraint_mode),
        )
        for declaration_row, payload_row, combined_row in zip(batch_decl, batch_payload, batch_combined):
            old_request_id = str(combined_row["request_id"])
            request_id = f"rep{repetition:02d}-{old_request_id}"
            payload_id = f"payload-{request_id}"

            declaration_row["request_id"] = request_id
            declaration_row["payload_ref"] = payload_id
            declaration_row["payload_id"] = payload_id
            declaration_row["repetition"] = repetition

            payload_row["request_id"] = request_id
            payload_row["payload_ref"] = payload_id
            payload_row["payload_id"] = payload_id
            payload_row["repetition"] = repetition

            combined_row["request_id"] = request_id
            combined_row["payload_ref"] = payload_id
            combined_row["payload_id"] = payload_id
            combined_row["repetition"] = repetition
            combined_row["matching_mode"] = TWO_STAGE_MODE
            combined_row["constraint_mode"] = str(args.constraint_mode)

            declarations.append(declaration_row)
            payloads.append(payload_row)
            requests.append(combined_row)

    write_jsonl(requests_path, requests)
    write_jsonl(declaration_requests_path, declarations)
    write_jsonl(payload_requests_path, payloads)
    payload_map = {str(row["request_id"]): row for row in payloads}

    request_timelines: List[Dict[str, Any]] = []
    for request_row in requests:
        request_id = str(request_row["request_id"])
        payload_row = payload_map[request_id]
        requester_id = int(request_row["requester_id"])
        requester_port = node_ports[requester_id]
        declaration_task_id = f"dw-decl-{request_id}"
        payload_bytes = int(request_row["payload_bytes"])
        common_metadata = {
            "required_family": request_row["required_family"],
            "coarse_domain": request_row["coarse_domain"],
            "payload_ref": request_row["payload_ref"],
            "payload_id": request_row["payload_id"],
            "payload_bytes": payload_bytes,
            "required_tools": list(payload_row.get("required_tools", [])),
            "required_trust_domain": payload_row.get("required_trust_domain"),
            "max_latency_tier": payload_row.get("max_latency_tier"),
            "optional_specialization_preference": payload_row.get("optional_specialization_preference"),
            "specialization_hard": payload_row.get("specialization_hard", specialization_hard_default),
            "constraint_mode": payload_row.get("constraint_mode", args.constraint_mode),
            "maybe_deadline_class": payload_row.get("maybe_deadline_class"),
        }

        declaration_body = {
            "task_id": declaration_task_id,
            "taxonomy": "ioa.discovery.declaration",
            "topics": [args.global_topic],
            "summary": f"discovery declaration {request_id}",
            "detail": {
                "kind": "discovery_declaration_two_stage",
                "request_id": request_id,
                "requester_id": requester_id,
                "required_family": request_row["required_family"],
                "coarse_domain": request_row["coarse_domain"],
                "payload_ref": request_row["payload_ref"],
                "payload_bytes": payload_bytes,
                "created_ts": request_row["created_ts"],
            },
            "conf": 900,
            "ttl_sec": 3600,
        }

        declaration_sent_ts = now_ts()
        try:
            publish_result = post_json(requester_port, "/v1/tasks/publish", declaration_body)
        except Exception as exc:
            request_timelines.append(
                {
                    "request_id": request_id,
                    "requester_id": requester_id,
                    "required_family": request_row["required_family"],
                    "coarse_domain": request_row["coarse_domain"],
                    "declaration_task_id": declaration_task_id,
                    "declaration_sent_ts": declaration_sent_ts,
                    "first_visible_ts": None,
                    "last_visible_ts": None,
                    "first_pull_ts": None,
                    "first_response_ts": None,
                    "response_count_total": 0,
                    "visible_count": 0,
                    "coarse_match_count": 0,
                    "full_match_count": 0,
                    "actual_pull_count": 0,
                    "actual_response_count": 0,
                    "eligible_count": 0,
                    "pulling_count": 0,
                    "responding_count": 0,
                    "request_fail_reason": f"publish_failed:{type(exc).__name__}",
                }
            )
            continue

        detail_ref = None
        if isinstance(publish_result, dict):
            raw_ref = publish_result.get("detail_ref")
            if isinstance(raw_ref, str) and raw_ref:
                detail_ref = raw_ref

        visible_nodes, visible_detail_ref = collect_visibility(
            request_id=request_id,
            declaration_task_id=declaration_task_id,
            requester_id=requester_id,
            node_ports=node_ports,
            declaration_observe_sec=float(args.declaration_observe_sec),
            poll_sec=float(args.poll_sec),
            event_path=event_path,
            node_payload_for_event=node_payload_for_event,
            common_metadata=common_metadata,
        )
        if detail_ref is None:
            detail_ref = visible_detail_ref

        coarse_nodes: Set[int] = set()
        for node_id in sorted(visible_nodes):
            if bool(int(args.exclude_requester_from_match_count)) and node_id == requester_id:
                continue
            node_profile = node_profiles[node_id]
            if not coarse_match(node_profile, request_row):
                continue
            coarse_nodes.add(node_id)
            append_event(
                path=event_path,
                request_id=request_id,
                node_id=node_id,
                event_type=COARSE_MATCH_TRUE,
                node_capabilities=node_payload_for_event[node_id],
                metadata={
                    **common_metadata,
                    "requester_id": requester_id,
                    "declaration_task_id": declaration_task_id,
                },
            )

        full_match_nodes: Set[int] = set()
        for node_id in sorted(coarse_nodes):
            specialization_hard = bool(payload_row.get("specialization_hard", specialization_hard_default))
            matched, evidence = full_match(
                node_profiles[node_id],
                payload_row,
                latency_tiers=latency_tiers,
                specialization_hard=specialization_hard,
            )
            if not matched:
                continue
            full_match_nodes.add(node_id)
            append_event(
                path=event_path,
                request_id=request_id,
                node_id=node_id,
                event_type=FULL_MATCH_TRUE,
                node_capabilities=node_payload_for_event[node_id],
                metadata={
                    **common_metadata,
                    "requester_id": requester_id,
                    "full_match_evidence": evidence,
                    "specialization_hard": specialization_hard,
                },
            )

        selected_pullers = choose_pullers(
            policy=args.pull_policy,
            candidates=sorted(coarse_nodes),
            rng=rng,
            pull_probability=float(args.pull_probability),
            pull_topk=int(args.pull_topk),
        )

        pulling_nodes: Set[int] = set()
        responding_nodes: Set[int] = set()
        first_pull_ts: Optional[float] = None
        first_response_ts: Optional[float] = None
        response_task_ids: Dict[int, str] = {}
        request_fail_reason = ""

        if detail_ref is None:
            request_fail_reason = "detail_ref_missing"
        else:
            for node_id in selected_pullers:
                pull_ts = append_event(
                    path=event_path,
                    request_id=request_id,
                    node_id=node_id,
                    event_type=PULL_SENT,
                    node_capabilities=node_payload_for_event[node_id],
                    metadata={
                        **common_metadata,
                        "requester_id": requester_id,
                        "detail_ref": detail_ref,
                        "declaration_task_id": declaration_task_id,
                    },
                )
                pulling_nodes.add(node_id)
                if first_pull_ts is None or pull_ts < first_pull_ts:
                    first_pull_ts = pull_ts

                try:
                    post_json(node_ports[node_id], "/v1/fetch", {"detail_ref": detail_ref})
                except Exception:
                    continue

                append_event(
                    path=event_path,
                    request_id=request_id,
                    node_id=node_id,
                    event_type=PAYLOAD_RECEIVED,
                    node_capabilities=node_payload_for_event[node_id],
                    metadata={
                        **common_metadata,
                        "requester_id": requester_id,
                        "detail_ref": detail_ref,
                    },
                )

                if node_id not in full_match_nodes:
                    continue

                response_task_id = f"dw-resp-{request_id}-n{node_id:03d}"
                response_body = {
                    "task_id": response_task_id,
                    "taxonomy": "ioa.discovery.response",
                    "topics": [args.global_topic],
                    "summary": f"discovery response {request_id} from node {node_id}",
                    "detail": {
                        "kind": "discovery_response_two_stage",
                        "request_id": request_id,
                        "requester_id": requester_id,
                        "responder_node_id": node_id,
                        "required_family": request_row["required_family"],
                        "coarse_domain": request_row["coarse_domain"],
                        "required_tools": payload_row["required_tools"],
                        "required_trust_domain": payload_row["required_trust_domain"],
                        "max_latency_tier": payload_row["max_latency_tier"],
                        "optional_specialization_preference": payload_row["optional_specialization_preference"],
                    },
                    "conf": 900,
                    "ttl_sec": 3600,
                }
                try:
                    post_json(node_ports[node_id], "/v1/tasks/publish", response_body)
                except Exception:
                    continue

                append_event(
                    path=event_path,
                    request_id=request_id,
                    node_id=node_id,
                    event_type=RESPONSE_SENT,
                    node_capabilities=node_payload_for_event[node_id],
                    metadata={
                        **common_metadata,
                        "requester_id": requester_id,
                        "response_task_id": response_task_id,
                    },
                )
                response_task_ids[node_id] = response_task_id

        received_response_nodes: Set[int] = set()
        response_deadline = time.time() + float(args.response_observe_sec)
        while time.time() < response_deadline and response_task_ids:
            requester_rows = fetch_tasks(requester_port)
            unresolved = [node_id for node_id in response_task_ids if node_id not in received_response_nodes]
            for responder_id in unresolved:
                response_task_id = response_task_ids[responder_id]
                if find_task(requester_rows, response_task_id) is None:
                    continue
                recv_ts = append_event(
                    path=event_path,
                    request_id=request_id,
                    node_id=responder_id,
                    event_type=RESPONSE_RECEIVED_BY_REQUESTER,
                    node_capabilities=node_payload_for_event[responder_id],
                    metadata={
                        **common_metadata,
                        "requester_id": requester_id,
                        "response_task_id": response_task_id,
                    },
                )
                received_response_nodes.add(responder_id)
                responding_nodes.add(responder_id)
                if first_response_ts is None or recv_ts < first_response_ts:
                    first_response_ts = recv_ts
            if len(received_response_nodes) == len(response_task_ids):
                break
            time.sleep(float(args.poll_sec))

        visible_ts = list(visible_nodes.values())
        request_timelines.append(
            {
                "request_id": request_id,
                "requester_id": requester_id,
                "required_family": request_row["required_family"],
                "coarse_domain": request_row["coarse_domain"],
                "payload_ref": request_row["payload_ref"],
                "payload_id": request_row["payload_id"],
                "declaration_task_id": declaration_task_id,
                "declaration_sent_ts": declaration_sent_ts,
                "first_visible_ts": min(visible_ts) if visible_ts else None,
                "last_visible_ts": max(visible_ts) if visible_ts else None,
                "first_pull_ts": first_pull_ts,
                "first_response_ts": first_response_ts,
                "response_count_total": len(received_response_nodes),
                "visible_count": len(visible_nodes),
                "coarse_match_count": len(coarse_nodes),
                "full_match_count": len(full_match_nodes),
                "actual_pull_count": len(pulling_nodes),
                "actual_response_count": len(responding_nodes),
                "eligible_count": len(coarse_nodes),
                "pulling_count": len(pulling_nodes),
                "responding_count": len(responding_nodes),
                "request_fail_reason": request_fail_reason,
                "detail_ref": detail_ref,
                "payload_bytes": payload_bytes,
                "constraint_mode": payload_row.get("constraint_mode", args.constraint_mode),
            }
        )
        time.sleep(float(args.request_gap_sec))

    write_jsonl(timeline_path, request_timelines)
    return request_timelines


def build_summary(
    *,
    args: argparse.Namespace,
    request_timelines: List[Dict[str, Any]],
    launch_ok: bool,
    health_ok: bool,
    bootstrap_pass: bool,
    healthz_ok_count_runtime: int,
    started_at: float,
    node_specific: Dict[str, Any],
) -> Dict[str, Any]:
    visible_values = [int(row.get("visible_count", 0)) for row in request_timelines]
    coarse_values = [int(row.get("coarse_match_count", row.get("eligible_count", 0))) for row in request_timelines]
    full_values = [int(row.get("full_match_count", row.get("responding_count", 0))) for row in request_timelines]
    actual_pull_values = [int(row.get("actual_pull_count", row.get("pulling_count", 0))) for row in request_timelines]
    actual_response_values = [
        int(row.get("actual_response_count", row.get("responding_count", 0))) for row in request_timelines
    ]
    request_fail_count = sum(1 for row in request_timelines if str(row.get("request_fail_reason") or ""))

    return {
        "label": args.label,
        "variant": args.variant,
        "matching_mode": args.matching_mode,
        "node_count": args.node_count,
        "global_topic": args.global_topic,
        "request_count": len(request_timelines),
        "repetitions": args.repetitions,
        "random_seed": args.random_seed,
        "constraint_mode": args.constraint_mode,
        "pull_policy": args.pull_policy,
        "pull_probability": args.pull_probability,
        "pull_topk": args.pull_topk,
        "launch_ok": launch_ok,
        "health_ok": health_ok and (healthz_ok_count_runtime == args.node_count),
        "bootstrap_pass": bootstrap_pass,
        "bench_ok": True,
        "bench_fail_reason": "",
        "healthz_ok_count_runtime": healthz_ok_count_runtime,
        "request_fail_count": request_fail_count,
        "request_success_count": len(request_timelines) - request_fail_count,
        "visible_count": stats(visible_values),
        "eligible_count": stats(coarse_values),
        "pulling_count": stats(actual_pull_values),
        "responding_count": stats(actual_response_values),
        "coarse_match_count": stats(coarse_values),
        "full_match_count": stats(full_values),
        "actual_pull_count": stats(actual_pull_values),
        "actual_response_count": stats(actual_response_values),
        "duration_sec": round(now_ts() - started_at, 3),
        **node_specific,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--label", default="workload_char")
    parser.add_argument("--variant", default="ablation_no_semantic_public_topic")
    parser.add_argument("--matching-mode", choices=MATCHING_MODES, default=LEGACY_MODE)
    parser.add_argument("--node-count", type=int, default=100)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--payload-bytes", type=int, default=32768)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--random-seed", type=int, default=20260402)
    parser.add_argument("--global-topic", default="openagent/v1/general")
    parser.add_argument("--api-base", type=int, default=8500)
    parser.add_argument("--declaration-observe-sec", type=float, default=30.0)
    parser.add_argument("--response-observe-sec", type=float, default=20.0)
    parser.add_argument("--poll-sec", type=float, default=0.5)
    parser.add_argument("--request-gap-sec", type=float, default=0.2)

    parser.add_argument("--capability-tags", default=",".join(DEFAULT_CAPABILITY_TAGS))
    parser.add_argument("--capabilities-per-node", type=int, default=2)

    parser.add_argument("--families", default=",".join(DEFAULT_FAMILIES))
    parser.add_argument("--domains", default=",".join(DEFAULT_DOMAINS))
    parser.add_argument("--tools", default=",".join(DEFAULT_TOOLS))
    parser.add_argument("--trust-domains", default=",".join(DEFAULT_TRUST_DOMAINS))
    parser.add_argument("--latency-tiers", default=",".join(DEFAULT_LATENCY_TIERS))
    parser.add_argument("--specialization-tags", default=",".join(DEFAULT_SPECIALIZATION_TAGS))
    parser.add_argument("--deadline-classes", default=",".join(DEFAULT_DEADLINE_CLASSES))
    parser.add_argument("--families-per-node", type=int, default=2)
    parser.add_argument("--domains-min", type=int, default=1)
    parser.add_argument("--domains-max", type=int, default=2)
    parser.add_argument("--tools-min", type=int, default=1)
    parser.add_argument("--tools-max", type=int, default=3)
    parser.add_argument("--specialization-max", type=int, default=2)
    parser.add_argument("--required-tools-min", type=int, default=1)
    parser.add_argument("--required-tools-max", type=int, default=2)
    parser.add_argument("--specialization-preference-prob", type=float, default=0.55)
    parser.add_argument("--specialization-hard", type=int, choices=[0, 1], default=1)
    parser.add_argument("--constraint-mode", choices=CONSTRAINT_MODES, default=DEFAULT_CONSTRAINT_MODE)
    parser.add_argument("--exclude-requester-from-match-count", type=int, choices=[0, 1], default=1)

    parser.add_argument("--pull-policy", choices=PULL_POLICIES, default="probabilistic")
    parser.add_argument("--pull-probability", type=float, default=0.30)
    parser.add_argument("--pull-topk", type=int, default=5)
    args = parser.parse_args()

    if args.node_count < 2:
        raise SystemExit("node-count must be >= 2")
    if args.requests < 1:
        raise SystemExit("requests must be >= 1")
    if args.repetitions < 1:
        raise SystemExit("repetitions must be >= 1")
    if args.payload_bytes < 1:
        raise SystemExit("payload-bytes must be >= 1")
    if not 0.0 <= float(args.pull_probability) <= 1.0:
        raise SystemExit("pull-probability must be in [0, 1]")
    if args.constraint_mode not in CONSTRAINT_MODES:
        raise SystemExit(f"constraint-mode must be one of {CONSTRAINT_MODES}")

    run_root = Path(args.root).expanduser()
    configs_dir = ensure_dir(run_root / "configs")
    logs_dir = ensure_dir(run_root / "logs")
    metrics_dir = ensure_dir(run_root / "metrics")
    results_dir = ensure_dir(run_root / "results")
    summaries_dir = ensure_dir(run_root / "summaries")

    event_path = logs_dir / "events.jsonl"
    requests_path = results_dir / "requests.jsonl"
    declaration_requests_path = results_dir / "requests_declaration.jsonl"
    payload_requests_path = results_dir / "requests_payload.jsonl"
    node_caps_path = results_dir / "node_capabilities.json"
    timeline_path = results_dir / "request_timeline.jsonl"
    config_snapshot_path = configs_dir / "runtime_config.json"
    runtime_metrics_path = metrics_dir / "runtime_metrics.json"
    runtime_summary_path = summaries_dir / "runtime_summary.json"

    node_ports = {node_id: args.api_base + node_id - 1 for node_id in range(1, args.node_count + 1)}
    healthz_ok = health_ok_count(node_ports)

    config_snapshot = {
        "generated_at": now_iso(),
        "label": args.label,
        "variant": args.variant,
        "matching_mode": args.matching_mode,
        "node_count": args.node_count,
        "requests": args.requests,
        "repetitions": args.repetitions,
        "payload_bytes": args.payload_bytes,
        "global_topic": args.global_topic,
        "api_base": args.api_base,
        "declaration_observe_sec": args.declaration_observe_sec,
        "response_observe_sec": args.response_observe_sec,
        "poll_sec": args.poll_sec,
        "request_gap_sec": args.request_gap_sec,
        "random_seed": args.random_seed,
        "capability_tags": parse_csv_list(args.capability_tags, "capability-tags"),
        "capabilities_per_node": args.capabilities_per_node,
        "families": parse_csv_list(args.families, "families"),
        "domains": parse_csv_list(args.domains, "domains"),
        "tools": parse_csv_list(args.tools, "tools"),
        "trust_domains": parse_csv_list(args.trust_domains, "trust-domains"),
        "latency_tiers": parse_csv_list(args.latency_tiers, "latency-tiers"),
        "specialization_tags": parse_csv_list(args.specialization_tags, "specialization-tags"),
        "deadline_classes": parse_csv_list(args.deadline_classes, "deadline-classes"),
        "families_per_node": args.families_per_node,
        "domains_min": args.domains_min,
        "domains_max": args.domains_max,
        "tools_min": args.tools_min,
        "tools_max": args.tools_max,
        "specialization_max": args.specialization_max,
        "required_tools_min": args.required_tools_min,
        "required_tools_max": args.required_tools_max,
        "specialization_preference_prob": args.specialization_preference_prob,
        "specialization_hard": bool(int(args.specialization_hard)),
        "constraint_mode": args.constraint_mode,
        "exclude_requester_from_match_count": bool(int(args.exclude_requester_from_match_count)),
        "pull_policy": args.pull_policy,
        "pull_probability": args.pull_probability,
        "pull_topk": args.pull_topk,
        "mandatory_event_types": sorted(MANDATORY_EVENT_TYPES),
    }
    dump_json(config_snapshot_path, config_snapshot)

    bootstrap_summary_path = summaries_dir / "bootstrap_summary.json"
    if not bootstrap_summary_path.exists():
        summary = synthetic_summary(
            args=args,
            reason="bootstrap_summary_missing",
            launch_ok=False,
            health_ok=False,
            bootstrap_pass=False,
        )
        dump_json(runtime_metrics_path, {"summary": summary})
        dump_json(runtime_summary_path, summary)
        print(json.dumps(summary, indent=2))
        return

    bootstrap_summary = json.loads(bootstrap_summary_path.read_text(encoding="utf-8"))
    launch_ok = bool(bootstrap_summary.get("launch_ok"))
    health_ok = bool(bootstrap_summary.get("health_ok"))
    bootstrap_pass = bool(bootstrap_summary.get("bootstrap_pass"))
    if not bootstrap_pass:
        summary = synthetic_summary(
            args=args,
            reason=bootstrap_summary.get("bootstrap_fail_reason") or "bootstrap_not_passed",
            launch_ok=launch_ok,
            health_ok=health_ok,
            bootstrap_pass=False,
        )
        dump_json(runtime_metrics_path, {"summary": summary})
        dump_json(runtime_summary_path, summary)
        print(json.dumps(summary, indent=2))
        return

    started_at = now_ts()
    rng = random.Random(args.random_seed)
    if args.matching_mode == LEGACY_MODE:
        request_timelines = run_legacy_mode(
            args=args,
            rng=rng,
            event_path=event_path,
            requests_path=requests_path,
            timeline_path=timeline_path,
            node_caps_path=node_caps_path,
            node_ports=node_ports,
        )
        node_specific = {
            "capabilities_per_node": args.capabilities_per_node,
            "capability_tags": parse_csv_list(args.capability_tags, "capability-tags"),
        }
    else:
        request_timelines = run_two_stage_mode(
            args=args,
            rng=rng,
            event_path=event_path,
            requests_path=requests_path,
            declaration_requests_path=declaration_requests_path,
            payload_requests_path=payload_requests_path,
            timeline_path=timeline_path,
            node_caps_path=node_caps_path,
            node_ports=node_ports,
        )
        node_specific = {
            "families_per_node": args.families_per_node,
            "domains_min": args.domains_min,
            "domains_max": args.domains_max,
            "tools_min": args.tools_min,
            "tools_max": args.tools_max,
            "specialization_max": args.specialization_max,
            "specialization_hard": bool(int(args.specialization_hard)),
            "constraint_mode": args.constraint_mode,
            "exclude_requester_from_match_count": bool(int(args.exclude_requester_from_match_count)),
        }

    summary = build_summary(
        args=args,
        request_timelines=request_timelines,
        launch_ok=launch_ok,
        health_ok=health_ok,
        bootstrap_pass=bootstrap_pass,
        healthz_ok_count_runtime=healthz_ok,
        started_at=started_at,
        node_specific=node_specific,
    )
    dump_json(runtime_metrics_path, {"summary": summary})
    dump_json(runtime_summary_path, summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
