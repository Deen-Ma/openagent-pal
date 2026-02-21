import base64
import hashlib
import hmac
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from jsonschema import ValidationError, validate


APP_VERSION = "0.1.0"


class Settings(BaseModel):
    pal_api_token: str = Field(default="dev-token")
    pal_origin_did: str = Field(default="did:key:z6MkhOpenAgentLocal")
    pal_publisher_did: str = Field(default="did:key:z6MkhOpenAgentLocal")
    pal_signing_secret: str = Field(default="dev-signing-secret")
    pal_conf_min: int = Field(default=700)
    pal_ttl_min_sec: int = Field(default=300)
    pal_ttl_max_sec: int = Field(default=604800)
    pal_max_topics: int = Field(default=8)
    pal_summary_max_len: int = Field(default=140)
    pal_event_size_max_bytes: int = Field(default=1024)


def load_settings() -> Settings:
    return Settings(
        pal_api_token=os.getenv("PAL_API_TOKEN", "dev-token"),
        pal_origin_did=os.getenv("PAL_ORIGIN_DID", "did:key:z6MkhOpenAgentLocal"),
        pal_publisher_did=os.getenv("PAL_PUBLISHER_DID", "did:key:z6MkhOpenAgentLocal"),
        pal_signing_secret=os.getenv("PAL_SIGNING_SECRET", "dev-signing-secret"),
        pal_conf_min=int(os.getenv("PAL_CONF_MIN", "700")),
        pal_ttl_min_sec=int(os.getenv("PAL_TTL_MIN_SEC", "300")),
        pal_ttl_max_sec=int(os.getenv("PAL_TTL_MAX_SEC", "604800")),
        pal_max_topics=int(os.getenv("PAL_MAX_TOPICS", "8")),
        pal_summary_max_len=int(os.getenv("PAL_SUMMARY_MAX_LEN", "140")),
        pal_event_size_max_bytes=int(os.getenv("PAL_EVENT_SIZE_MAX_BYTES", "1024")),
    )


SETTINGS = load_settings()


def now_ms() -> int:
    return int(time.time() * 1000)


def b64url_no_pad(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def canonical_json(data: Dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def normalize_text(text: str) -> str:
    return " ".join(text.strip().split())


def compile_intent(intent_text: str) -> Dict[str, Any]:
    normalized = normalize_text(intent_text)
    lowered = normalized.lower()
    taxonomy = "social.general"
    conf = 650

    if any(token in lowered for token in ["标注", "annotation", "label", "labeling"]):
        taxonomy = "crowd.data_labeling"
        conf = 920
    elif any(token in lowered for token in ["翻译", "translate", "translation"]):
        taxonomy = "crowd.translation"
        conf = 900
    elif any(token in lowered for token in ["设计", "design", "poster", "海报"]):
        taxonomy = "crowd.design"
        conf = 880

    summary = normalized[:280]
    return {
        "canonical_intent": {"text": normalized},
        "taxonomy": taxonomy,
        "conf": conf,
        "summary": summary,
    }


def taxonomy_to_topics(taxonomy: str) -> List[str]:
    parts = taxonomy.split(".")
    topics: List[str] = []
    current: List[str] = []
    for part in parts:
        current.append(part)
        topics.append("agentnet/v1/" + "/".join(current))
    return topics


def sign_event(event: Dict[str, Any], secret: str) -> str:
    payload = canonical_json(event).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).digest()
    return "b64:" + b64url_no_pad(sig)


def build_task_id(origin: str, taxonomy: str, canonical_intent: Dict[str, Any], nonce: str) -> str:
    payload = f"{origin}|{taxonomy}|{canonical_json(canonical_intent)}|{nonce}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    return "b64:" + b64url_no_pad(digest[:18])


def validate_event_size(event: Dict[str, Any]) -> None:
    payload = canonical_json(event).encode("utf-8")
    if len(payload) > SETTINGS.pal_event_size_max_bytes:
        raise_pal_error(
            "PAL-VAL-002",
            f"event exceeds {SETTINGS.pal_event_size_max_bytes} bytes",
            400,
        )


def policy_checks(conf: int, ttl_sec: int, topics: List[str], summary: str) -> List[Dict[str, str]]:
    blocks: List[Dict[str, str]] = []
    if conf < SETTINGS.pal_conf_min:
        blocks.append({"code": "PAL-POL-001", "message": "confidence below threshold"})
    if ttl_sec < SETTINGS.pal_ttl_min_sec or ttl_sec > SETTINGS.pal_ttl_max_sec:
        blocks.append({"code": "PAL-VAL-002", "message": "ttl out of allowed range"})
    if len(topics) > SETTINGS.pal_max_topics:
        blocks.append({"code": "PAL-VAL-002", "message": "too many topics"})
    if len(summary) > SETTINGS.pal_summary_max_len:
        blocks.append({"code": "PAL-VAL-002", "message": "summary too long"})
    return blocks


def raise_pal_error(code: str, message: str, status: int, details: Optional[Dict[str, Any]] = None) -> None:
    payload: Dict[str, Any] = {"error": {"code": code, "message": message}}
    if details:
        payload["error"]["details"] = details
    raise HTTPException(status_code=status, detail=payload)


def load_schema() -> Dict[str, Any]:
    schema_path = Path(__file__).resolve().parent.parent / "spec" / "canonical-event.v0.1.schema.json"
    if not schema_path.exists():
        raise RuntimeError(f"Schema file missing: {schema_path}")
    return json.loads(schema_path.read_text(encoding="utf-8"))


SCHEMA = load_schema()


class PrepareBroadcastRequest(BaseModel):
    intent_text: str = Field(min_length=1, max_length=2000)
    context: Dict[str, Any] = Field(default_factory=dict)
    requested_ttl_sec: int = Field(default=3600)
    api_token: Optional[str] = None


class PublishDraftRequest(BaseModel):
    draft_id: str
    approval: Dict[str, Any] = Field(default_factory=dict)
    api_token: Optional[str] = None


class UpdateTaskRequest(BaseModel):
    task_id: str
    patch: Dict[str, Any] = Field(default_factory=dict)
    api_token: Optional[str] = None


class CompleteTaskRequest(BaseModel):
    task_id: str
    result_ref: str = Field(default="")
    api_token: Optional[str] = None


class WithdrawTaskRequest(BaseModel):
    task_id: str
    reason: str = Field(default="")
    api_token: Optional[str] = None


class QueryTasksRequest(BaseModel):
    status: Optional[Literal["ACTIVE", "TERMINAL_COMPLETE", "TERMINAL_WITHDRAW"]] = None
    taxonomy: Optional[str] = None
    limit: int = Field(default=100, ge=1, le=1000)
    api_token: Optional[str] = None


@dataclass
class DraftRecord:
    draft_id: str
    created_at: int
    intent_text: str
    canonical_intent: Dict[str, Any]
    taxonomy: str
    conf: int
    topics: List[str]
    summary: str
    ttl_sec: int
    expires_at: int
    nonce: str
    policy_blocks: List[Dict[str, str]]


class StateStore:
    def __init__(self) -> None:
        self.lock = Lock()
        self.drafts: Dict[str, DraftRecord] = {}
        self.tasks: Dict[str, Dict[str, Any]] = {}

    def _task_key(self, origin: str, task_id: str) -> str:
        return f"{origin}|{task_id}"

    def create_draft(self, draft: DraftRecord) -> None:
        with self.lock:
            self.drafts[draft.draft_id] = draft

    def get_draft(self, draft_id: str) -> Optional[DraftRecord]:
        with self.lock:
            return self.drafts.get(draft_id)

    def consume_draft(self, draft_id: str) -> Optional[DraftRecord]:
        with self.lock:
            return self.drafts.pop(draft_id, None)

    def current_seq(self, origin: str, task_id: str) -> int:
        with self.lock:
            key = self._task_key(origin, task_id)
            record = self.tasks.get(key)
            if not record:
                return 0
            return int(record["last_seq"])

    def get_task(self, origin: str, task_id: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            return self.tasks.get(self._task_key(origin, task_id))

    def upsert_task_record(self, origin: str, task_id: str, record: Dict[str, Any]) -> None:
        with self.lock:
            self.tasks[self._task_key(origin, task_id)] = record

    def list_tasks(self) -> List[Dict[str, Any]]:
        with self.lock:
            return list(self.tasks.values())


STORE = StateStore()


def enforce_auth(api_token: Optional[str]) -> None:
    if SETTINGS.pal_api_token and api_token != SETTINGS.pal_api_token:
        raise_pal_error("PAL-AUTH-001", "invalid api token", 401)


def validate_schema_or_raise(event: Dict[str, Any]) -> None:
    try:
        validate(instance=event, schema=SCHEMA)
    except ValidationError as exc:
        raise_pal_error(
            "PAL-VAL-001",
            "schema validation failed",
            400,
            {"path": list(exc.path), "message": exc.message},
        )


def apply_event(event: Dict[str, Any]) -> Dict[str, Any]:
    event_now = now_ms()
    if event_now > int(event["expires_at"]):
        raise_pal_error("PAL-EXP-001", "event expired", 400)

    key = f"{event['origin']}|{event['task_id']}"
    existing = STORE.get_task(event["origin"], event["task_id"])

    if existing and existing["status"] in {"TERMINAL_COMPLETE", "TERMINAL_WITHDRAW"}:
        raise_pal_error("PAL-STA-001", "terminal task cannot be updated", 409)

    last_seq = int(existing["last_seq"]) if existing else 0
    if int(event["seq"]) <= last_seq:
        raise_pal_error("PAL-STA-002", "stale event sequence", 409)

    if event["op"] == "UPSERT":
        status = "ACTIVE"
    elif event["op"] == "COMPLETE":
        status = "TERMINAL_COMPLETE"
    else:
        status = "TERMINAL_WITHDRAW"

    record = {
        "key": key,
        "origin": event["origin"],
        "task_id": event["task_id"],
        "taxonomy": event["taxonomy"],
        "topics": event["topics"],
        "summary": event["summary"],
        "detail_ref": event["detail_ref"],
        "status": status,
        "last_seq": int(event["seq"]),
        "last_op": event["op"],
        "conf": int(event["conf"]),
        "expires_at": int(event["expires_at"]),
        "updated_at": event_now,
        "last_event": event,
    }
    STORE.upsert_task_record(event["origin"], event["task_id"], record)
    return record


def build_event(
    task_id: str,
    seq: int,
    op: str,
    taxonomy: str,
    conf: int,
    topics: List[str],
    summary: str,
    detail_ref: str,
    expires_at: int,
) -> Dict[str, Any]:
    event: Dict[str, Any] = {
        "v": "0.2",
        "origin": SETTINGS.pal_origin_did,
        "publisher": SETTINGS.pal_publisher_did,
        "task_id": task_id,
        "seq": seq,
        "op": op,
        "expires_at": expires_at,
        "topics": topics,
        "taxonomy": taxonomy,
        "conf": conf,
        "summary": summary,
        "detail_ref": detail_ref,
        "sig": "",
    }
    event["sig"] = sign_event(event, SETTINGS.pal_signing_secret)
    validate_schema_or_raise(event)
    validate_event_size(event)
    return event


app = FastAPI(title="OpenAgent PAL", version=APP_VERSION)


@app.get("/healthz")
def healthz() -> Dict[str, Any]:
    return {"ok": True, "service": "pal", "version": APP_VERSION, "time_ms": now_ms()}


@app.post("/v1/prepare_broadcast")
def prepare_broadcast(req: PrepareBroadcastRequest) -> Dict[str, Any]:
    enforce_auth(req.api_token)
    compiled = compile_intent(req.intent_text)
    taxonomy = compiled["taxonomy"]
    conf = int(compiled["conf"])
    canonical_intent = compiled["canonical_intent"]
    topics = taxonomy_to_topics(taxonomy)
    summary = normalize_text(compiled["summary"])
    ttl_sec = int(req.requested_ttl_sec)
    expires_at = now_ms() + ttl_sec * 1000

    blocks = policy_checks(conf=conf, ttl_sec=ttl_sec, topics=topics, summary=summary)

    draft_id = f"draft_{uuid.uuid4().hex[:12]}"
    nonce = uuid.uuid4().hex[:8]
    draft = DraftRecord(
        draft_id=draft_id,
        created_at=now_ms(),
        intent_text=req.intent_text,
        canonical_intent=canonical_intent,
        taxonomy=taxonomy,
        conf=conf,
        topics=topics,
        summary=summary,
        ttl_sec=ttl_sec,
        expires_at=expires_at,
        nonce=nonce,
        policy_blocks=blocks,
    )
    STORE.create_draft(draft)

    return {
        "draft_id": draft_id,
        "taxonomy": taxonomy,
        "conf": conf,
        "topics": topics,
        "summary": summary,
        "expires_at": expires_at,
        "policy": {"allowed": len(blocks) == 0, "blocks": blocks},
        "preview_event": {"v": "0.2", "op": "UPSERT"},
    }


@app.post("/v1/publish_draft")
def publish_draft(req: PublishDraftRequest) -> Dict[str, Any]:
    enforce_auth(req.api_token)
    draft = STORE.consume_draft(req.draft_id)
    if not draft:
        raise_pal_error("PAL-DRAFT-001", "draft not found or already consumed", 404)
    if now_ms() > draft.expires_at:
        raise_pal_error("PAL-DRAFT-001", "draft expired", 400)
    if draft.policy_blocks:
        raise_pal_error("PAL-POL-001", "policy rejected draft", 400, {"blocks": draft.policy_blocks})

    task_id = build_task_id(
        origin=SETTINGS.pal_origin_did,
        taxonomy=draft.taxonomy,
        canonical_intent=draft.canonical_intent,
        nonce=draft.nonce,
    )
    seq = STORE.current_seq(SETTINGS.pal_origin_did, task_id) + 1
    event = build_event(
        task_id=task_id,
        seq=seq,
        op="UPSERT",
        taxonomy=draft.taxonomy,
        conf=draft.conf,
        topics=draft.topics,
        summary=draft.summary,
        detail_ref=f"p2p://peer/local/openagent/session?task_id={task_id}",
        expires_at=draft.expires_at,
    )
    record = apply_event(event)
    return {
        "accepted": True,
        "task_id": task_id,
        "seq": seq,
        "published_at": now_ms(),
        "status": record["status"],
        "event": event,
    }


@app.post("/v1/update_task")
def update_task(req: UpdateTaskRequest) -> Dict[str, Any]:
    enforce_auth(req.api_token)
    current = STORE.get_task(SETTINGS.pal_origin_did, req.task_id)
    if not current:
        raise_pal_error("PAL-STA-001", "task not found", 404)
    if current["status"] != "ACTIVE":
        raise_pal_error("PAL-STA-001", "terminal task cannot be updated", 409)

    summary = normalize_text(str(req.patch.get("summary", current["summary"])))
    detail_ref = str(req.patch.get("detail_ref", current["detail_ref"]))
    ttl_sec = int(req.patch.get("requested_ttl_sec", SETTINGS.pal_ttl_min_sec))
    expires_at = now_ms() + ttl_sec * 1000

    blocks = policy_checks(
        conf=int(current["conf"]),
        ttl_sec=ttl_sec,
        topics=list(current["topics"]),
        summary=summary,
    )
    if blocks:
        raise_pal_error("PAL-POL-001", "policy rejected update", 400, {"blocks": blocks})

    seq = int(current["last_seq"]) + 1
    event = build_event(
        task_id=req.task_id,
        seq=seq,
        op="UPSERT",
        taxonomy=str(current["taxonomy"]),
        conf=int(current["conf"]),
        topics=list(current["topics"]),
        summary=summary,
        detail_ref=detail_ref,
        expires_at=expires_at,
    )
    record = apply_event(event)
    return {"accepted": True, "task_id": req.task_id, "seq": seq, "status": record["status"], "event": event}


@app.post("/v1/complete_task")
def complete_task(req: CompleteTaskRequest) -> Dict[str, Any]:
    enforce_auth(req.api_token)
    current = STORE.get_task(SETTINGS.pal_origin_did, req.task_id)
    if not current:
        raise_pal_error("PAL-STA-001", "task not found", 404)
    if current["status"] != "ACTIVE":
        raise_pal_error("PAL-STA-001", "task already terminal", 409)

    seq = int(current["last_seq"]) + 1
    event = build_event(
        task_id=req.task_id,
        seq=seq,
        op="COMPLETE",
        taxonomy=str(current["taxonomy"]),
        conf=int(current["conf"]),
        topics=list(current["topics"]),
        summary=str(current["summary"]),
        detail_ref=req.result_ref or str(current["detail_ref"]),
        expires_at=int(current["expires_at"]),
    )
    record = apply_event(event)
    return {"accepted": True, "task_id": req.task_id, "seq": seq, "status": record["status"], "event": event}


@app.post("/v1/withdraw_task")
def withdraw_task(req: WithdrawTaskRequest) -> Dict[str, Any]:
    enforce_auth(req.api_token)
    current = STORE.get_task(SETTINGS.pal_origin_did, req.task_id)
    if not current:
        raise_pal_error("PAL-STA-001", "task not found", 404)
    if current["status"] != "ACTIVE":
        raise_pal_error("PAL-STA-001", "task already terminal", 409)

    seq = int(current["last_seq"]) + 1
    reason_suffix = f" | reason={normalize_text(req.reason)}" if req.reason else ""
    event = build_event(
        task_id=req.task_id,
        seq=seq,
        op="WITHDRAW",
        taxonomy=str(current["taxonomy"]),
        conf=int(current["conf"]),
        topics=list(current["topics"]),
        summary=str(current["summary"]) + reason_suffix,
        detail_ref=str(current["detail_ref"]),
        expires_at=int(current["expires_at"]),
    )
    record = apply_event(event)
    return {"accepted": True, "task_id": req.task_id, "seq": seq, "status": record["status"], "event": event}


@app.get("/v1/tasks")
def query_tasks(status: Optional[str] = None, taxonomy: Optional[str] = None, limit: int = 100, api_token: Optional[str] = None) -> Dict[str, Any]:
    enforce_auth(api_token)
    items = STORE.list_tasks()
    if status:
        items = [item for item in items if item["status"] == status]
    if taxonomy:
        items = [item for item in items if item["taxonomy"] == taxonomy]
    items = sorted(items, key=lambda x: x["updated_at"], reverse=True)[:limit]
    return {"items": items, "count": len(items)}
