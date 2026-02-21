# OpenAgent PAL (Phase B Skeleton)

This repository contains a deployable PAL (Control Plane) service aligned to:
- `spec/openagent-phase-a-baseline-v0.1.md`
- `spec/canonical-event.v0.1.schema.json`

## What is implemented
- HTTP API endpoints:
  - `GET /healthz`
  - `POST /v1/prepare_broadcast`
  - `POST /v1/publish_draft`
  - `POST /v1/update_task`
  - `POST /v1/complete_task`
  - `POST /v1/withdraw_task`
  - `GET /v1/tasks`
- Deterministic task state handling:
  - key = `(origin, task_id)`
  - apply only when `seq > last_seq`
  - terminal absorbing states for `COMPLETE/WITHDRAW`
  - expiry rejection
- Policy checks:
  - confidence threshold
  - TTL bounds
  - topic count limit
  - summary length limit
- Canonical event schema validation and event-size guard (`<= 1KB` by default).

## Run locally

1. Create env file:
```bash
cp .env.example .env
```

2. Start with Docker:
```bash
docker compose up -d --build
```

3. Health check:
```bash
curl http://127.0.0.1:8787/healthz
```

## API quick test
Use your API token from `.env`.

1. Prepare draft:
```bash
curl -X POST http://127.0.0.1:8787/v1/prepare_broadcast \
  -H 'content-type: application/json' \
  -d '{
    "api_token":"replace-me",
    "intent_text":"中文图片标注500张，预算200元，48小时交付",
    "requested_ttl_sec":172800,
    "context":{"locale":"zh-CN"}
  }'
```

2. Publish draft:
```bash
curl -X POST http://127.0.0.1:8787/v1/publish_draft \
  -H 'content-type: application/json' \
  -d '{
    "api_token":"replace-me",
    "draft_id":"<from previous response>",
    "approval":{"mode":"user_confirmed","actor":"user:local"}
  }'
```

3. Query tasks:
```bash
curl "http://127.0.0.1:8787/v1/tasks?api_token=replace-me"
```

## OpenClaw integration config
On the machine where OpenClaw runs:

```bash
PAL_BASE_URL=http://127.0.0.1:8787
PAL_API_TOKEN=replace-me
PAL_TIMEOUT_MS=5000
```

Call flow:
1. `prepare_broadcast`
2. user approval
3. `publish_draft`
4. `update_task` / `complete_task` / `withdraw_task`
5. `GET /v1/tasks` for local-first view

## GitHub workflow
From this directory:

```bash
git init
git add .
git commit -m "feat: add deployable PAL service skeleton"
gh repo create <your-repo-name> --private --source . --remote origin --push
```

## Mac mini deploy workflow
On Mac mini:

```bash
git clone <repo-url>
cd <repo-folder>
cp .env.example .env
# edit .env: set PAL_API_TOKEN and PAL_SIGNING_SECRET
docker compose up -d --build
```

Then point OpenClaw to:
- `PAL_BASE_URL=http://127.0.0.1:8787`
- `PAL_API_TOKEN=<same token>`

## Notes
- Current state storage is in-memory; restart clears tasks/drafts.
- Signature is HMAC-based placeholder for dev; replace with DID key signing in next iteration.
