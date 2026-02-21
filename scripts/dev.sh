#!/usr/bin/env bash
set -euo pipefail

if [[ ! -f ".env" ]]; then
  cp .env.example .env
fi

set -a
source .env
set +a

uvicorn app.main:app --host 0.0.0.0 --port 8787 --reload
