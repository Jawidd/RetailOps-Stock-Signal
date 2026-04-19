#!/usr/bin/env bash

set -e

echo "Running stockout risk analysis in Docker..."

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

docker run --rm \
  -v "${REPO_ROOT}":/workspace \
  -v ~/.aws:/root/.aws \
  -w /workspace/analytics \
  retailops-stock-signal-python:latest \
  bash -c "pip install python-dotenv && python stockout_risk.py"

echo "Done."
