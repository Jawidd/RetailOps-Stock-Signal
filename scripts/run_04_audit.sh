#!/usr/bin/env bash

set -e

echo "Running raw data coverage audit in Docker..."

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

docker run --rm \
  -v "${REPO_ROOT}":/workspace \
  -v ~/.aws:/root/.aws \
  -w /workspace \
  retailops-stock-signal-python:latest \
  bash -c "pip install python-dotenv && python scripts/04_audit_raw_data_coverage.py"

echo "Done."