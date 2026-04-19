#!/usr/bin/env bash
# scripts/04_upload_ml_pipeline_to_s3.sh
#
# Zips the ml/ directory and uploads it to S3 so the ECS ML task can
# download and run it at runtime.
#
# Usage:
#   ./scripts/04_upload_ml_pipeline_to_s3.sh
#
# The ECS task definition expects the zip at:
#   s3://<DATALAKE_BUCKET>/metadata/ml/ml_pipeline.zip
#
# The zip must contain ml/run_pipeline.py at the path:
#   ml_pipeline/ml/run_pipeline.py   (top-level folder = ml_pipeline)
# which matches the PIPELINE_DIR resolution logic in retops-ecs-ml.yaml.

set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-eu-west-2}"
BUCKET="retailops-data-lake-${REGION}"
S3_KEY="metadata/ml/ml_pipeline.zip"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TMP_ZIP="/tmp/ml_pipeline.zip"

echo "=== Packaging ML pipeline ==="
echo "Source : ${REPO_ROOT}/ml/"
echo "Target : s3://${BUCKET}/${S3_KEY}"

# Remove stale zip if present
rm -f "$TMP_ZIP"

# Zip the ml/ directory under a top-level folder called ml_pipeline
# so the ECS container finds it at /tmp/ml_pipeline/ml_pipeline/
cd "$REPO_ROOT"
zip -r "$TMP_ZIP" ml/ \
  --exclude "ml/__pycache__/*" \
  --exclude "ml/*.pyc" \
  --exclude "ml/notebooks/*" \
  --exclude "ml/.ipynb_checkpoints/*"

echo "=== Uploading to S3 ==="
aws s3 cp "$TMP_ZIP" "s3://${BUCKET}/${S3_KEY}" --region "$REGION"

echo "=== Done ==="
echo "Uploaded: s3://${BUCKET}/${S3_KEY}"
