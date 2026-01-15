#!/usr/bin/env bash
set -euo pipefail

REGION="eu-west-2"
PROJECT="retailops"
ENV="dev"

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
ECR_HOST="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

IMAGE_URI="$(aws cloudformation list-exports --region "$REGION" \
  --query "Exports[?Name=='${PROJECT}-${ENV}-DataGeneratorImageUri'].Value" --output text)"

if [[ -z "$IMAGE_URI" || "$IMAGE_URI" == "None" ]]; then
  echo "Export ${PROJECT}-${ENV}-DataGeneratorImageUri not found. Deploy retops-ecr-data-generator first."
  exit 1
fi

cd "$(dirname "$0")"

aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ECR_HOST"

# IMPORTANT:
# - use buildx so we can force linux/amd64
# - use --load to produce a single-arch Docker image locally (not an OCI index)
docker buildx create --use --name lambda_builder >/dev/null 2>&1 || true

docker buildx build \
  --platform linux/amd64 \
  --load \
  -t data-generator:latest \
  .

docker tag data-generator:latest "$IMAGE_URI"
docker push "$IMAGE_URI"

echo "Pushed (linux/amd64): $IMAGE_URI"
