#!/usr/bin/env bash
set -euo pipefail

REGION=eu-west-2
REPO=retailops-dev-ml-pipeline
TAG=latest

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE="${ECR_URI}/${REPO}:${TAG}"

# Login to ECR
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ECR_URI"

# Build the ML image (force linux/amd64 for Fargate)
cd "$(dirname "$0")"

docker buildx create --use --name ml_builder >/dev/null 2>&1 || true

docker buildx build \
  --platform linux/amd64 \
  --load \
  -f Dockerfile.ml \
  -t ml-pipeline:latest \
  .

docker tag ml-pipeline:latest "$IMAGE"
docker push "$IMAGE"

echo "Pushed (linux/amd64): $IMAGE"