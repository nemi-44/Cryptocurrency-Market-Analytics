#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="${STACK_NAME:-crypto-analytics}"
REGION="${AWS_REGION:-us-east-1}"
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$BUILD_DIR"' EXIT

API_URL="$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='ApiLatestUrl'].OutputValue | [0]" \
  --output text)"
BUCKET_NAME="$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='DashboardBucketName'].OutputValue | [0]" \
  --output text)"

sed "s|API_URL_PLACEHOLDER|$API_URL|g" frontend/index.html >"$BUILD_DIR/index.html"
aws s3 cp \
  "$BUILD_DIR/index.html" \
  "s3://$BUCKET_NAME/index.html" \
  --region "$REGION" \
  --content-type text/html \
  --cache-control no-store

if [[ -d "results/aws" ]]; then
  aws s3 sync \
    results/aws \
    "s3://$BUCKET_NAME/benchmarks/" \
    --region "$REGION" \
    --exclude "*" \
    --include "*.csv" \
    --include "*.json" \
    --include "figures/*.png" \
    --cache-control no-store
fi

aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='DashboardWebsiteUrl'].OutputValue | [0]" \
  --output text
