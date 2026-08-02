#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="${STACK_NAME:-crypto-analytics}"
REGION="${AWS_REGION:-us-east-1}"
CODE_ARCHIVE_KEY="${CODE_ARCHIVE_KEY:-artifacts/crypto-analytics.zip}"
PACKAGE_DIR="$(mktemp -d)"
trap 'rm -rf "$PACKAGE_DIR"' EXIT

RAW_BUCKET="$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "Stacks[0].Outputs[?OutputKey=='RawBucketName'].OutputValue | [0]" \
  --output text)"

zip -qr "$PACKAGE_DIR/crypto-analytics.zip" \
  crypto_analytics \
  dashboard_app.py \
  requirements.txt \
  -x "*/__pycache__/*" "*.pyc"

aws s3 cp \
  "$PACKAGE_DIR/crypto-analytics.zip" \
  "s3://$RAW_BUCKET/$CODE_ARCHIVE_KEY" \
  --region "$REGION"

echo "s3://$RAW_BUCKET/$CODE_ARCHIVE_KEY"
