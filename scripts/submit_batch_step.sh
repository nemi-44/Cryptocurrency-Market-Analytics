#!/usr/bin/env bash
set -euo pipefail

: "${CLUSTER_ID:?Set CLUSTER_ID to the active EMR cluster id}"
: "${CODE_S3_PATH:?Set CODE_S3_PATH, for example s3://bucket/code/pyspark_batch.py}"
: "${INPUT_PATH:?Set INPUT_PATH, for example s3://bucket/historical/extracted/*/1m/*.csv}"
: "${OUTPUT_PATH:?Set OUTPUT_PATH, for example s3://bucket/baselines/parquet/}"

REGION="${AWS_REGION:-eu-west-1}"
STEP_ARGS="[--deploy-mode,cluster,$CODE_S3_PATH,--input,$INPUT_PATH,--output,$OUTPUT_PATH"
if [[ -n "${JSON_OUTPUT_PATH:-}" ]]; then
  STEP_ARGS="$STEP_ARGS,--json-output,$JSON_OUTPUT_PATH"
fi
STEP_ARGS="$STEP_ARGS]"

aws emr add-steps \
  --region "$REGION" \
  --cluster-id "$CLUSTER_ID" \
  --steps "Type=Spark,Name=crypto-baseline-batch,ActionOnFailure=CONTINUE,Args=$STEP_ARGS"
