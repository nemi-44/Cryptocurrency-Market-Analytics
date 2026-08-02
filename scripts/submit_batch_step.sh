#!/usr/bin/env bash
set -euo pipefail

: "${CLUSTER_ID:?Set CLUSTER_ID to the active EMR cluster id}"
: "${CODE_S3_PATH:?Set CODE_S3_PATH, for example s3://bucket/code/pyspark_batch.py}"
: "${INPUT_PATH:?Set INPUT_PATH, for example s3://bucket/historical/extracted/*/1m/*.csv}"
: "${OUTPUT_PATH:?Set OUTPUT_PATH, for example s3://bucket/baselines/parquet/}"

REGION="${AWS_REGION:-us-east-1}"
SPARK_DEPLOY_MODE="${SPARK_DEPLOY_MODE:-client}"
STEP_ARGS="[--deploy-mode,$SPARK_DEPLOY_MODE,$CODE_S3_PATH,--input,$INPUT_PATH,--output,$OUTPUT_PATH"
if [[ -n "${JSON_OUTPUT_PATH:-}" ]]; then
  STEP_ARGS="$STEP_ARGS,--json-output,$JSON_OUTPUT_PATH"
fi
if [[ -n "${BATCH_VIEW_OUTPUT_PATH:-}" ]]; then
  STEP_ARGS="$STEP_ARGS,--batch-view-output,$BATCH_VIEW_OUTPUT_PATH"
fi
STEP_ARGS="$STEP_ARGS]"

aws emr add-steps \
  --region "$REGION" \
  --cluster-id "$CLUSTER_ID" \
  --steps "Type=Spark,Name=crypto-baseline-batch,ActionOnFailure=CONTINUE,Args=$STEP_ARGS"
