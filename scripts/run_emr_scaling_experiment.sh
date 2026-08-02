#!/usr/bin/env bash
set -euo pipefail

: "${CLUSTER_ID:?Set CLUSTER_ID to the active EMR cluster id}"
: "${CORE_GROUP_ID:?Set CORE_GROUP_ID to the EMR core instance group id}"
: "${STRESS_CODE_S3_PATH:?Set STRESS_CODE_S3_PATH to pyspark_scale_stress.py in S3}"

REGION="${AWS_REGION:-us-east-1}"
OUTPUT_CSV="${OUTPUT_CSV:-results/aws/emr_scaling_events.csv}"
POLL_SECONDS="${POLL_SECONDS:-30}"
STRESS_SECONDS="${STRESS_SECONDS:-420}"
MAX_OBSERVATION_SECONDS="${MAX_OBSERVATION_SECONDS:-1200}"

mkdir -p "$(dirname "$OUTPUT_CSV")"
printf '%s\n' \
  "timestamp,elapsed_seconds,step_id,step_state,requested_workers,running_workers,policy_state,yarn_memory_available_pct" \
  >"$OUTPUT_CSV"

step_id="$(aws emr add-steps \
  --region "$REGION" \
  --cluster-id "$CLUSTER_ID" \
  --steps "Type=Spark,Name=h1-emr-auto-scaling-evidence,ActionOnFailure=CONTINUE,Args=[--deploy-mode,client,--conf,spark.dynamicAllocation.enabled=false,--num-executors,1,--executor-cores,4,--executor-memory,9g,$STRESS_CODE_S3_PATH,--duration-seconds,$STRESS_SECONDS,--partitions,4]" \
  --query "StepIds[0]" \
  --output text)"

started_epoch="$(date +%s)"
saw_scale_out="false"
while true; do
  now_epoch="$(date +%s)"
  elapsed="$((now_epoch - started_epoch))"
  timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  step_state="$(aws emr describe-step \
    --region "$REGION" \
    --cluster-id "$CLUSTER_ID" \
    --step-id "$step_id" \
    --query "Step.Status.State" \
    --output text)"
  requested="$(aws emr describe-cluster \
    --region "$REGION" \
    --cluster-id "$CLUSTER_ID" \
    --query "Cluster.InstanceGroups[?Id=='$CORE_GROUP_ID'].RequestedInstanceCount | [0]" \
    --output text)"
  running="$(aws emr describe-cluster \
    --region "$REGION" \
    --cluster-id "$CLUSTER_ID" \
    --query "Cluster.InstanceGroups[?Id=='$CORE_GROUP_ID'].RunningInstanceCount | [0]" \
    --output text)"
  policy_state="$(aws emr describe-cluster \
    --region "$REGION" \
    --cluster-id "$CLUSTER_ID" \
    --query "Cluster.InstanceGroups[?Id=='$CORE_GROUP_ID'].AutoScalingPolicy.Status.State | [0]" \
    --output text)"
  start_time="$(python3 -c \
    'from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc)-timedelta(minutes=6)).isoformat())')"
  end_time="$(python3 -c \
    'from datetime import datetime, timezone; print(datetime.now(timezone.utc).isoformat())')"
  yarn_memory="$(aws cloudwatch get-metric-statistics \
    --region "$REGION" \
    --namespace AWS/ElasticMapReduce \
    --metric-name YARNMemoryAvailablePercentage \
    --dimensions "Name=JobFlowId,Value=$CLUSTER_ID" \
    --start-time "$start_time" \
    --end-time "$end_time" \
    --period 60 \
    --statistics Average \
    --query "sort_by(Datapoints,&Timestamp)[-1].Average" \
    --output text)"

  printf '%s\n' \
    "$timestamp,$elapsed,$step_id,$step_state,$requested,$running,$policy_state,$yarn_memory" \
    >>"$OUTPUT_CSV"
  echo "elapsed=$elapsed step=$step_state requested=$requested running=$running yarn_available=$yarn_memory"

  if [[ "$requested" -gt 1 || "$running" -gt 1 ]]; then
    saw_scale_out="true"
  fi
  if [[ "$saw_scale_out" == "true" && "$step_state" == "COMPLETED" && "$requested" == "1" && "$running" == "1" ]]; then
    break
  fi
  case "$step_state" in
    FAILED|CANCELLED|INTERRUPTED)
      exit 1
      ;;
  esac
  if [[ "$elapsed" -ge "$MAX_OBSERVATION_SECONDS" ]]; then
    echo "Scaling observation timed out" >&2
    exit 1
  fi
  sleep "$POLL_SECONDS"
done

echo "$OUTPUT_CSV"
