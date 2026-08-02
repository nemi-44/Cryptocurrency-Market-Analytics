#!/usr/bin/env bash
set -euo pipefail

: "${CLUSTER_ID:?Set CLUSTER_ID to the active EMR cluster id}"
: "${CORE_GROUP_ID:?Set CORE_GROUP_ID to the EMR core instance group id}"
: "${CODE_S3_PATH:?Set CODE_S3_PATH to the uploaded pyspark_batch.py}"
: "${INPUT_PATH:?Set INPUT_PATH to the immutable real-data S3 prefix}"
: "${OUTPUT_ROOT:?Set OUTPUT_ROOT to an S3 benchmark output prefix}"

REGION="${AWS_REGION:-us-east-1}"
REPEATS="${REPEATS:-3}"
WORKER_COUNTS="${WORKER_COUNTS:-1 2 4}"
INPUT_RECORDS="${INPUT_RECORDS:-2416619}"
OUTPUT_CSV="${OUTPUT_CSV:-results/aws/emr_benchmark_runs.csv}"
POLL_SECONDS="${POLL_SECONDS:-15}"
POLICY_FILE="${POLICY_FILE:-scripts/emr-auto-scaling-policy.json}"

mkdir -p "$(dirname "$OUTPUT_CSV")"
TEMP_CSV="$(mktemp)"
trap 'rm -f "$TEMP_CSV"' EXIT

printf '%s\n' \
  "data_source,environment,cluster_id,step_id,worker_count,repeat,input_records,runtime_seconds,state,started_at,ended_at" \
  >"$TEMP_CSV"

policy_state="$(aws emr describe-cluster \
  --region "$REGION" \
  --cluster-id "$CLUSTER_ID" \
  --query "Cluster.InstanceGroups[?Id=='$CORE_GROUP_ID'].AutoScalingPolicy.Status.State | [0]" \
  --output text)"
if [[ "$policy_state" == "ATTACHED" || "$policy_state" == "ATTACHING" ]]; then
  aws emr remove-auto-scaling-policy \
    --region "$REGION" \
    --cluster-id "$CLUSTER_ID" \
    --instance-group-id "$CORE_GROUP_ID" >/dev/null
fi

wait_for_worker_count() {
  local target="$1"
  local running requested
  for _ in $(seq 1 80); do
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
    echo "EMR workers requested=$requested running=$running target=$target"
    if [[ "$requested" == "$target" && "$running" == "$target" ]]; then
      return 0
    fi
    sleep "$POLL_SECONDS"
  done
  echo "Timed out waiting for $target core workers" >&2
  return 1
}

resize_workers() {
  local target="$1"
  aws emr modify-instance-groups \
    --region "$REGION" \
    --cluster-id "$CLUSTER_ID" \
    --instance-groups "InstanceGroupId=$CORE_GROUP_ID,InstanceCount=$target" >/dev/null
  wait_for_worker_count "$target"
}

run_step() {
  local workers="$1"
  local repeat="$2"
  local run_root="${OUTPUT_ROOT%/}/workers=$workers/repeat=$repeat"
  local step_id state started ended runtime

  step_id="$(aws emr add-steps \
    --region "$REGION" \
    --cluster-id "$CLUSTER_ID" \
    --steps "Type=Spark,Name=h1-real-emr-w${workers}-r${repeat},ActionOnFailure=CONTINUE,Args=[--deploy-mode,client,--conf,spark.dynamicAllocation.enabled=false,--num-executors,$workers,--executor-cores,4,--executor-memory,8g,$CODE_S3_PATH,--input,$INPUT_PATH,--output,$run_root/baselines,--json-output,$run_root/latest,--batch-view-output,$run_root/windows]" \
    --query "StepIds[0]" \
    --output text)"

  while true; do
    state="$(aws emr describe-step \
      --region "$REGION" \
      --cluster-id "$CLUSTER_ID" \
      --step-id "$step_id" \
      --query "Step.Status.State" \
      --output text)"
    echo "step=$step_id workers=$workers repeat=$repeat state=$state"
    case "$state" in
      COMPLETED)
        break
        ;;
      FAILED|CANCELLED|INTERRUPTED)
        aws emr describe-step \
          --region "$REGION" \
          --cluster-id "$CLUSTER_ID" \
          --step-id "$step_id" \
          --output json
        return 1
        ;;
    esac
    sleep "$POLL_SECONDS"
  done

  started="$(aws emr describe-step \
    --region "$REGION" \
    --cluster-id "$CLUSTER_ID" \
    --step-id "$step_id" \
    --query "Step.Status.Timeline.StartDateTime" \
    --output text)"
  ended="$(aws emr describe-step \
    --region "$REGION" \
    --cluster-id "$CLUSTER_ID" \
    --step-id "$step_id" \
    --query "Step.Status.Timeline.EndDateTime" \
    --output text)"
  runtime="$(python3 -c \
    'from datetime import datetime; import sys; print(round((datetime.fromisoformat(sys.argv[2])-datetime.fromisoformat(sys.argv[1])).total_seconds(), 3))' \
    "$started" "$ended")"

  printf '%s\n' \
    "binance-vision-aggTrades,aws-learner-lab,$CLUSTER_ID,$step_id,$workers,$repeat,$INPUT_RECORDS,$runtime,$state,$started,$ended" \
    >>"$TEMP_CSV"
}

for workers in $WORKER_COUNTS; do
  resize_workers "$workers"
  for repeat in $(seq 1 "$REPEATS"); do
    run_step "$workers" "$repeat"
  done
done

resize_workers 1
aws emr put-auto-scaling-policy \
  --region "$REGION" \
  --cluster-id "$CLUSTER_ID" \
  --instance-group-id "$CORE_GROUP_ID" \
  --auto-scaling-policy "file://$POLICY_FILE" >/dev/null

mv "$TEMP_CSV" "$OUTPUT_CSV"
trap - EXIT
echo "$OUTPUT_CSV"
