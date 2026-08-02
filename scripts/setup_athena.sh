#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="${STACK_NAME:-crypto-analytics}"
REGION="${AWS_REGION:-us-east-1}"
SQL_DIR="${SQL_DIR:-sql/athena}"

stack_output() {
  local output_key="$1"
  aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue | [0]" \
    --output text
}

DATABASE_NAME="${ATHENA_DATABASE_NAME:-$(stack_output AthenaDatabaseName)}"
WORKGROUP_NAME="${ATHENA_WORKGROUP_NAME:-$(stack_output AthenaWorkGroupName)}"

if [[ -z "$DATABASE_NAME" || "$DATABASE_NAME" == "None" ]]; then
  echo "AthenaDatabaseName is missing from stack $STACK_NAME" >&2
  exit 1
fi
if [[ -z "$WORKGROUP_NAME" || "$WORKGROUP_NAME" == "None" ]]; then
  echo "AthenaWorkGroupName is missing from stack $STACK_NAME" >&2
  exit 1
fi

run_query() {
  local sql_file="$1"
  local query_id
  local state
  local reason

  query_id="$(
    aws athena start-query-execution \
      --region "$REGION" \
      --work-group "$WORKGROUP_NAME" \
      --query-execution-context "Database=${DATABASE_NAME}" \
      --query-string "file://${sql_file}" \
      --query "QueryExecutionId" \
      --output text
  )"

  while true; do
    state="$(
      aws athena get-query-execution \
        --region "$REGION" \
        --query-execution-id "$query_id" \
        --query "QueryExecution.Status.State" \
        --output text
    )"
    case "$state" in
      SUCCEEDED)
        echo "SUCCEEDED $query_id $sql_file"
        return
        ;;
      FAILED|CANCELLED)
        reason="$(
          aws athena get-query-execution \
            --region "$REGION" \
            --query-execution-id "$query_id" \
            --query "QueryExecution.Status.StateChangeReason" \
            --output text
        )"
        echo "$state $query_id $sql_file: $reason" >&2
        exit 1
        ;;
    esac
    sleep 2
  done
}

for sql_file in "$SQL_DIR"/*.sql; do
  run_query "$sql_file"
done

echo "Athena views are ready in database $DATABASE_NAME using workgroup $WORKGROUP_NAME."
