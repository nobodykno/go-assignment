
#!/usr/bin/env bash

set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
POLL_INTERVAL=2
MAX_ATTEMPTS=90

for command in curl jq; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "Error: '$command' is required."
        exit 1
    fi
done

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

INPUT_FILE="$TMP_DIR/e2e-input.csv"

cat > "$INPUT_FILE" <<'CSV'
date,country,cases,deaths
2026-10-27,India,100,5
2026-10-27,India,150,7
CSV

echo "Checking API health..."

if ! curl --fail --silent --show-error \
    "$API_URL/v1/health" >/dev/null; then
    echo "Error: API is unavailable at $API_URL"
    exit 1
fi

echo "Submitting test CSV..."

RESPONSE="$(
    curl --fail --silent --show-error \
        -X POST "$API_URL/v1/jobs/mixed/upload" \
        -F "name=e2e-pipeline-test" \
        -F "files=@$INPUT_FILE;type=text/csv"
)"

JOB_ID="$(echo "$RESPONSE" | jq -er '.job.id')"

echo "Created job: $JOB_ID"
echo "Waiting for the pipeline to finish..."

for ((attempt = 1; attempt <= MAX_ATTEMPTS; attempt++)); do
    JOB_RESPONSE="$(
        curl --fail --silent --show-error \
            "$API_URL/v1/jobs/$JOB_ID"
    )"

    STATUS="$(echo "$JOB_RESPONSE" | jq -r '.status')"

    echo "Attempt $attempt/$MAX_ATTEMPTS: status=$STATUS"

    case "$STATUS" in
        exported)
            break
            ;;
        failed)
            echo "Pipeline failed:"
            echo "$JOB_RESPONSE" | jq .
            exit 1
            ;;
    esac

    if [[ "$attempt" -eq "$MAX_ATTEMPTS" ]]; then
        echo "Error: timed out waiting for job $JOB_ID"
        echo "$JOB_RESPONSE" | jq .
        exit 1
    fi

    sleep "$POLL_INTERVAL"
done

echo "Verifying final job..."

echo "$JOB_RESPONSE" | jq .

jq -e '
    .status == "exported"
    and .progress_percentage == 100
    and .total_records == 2
    and .processed_records == 2
    and .valid_records == 2
    and .invalid_records == 0
    and (.transformed_file_path | length > 0)
    and (.aggregated_file_path | length > 0)
    and (.exported_file_path | length > 0)
' <<< "$JOB_RESPONSE" >/dev/null

echo ""
echo "PASS: End-to-end pipeline completed successfully."
echo "Job ID: $JOB_ID"
echo "Final status: exported"
