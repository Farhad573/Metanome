#!/usr/bin/env bash
set -euo pipefail

# -------------------- Config --------------------
BASE_URL="${BASE_URL:-http://localhost:5172/api}"
ENGINE_ID="${ENGINE_ID:-1}"                      # testing engine id
RESULTS_DIR="${RESULTS_DIR:-./results}"          # contains <executionId>.ndjson
POLL_INTERVAL_SEC="${POLL_INTERVAL_SEC:-1}"
REPEAT_COUNT="${REPEAT_COUNT:-3}"                # repeat timing-sensitive measurements

HAS_JQ=0
command -v jq >/dev/null 2>&1 && HAS_JQ=1
HAS_BC=0
command -v bc >/dev/null 2>&1 && HAS_BC=1

# -------------------- Helpers --------------------
curl_json() {
  local method="$1" url="$2" data="${3:-}"
  if [[ -n "$data" ]]; then
    curl -sS -X "$method" -H "Content-Type: application/json" --data "$data" "$url"
  else
    curl -sS -X "$method" "$url"
  fi
}

curl_time() {
  local method="$1" url="$2" data="${3:-}"
  if [[ -n "$data" ]]; then
    curl -sS -o /dev/null -w "%{time_total}" -X "$method" -H "Content-Type: application/json" --data "$data" "$url"
  else
    curl -sS -o /dev/null -w "%{time_total}" -X "$method" "$url"
  fi
}

urlencode() {
  python3 - "$1" <<'PY'
import urllib.parse, sys
print(urllib.parse.quote(sys.argv[1]))
PY
}

file_size_bytes() {
  local p="$1"
  [[ -f "$p" ]] && stat -c%s "$p" 2>/dev/null || echo ""
}

start_run() {
  # args: query normalizedOnly rowCount batchDelayMs
  local query="$1" normalized="$2" rowCount="$3" batchDelay="$4"
  local payload
  payload="$(cat <<JSON
{
  "query": "$query",
  "engineId": $ENGINE_ID,
  "normalizedOnly": $normalized,
  "engineParameters": {
    "rowCount": "$rowCount",
    "batchDelayMs": "$batchDelay"
  }
}
JSON
)"
  local resp id
  resp="$(curl_json POST "$BASE_URL/dpql/execute" "$payload")"
  if [[ "$HAS_JQ" == "1" ]]; then
    id="$(echo "$resp" | jq -r '.executionId')"
  else
    id="$(echo "$resp" | sed -nE 's/.*"executionId"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p')"
  fi
  if [[ -z "$id" || "$id" == "null" ]]; then
    echo "Failed to start run. Response:" >&2
    echo "$resp" >&2
    exit 1
  fi
  echo "$id"
}

wait_for_status() {
  # args: executionId targetStatus timeoutSec
  local id="$1" target="$2" timeout="$3"
  local start now elapsed resp status
  start="$(date +%s)"
  while true; do
    now="$(date +%s)"
    elapsed=$((now - start))
    if (( elapsed > timeout )); then
      echo "TIMEOUT" >&2
      return 1
    fi

    resp="$(curl_json GET "$BASE_URL/dpql/runs/$id/status")"
    if [[ "$HAS_JQ" == "1" ]]; then
      status="$(echo "$resp" | jq -r '.status // .state // .runState // empty')"
    else
      status="$(echo "$resp" | sed -nE 's/.*"status"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p')"
    fi
    [[ -z "$status" ]] && status="(unknown)"

    # success or terminal
    if [[ "$status" == "$target" || "$status" == "FINISHED" || "$status" == "FAILED" || "$status" == "CANCELED" ]]; then
      echo "$status"
      return 0
    fi
    sleep "$POLL_INTERVAL_SEC"
  done
}

get_results_overview() {
  local id="$1"
  curl_json GET "$BASE_URL/dpql/results/$id"
}

get_first_table_id() {
  local overview="$1"
  if [[ "$HAS_JQ" == "1" ]]; then
    echo "$overview" | jq -r '.tables[0].tableId // .tables[0].id // .tables[0].sourceTableId // empty'
  else
    echo "$overview" | sed -nE 's/.*"tableId"[[:space:]]*:[[:space:]]*([0-9]+).*/\1/p' | head -n 1
  fi
}

page_time_ndjson() {
  # args: executionId tableId offset limit search?
  local id="$1" tid="$2" offset="$3" limit="$4" search="${5:-}"
  local url="$BASE_URL/dpql/results/$id/table/$tid?offset=$offset&limit=$limit"
  if [[ -n "$search" ]]; then
    url="$url&search=$(urlencode "$search")"
  fi
  curl_time GET "$url"
}

page_time_norm() {
  # args: executionId tableId offset limit search?
  local id="$1" tid="$2" offset="$3" limit="$4" search="${5:-}"
  local url="$BASE_URL/dpql/runs/$id/normalized-table/$tid?offset=$offset&limit=$limit"
  if [[ -n "$search" ]]; then
    url="$url&search=$(urlencode "$search")"
  fi
  curl_time GET "$url"
}

cancel_run() {
  local id="$1"
  # body ignored by backend; send {} to satisfy content-type parsing
  curl_json POST "$BASE_URL/dpql/runs/$id/cancel" "{}" >/dev/null || true
}

compute_mean() {
  local values="$1"
  local count=0 sum="0"
  for v in $values; do
    if [[ "$HAS_BC" == "1" ]]; then
      sum="$(echo "$sum + $v" | bc -l)"
    else
      sum="$(python3 -c "print($sum + $v)")"
    fi
    count=$((count + 1))
  done
  if (( count == 0 )); then echo "0"; return; fi
  if [[ "$HAS_BC" == "1" ]]; then
    echo "scale=6; $sum / $count" | bc -l
  else
    python3 -c "print(round($sum / $count, 6))"
  fi
}

# -------------------- Run --------------------
echo "BASE_URL=$BASE_URL"
echo "ENGINE_ID=$ENGINE_ID"
echo "RESULTS_DIR=$RESULTS_DIR"
echo "REPEAT_COUNT=$REPEAT_COUNT"
echo "jq available: $HAS_JQ"
echo "bc available: $HAS_BC"
echo

# ===================================================================
# E1: Small denormalized
E1_QUERY='SELECT X, Y FROM CC(*) AS X, CC(*) AS Y WHERE FD(X,Y) AND IND(Y,X) AND UCC(X)'
E1_ROW_COUNT=100
E1_IS_NORMALIZED=false
E1_ID="$(start_run "$E1_QUERY" "$E1_IS_NORMALIZED" "$E1_ROW_COUNT" 0)"
E1_STATUS="$(wait_for_status "$E1_ID" "FINISHED" 600)"
E1_OVERVIEW="$(get_results_overview "$E1_ID")"
E1_TID="$(get_first_table_id "$E1_OVERVIEW")"
E1_P0_SAMPLES=""
for i in $(seq 1 "$REPEAT_COUNT"); do
  E1_P0_SAMPLES="$E1_P0_SAMPLES $(page_time_ndjson "$E1_ID" "$E1_TID" 0 100 "")"
done
E1_P0="$(compute_mean "$E1_P0_SAMPLES")"
E1_FILE_BYTES="$(file_size_bytes "$RESULTS_DIR/$E1_ID.ndjson")"
echo "E1, Execution_ID:$E1_ID, Is_Normalized:$E1_IS_NORMALIZED, Row_Count:$E1_ROW_COUNT, Query:\"$E1_QUERY\", Status:$E1_STATUS, NDJSON_Page_0:$E1_P0, File_Bytes:$E1_FILE_BYTES"

# ===================================================================
# E2 + E3: Large denormalized and paging cost
# ===================================================================
E2_QUERY='SELECT X, Y FROM CC(*) AS X, CC(*) AS Y WHERE FD(X,Y) AND IND(Y,X) AND UCC(X)'
E2_ROW_COUNT=1000000
E2_IS_NORMALIZED=false
E2_ID="$(start_run "$E2_QUERY" "$E2_IS_NORMALIZED" "$E2_ROW_COUNT" 0)"
E2_STATUS="$(wait_for_status "$E2_ID" "FINISHED" 7200)"
E2_OVERVIEW="$(get_results_overview "$E2_ID")"
E2_TID="$(get_first_table_id "$E2_OVERVIEW")"
E2_P0_SAMPLES="" E2_P100K_SAMPLES=""
for i in $(seq 1 "$REPEAT_COUNT"); do
  E2_P0_SAMPLES="$E2_P0_SAMPLES $(page_time_ndjson "$E2_ID" "$E2_TID" 0 100 "")"
  E2_P100K_SAMPLES="$E2_P100K_SAMPLES $(page_time_ndjson "$E2_ID" "$E2_TID" 100000 100 "")"
done
E2_P0="$(compute_mean "$E2_P0_SAMPLES")"
E2_P100K="$(compute_mean "$E2_P100K_SAMPLES")"
E2_FILE_BYTES="$(file_size_bytes "$RESULTS_DIR/$E2_ID.ndjson")"
echo "E2+E3, Execution_ID:$E2_ID,Is_Normalized:$E2_IS_NORMALIZED, Row_Count:$E2_ROW_COUNT, Query:\"$E2_QUERY\", Status:$E2_STATUS, NDJSON_Page_0:$E2_P0, NDJSON_Page_100K:$E2_P100K, File_Bytes:$E2_FILE_BYTES"

# ===================================================================
# E4: Normalized mode + SQL paging
# ===================================================================
E4_QUERY='SELECT X, Y FROM CC(*) AS X, CC(*) AS Y WHERE FD(X,Y) AND IND(Y,X) AND UCC(X)'
E4_ROW_COUNT=100000
E4_IS_NORMALIZED=true  
E4_ID="$(start_run "$E4_QUERY" "$E4_IS_NORMALIZED" "$E4_ROW_COUNT" 0)"
E4_STATUS="$(wait_for_status "$E4_ID" "FINISHED" 7200)"
E4_OVERVIEW="$(get_results_overview "$E4_ID" || true)"
E4_TID="$(get_first_table_id "$E4_OVERVIEW")"
[[ -z "$E4_TID" ]] && E4_TID=1
E4_NP0_SAMPLES="" E4_NSEARCH_SAMPLES=""
for i in $(seq 1 "$REPEAT_COUNT"); do
  E4_NP0_SAMPLES="$E4_NP0_SAMPLES $(page_time_norm "$E4_ID" "$E4_TID" 0 100 "")"
  E4_NSEARCH_SAMPLES="$E4_NSEARCH_SAMPLES $(page_time_norm "$E4_ID" "$E4_TID" 0 100 "y_1")"
done
E4_NP0="$(compute_mean "$E4_NP0_SAMPLES")"
E4_NSEARCH="$(compute_mean "$E4_NSEARCH_SAMPLES")"
E4_FILE_BYTES="$(file_size_bytes "$RESULTS_DIR/$E4_ID.ndjson")"
echo "E4, Execution_ID:$E4_ID, Is_Normalized:$E4_IS_NORMALIZED, Row_Count:$E4_ROW_COUNT, Query:\"$E4_QUERY\", Status:$E4_STATUS, NDJSON_PageNorm0:$E4_NP0,NDJSON_PageNormSearch0:$E4_NSEARCH, File_Bytes:$E4_FILE_BYTES"

# ===================================================================
# E5: Cancellation
# ===================================================================
E5_QUERY='SELECT X, Y FROM CC(*) AS X, CC(*) AS Y WHERE FD(X,Y) AND IND(Y,X) AND UCC(X)'
E5_ROW_COUNT=5000000
E5_IS_NORMALIZED=false

E5_ID="$(start_run "$E5_QUERY" "$E5_IS_NORMALIZED" "$E5_ROW_COUNT" 50)"
# wait until RUNNING or terminal
wait_for_status "$E5_ID" "RUNNING" 60 >/dev/null || true

# capture file size before cancel (may not exist yet)
before_bytes="$(file_size_bytes "$RESULTS_DIR/$E5_ID.ndjson")"
[[ -z "$before_bytes" ]] && before_bytes=0

start_cancel_ms="$(date +%s%3N)"
cancel_run "$E5_ID"
final_status="$(wait_for_status "$E5_ID" "CANCELED" 60)"
end_cancel_ms="$(date +%s%3N)"
cancel_latency_ms=$((end_cancel_ms - start_cancel_ms))

# check file growth after cancel
sleep 2
after_bytes="$(file_size_bytes "$RESULTS_DIR/$E5_ID.ndjson")"
[[ -z "$after_bytes" ]] && after_bytes=0
delta_bytes=$((after_bytes - before_bytes))

E5_FILE_BYTES="$(file_size_bytes "$RESULTS_DIR/$E5_ID.ndjson")"
echo "E5, Execution_ID:$E5_ID, Is_Normalized:$E5_IS_NORMALIZED, Row_Count:$E5_ROW_COUNT, Query:\"$E5_QUERY\", Status:$final_status, File_Bytes:$E5_FILE_BYTES, Cancel_Latency_ms:$cancel_latency_ms,Cancel_File_Growth_Bytes:$delta_bytes"

# ===================================================================
# Summary
# ===================================================================
echo
echo "=== Summary ==="
echo "E1  Small denormalized (100 rows, page-0 mean of $REPEAT_COUNT): $E1_P0 s"
echo "E2  Large denormalized (1M rows): page-0=$E2_P0 s, page-100k=$E2_P100K s"
echo "E4  Normalized (100k rows): page-0=$E4_NP0 s, search=$E4_NSEARCH s"
echo "E5  Cancel latency: ${cancel_latency_ms}ms, file growth in Bytes: ${delta_bytes}B"
echo
echo "All timing values are means of $REPEAT_COUNT runs (where applicable)."
echo "Done."