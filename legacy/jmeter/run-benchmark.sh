#!/usr/bin/env bash
# run-benchmark.sh - End-to-end benchmark: before compaction, run janitor, after compaction.
#
# Usage:
#   ./run-benchmark.sh [--table-path s3://warehouse/db/events] \
#                       [--janitor-config /etc/janitor/config.yaml] \
#                       [--query-service-port 8070] \
#                       [--jmeter-home /opt/jmeter]
#
# Prerequisites:
#   - JMeter installed (JMETER_HOME or --jmeter-home)
#   - Python 3.11+ with fastapi, uvicorn, duckdb installed
#   - iceberg-janitor package installed (for the maintenance step)

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

TABLE_PATH="${TABLE_PATH:-s3://warehouse/db/events}"
JANITOR_CONFIG="${JANITOR_CONFIG:-${PROJECT_DIR}/manifests/dev/config.yaml}"
QUERY_SERVICE_PORT="${QUERY_SERVICE_PORT:-8070}"
QUERY_SERVICE_HOST="${QUERY_SERVICE_HOST:-localhost}"
JMETER_HOME="${JMETER_HOME:-/opt/jmeter}"
JMX_FILE="${SCRIPT_DIR}/iceberg-query-benchmark.jmx"
RESULTS_DIR="${SCRIPT_DIR}/results"
NUM_THREADS="${NUM_THREADS:-5}"
RAMP_UP="${RAMP_UP:-10}"
LOOP_COUNT="${LOOP_COUNT:-20}"
START_TIME="${START_TIME:-2025-01-01T00:00:00}"

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --table-path)       TABLE_PATH="$2"; shift 2 ;;
        --janitor-config)   JANITOR_CONFIG="$2"; shift 2 ;;
        --query-service-port) QUERY_SERVICE_PORT="$2"; shift 2 ;;
        --jmeter-home)      JMETER_HOME="$2"; shift 2 ;;
        --threads)          NUM_THREADS="$2"; shift 2 ;;
        --ramp-up)          RAMP_UP="$2"; shift 2 ;;
        --loops)            LOOP_COUNT="$2"; shift 2 ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

JMETER="${JMETER_HOME}/bin/jmeter"

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
if [[ ! -x "${JMETER}" ]]; then
    echo "ERROR: JMeter not found at ${JMETER}" >&2
    echo "Set JMETER_HOME or pass --jmeter-home" >&2
    exit 1
fi

if [[ ! -f "${JMX_FILE}" ]]; then
    echo "ERROR: JMX file not found at ${JMX_FILE}" >&2
    exit 1
fi

mkdir -p "${RESULTS_DIR}"

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
QUERY_SERVICE_PID=""

start_query_service() {
    echo ">>> Starting query service on port ${QUERY_SERVICE_PORT}..."
    QUERY_SERVICE_PORT="${QUERY_SERVICE_PORT}" \
        python "${SCRIPT_DIR}/query-service.py" &
    QUERY_SERVICE_PID=$!

    # Wait for it to become healthy
    local retries=30
    while (( retries > 0 )); do
        if curl -sf "http://${QUERY_SERVICE_HOST}:${QUERY_SERVICE_PORT}/health" > /dev/null 2>&1; then
            echo "    Query service is ready (PID=${QUERY_SERVICE_PID})."
            return 0
        fi
        sleep 1
        (( retries-- ))
    done
    echo "ERROR: Query service failed to start within 30 seconds." >&2
    kill "${QUERY_SERVICE_PID}" 2>/dev/null || true
    exit 1
}

stop_query_service() {
    if [[ -n "${QUERY_SERVICE_PID}" ]]; then
        echo ">>> Stopping query service (PID=${QUERY_SERVICE_PID})..."
        kill "${QUERY_SERVICE_PID}" 2>/dev/null || true
        wait "${QUERY_SERVICE_PID}" 2>/dev/null || true
        QUERY_SERVICE_PID=""
    fi
}

run_jmeter() {
    local phase="$1"
    local output_csv="${RESULTS_DIR}/${phase}.csv"
    local log_file="${RESULTS_DIR}/${phase}.log"

    echo ">>> Running JMeter benchmark: ${phase}"
    echo "    Results: ${output_csv}"

    # Enable the matching thread group via JMeter properties.
    # Both groups are disabled in the JMX by default. We use
    # JMeter's -J flag to set properties but since the JMX uses
    # enabled="false", we modify a temp copy to enable the right group.
    local tmp_jmx
    tmp_jmx=$(mktemp "${RESULTS_DIR}/${phase}-XXXX.jmx")
    cp "${JMX_FILE}" "${tmp_jmx}"

    if [[ "${phase}" == "before-compaction" ]]; then
        # Enable "Before Compaction" thread group
        sed -i.bak 's/testname="Before Compaction" enabled="false"/testname="Before Compaction" enabled="true"/' "${tmp_jmx}"
    else
        # Enable "After Compaction" thread group
        sed -i.bak 's/testname="After Compaction" enabled="false"/testname="After Compaction" enabled="true"/' "${tmp_jmx}"
    fi

    "${JMETER}" -n \
        -t "${tmp_jmx}" \
        -l "${output_csv}" \
        -j "${log_file}" \
        -Jtable_path="${TABLE_PATH}" \
        -Jquery_service_url="${QUERY_SERVICE_HOST}" \
        -Jquery_service_port="${QUERY_SERVICE_PORT}" \
        -Jnum_threads="${NUM_THREADS}" \
        -Jramp_up="${RAMP_UP}" \
        -Jloop_count="${LOOP_COUNT}" \
        -Jstart_time="${START_TIME}"

    rm -f "${tmp_jmx}" "${tmp_jmx}.bak"
    echo "    Phase ${phase} complete."
}

run_janitor() {
    echo ">>> Running iceberg-janitor maintenance..."
    if command -v janitor &> /dev/null; then
        janitor run --config "${JANITOR_CONFIG}"
    else
        python -m iceberg_janitor.runner.cron --config "${JANITOR_CONFIG}"
    fi
    echo "    Maintenance complete."
}

cleanup() {
    stop_query_service
    echo ">>> Cleanup done."
}

trap cleanup EXIT

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "============================================================"
echo " Iceberg Query Benchmark"
echo "============================================================"
echo " Table:    ${TABLE_PATH}"
echo " Threads:  ${NUM_THREADS}  Ramp-up: ${RAMP_UP}s  Loops: ${LOOP_COUNT}"
echo " Results:  ${RESULTS_DIR}"
echo "============================================================"
echo ""

# Step 1: Start query service
start_query_service

# Step 2: Run benchmark BEFORE compaction
run_jmeter "before-compaction"

# Step 3: Stop query service, run maintenance, restart query service
# (Stop so DuckDB releases any cached state from the old table layout)
stop_query_service
run_janitor
start_query_service

# Step 4: Run benchmark AFTER compaction
run_jmeter "after-compaction"

# Step 5: Stop query service
stop_query_service

# Step 6: Generate comparison report
echo ""
echo ">>> Generating comparison report..."
python "${SCRIPT_DIR}/compare-results.py" \
    --before "${RESULTS_DIR}/before-compaction.csv" \
    --after "${RESULTS_DIR}/after-compaction.csv" \
    --output "${RESULTS_DIR}/comparison-report.txt"

echo ""
echo "============================================================"
echo " Benchmark complete!"
echo " Before results: ${RESULTS_DIR}/before-compaction.csv"
echo " After results:  ${RESULTS_DIR}/after-compaction.csv"
echo " Report:         ${RESULTS_DIR}/comparison-report.txt"
echo "============================================================"
