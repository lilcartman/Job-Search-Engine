#!/bin/bash
# launchd entry point for the daily sync. Triggered both at 8:00 AM and at
# login (RunAtLoad), because launchd skips calendar jobs missed while the
# machine is powered off — the login trigger catches those up. The stamp
# file guard keeps it to at most one sync per 20 hours.
set -euo pipefail

PROJECT_DIR="/Users/williamcartebisbee/Claude Projects/Job Search Engine/Friend Engine Operand"
STAMP="$PROJECT_DIR/.last_sync_success"
MIN_INTERVAL=$((20 * 60 * 60))

if [[ -f "$STAMP" ]]; then
  last=$(cat "$STAMP")
  now=$(date +%s)
  if (( now - last < MIN_INTERVAL )); then
    echo "Last sync $(( (now - last) / 3600 ))h ago — skipping"
    exit 0
  fi
fi

# Progress bars written to a non-TTY log balloon it by megabytes per run.
export HF_HUB_DISABLE_PROGRESS_BARS=1

cd "$PROJECT_DIR"
echo "=== sync started $(date '+%Y-%m-%d %H:%M:%S') ==="
./venv/bin/python -m etl.sync --variant minimal
# Utah/Remote scan DISABLED (2026-07-09): it fully replaces the
# utah_remote_jobs table and clobbered a curated 130-row version.
# Re-enable deliberately if you ever want the shortlist regenerated.
# ./venv/bin/python -m scripts.utah_remote_scan
date +%s > "$STAMP"
echo "=== sync finished $(date '+%Y-%m-%d %H:%M:%S') ==="
