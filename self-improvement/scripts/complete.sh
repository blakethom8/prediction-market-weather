#!/usr/bin/env bash
# complete.sh — Called by Chief when a task completion event arrives.
# Moves active lock to done/, optionally prints the adjustment log.
#
# Usage:
#   ./complete.sh TASK-001

set -euo pipefail

TASK_ID="${1:?Usage: complete.sh TASK-XXX}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SI="$REPO_ROOT/self-improvement"
ACTIVE="$SI/agent-tasks/active"
DONE="$SI/agent-tasks/done"

if [[ ! -f "$ACTIVE/$TASK_ID.md" ]]; then
  echo "WARNING: No active lock for $TASK_ID (already done or never started)" >&2
else
  mkdir -p "$DONE"
  mv "$ACTIVE/$TASK_ID.md" "$DONE/$TASK_ID.md"
  # Stamp completion time
  echo "" >> "$DONE/$TASK_ID.md"
  echo "**Completed:** $(date -u +"%Y-%m-%dT%H:%M:%SZ")" >> "$DONE/$TASK_ID.md"
  echo "Moved $TASK_ID → done/" >&2
fi

# Print adjustment log if it exists
TODAY=$(date +%Y-%m-%d)
ADJ_FILE=$(ls "$SI/adjustments/${TODAY}-${TASK_ID}.md" 2>/dev/null || ls "$SI/adjustments/"*"${TASK_ID}.md" 2>/dev/null | tail -1 || true)
if [[ -n "$ADJ_FILE" && -f "$ADJ_FILE" ]]; then
  echo "=== Adjustment Log ===" 
  cat "$ADJ_FILE"
fi
