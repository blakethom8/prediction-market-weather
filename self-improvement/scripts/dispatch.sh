#!/usr/bin/env bash
# dispatch.sh — Chief orchestration dispatcher
# Picks the top task from BACKLOG.md, creates a lock file, and spawns Codex.
#
# Usage:
#   ./dispatch.sh              # auto-picks next task
#   ./dispatch.sh TASK-001     # runs a specific task
#
# Output: prints the Codex session command to stdout (Chief runs it via exec+pty)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SI="$REPO_ROOT/self-improvement"
ACTIVE="$SI/agent-tasks/active"
DONE="$SI/agent-tasks/done"
BACKLOG="$SI/agent-tasks/BACKLOG.md"

# ── pick task ──────────────────────────────────────────────────────────────────
if [[ "${1:-}" =~ ^TASK-[0-9]+ ]]; then
  TASK_ID="$1"
else
  # Find first TASK-XXX in backlog that doesn't already have an active lock
  TASK_ID=$(grep -oE '\[TASK-[0-9]+\]' "$BACKLOG" | tr -d '[]' | while read tid; do
    [[ ! -f "$ACTIVE/$tid.md" ]] && echo "$tid" && break
  done)
fi

if [[ -z "${TASK_ID:-}" ]]; then
  echo "ERROR: No available tasks in backlog (all active or done)" >&2
  exit 1
fi

# ── extract task context from backlog ─────────────────────────────────────────
TASK_BLOCK=$(awk "/\[$TASK_ID\]/,/^---/" "$BACKLOG" | head -40)
TASK_TITLE=$(echo "$TASK_BLOCK" | grep -oE '\] .*' | head -1 | sed 's/^\] //')

# ── write active lock ─────────────────────────────────────────────────────────
mkdir -p "$ACTIVE"
cat > "$ACTIVE/$TASK_ID.md" <<EOF
# $TASK_ID — Active

**Task:** $TASK_TITLE
**Started:** $(date -u +"%Y-%m-%dT%H:%M:%SZ")
**Status:** running

## Task Block (from BACKLOG)

$TASK_BLOCK
EOF

echo "Dispatching $TASK_ID: $TASK_TITLE" >&2

# ── build prompt ──────────────────────────────────────────────────────────────
TODAY=$(date +%Y-%m-%d)
PROMPT="You are improving the prediction-market-weather betting platform.

REPO: $REPO_ROOT
VENV PYTHON: $REPO_ROOT/.venv/bin/python
PYTHONPATH: src

## Your Task: $TASK_ID — $TASK_TITLE

$TASK_BLOCK

## Instructions

1. Read the relevant source files before touching anything.
2. Make the change described above.
3. Run tests: cd $REPO_ROOT && PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v 2>&1 | tail -15
   - Pre-existing failures (ignore these): test_empty_market_responses_return_empty_list, test_weather_market_filtering_merges_event_and_market_payloads, test_run_auto_betting_session_splits_edge_and_coldmath_budgets
   - Do NOT introduce new failures.
4. Write a brief adjustment log: $SI/adjustments/${TODAY}-${TASK_ID}.md
   Include: what you changed, which files, why, and any caveats.
5. Commit with message: 'fix($TASK_ID): $TASK_TITLE'
6. When fully done, run EXACTLY this command:
   openclaw system event --text \"$TASK_ID complete: $TASK_TITLE\" --mode now"

# ── write prompt to temp file (avoids shell quoting hell) ─────────────────────
PROMPT_FILE=$(mktemp -t chief-task)
echo "$PROMPT" > "$PROMPT_FILE"
echo "$PROMPT_FILE"
