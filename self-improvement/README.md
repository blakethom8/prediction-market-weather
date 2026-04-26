# Self-Improvement Loop

This folder is the engine for continuous improvement of the prediction market betting platform. It's designed to be read, acted on, and updated by both humans and coding/analysis agents.

---

## Philosophy

The system improves by learning from every settled bet. Each win or loss contains a signal — about forecast accuracy, market behavior, strategy assumptions, or code quality. This folder captures those signals and converts them into concrete changes.

**The loop:**
```
Bet placed → Settles → Reviewed → Insight extracted → Adjustment logged → Code/config changed → Next bet
```

---

## Folder Structure

```
self-improvement/
├── README.md                    ← you are here
├── reviews/                     ← periodic performance reviews (human or agent-written)
│   └── YYYY-MM-DD-review.md
├── insights/                    ← atomic learnings — one file per insight
│   └── YYYY-MM-DD-<slug>.md
├── adjustments/                 ← code/config changes made as a result of learning
│   └── YYYY-MM-DD-<slug>.md
├── inspiration/                 ← external research, papers, strategies worth considering
│   └── YYYY-MM-DD-<slug>.md
└── agent-tasks/                 ← queued tasks for coding agents to pick up
    ├── BACKLOG.md               ← prioritized list of pending improvements
    ├── active/                  ← task files currently being worked
    └── done/                    ← completed tasks (move here when closed)
```

---

## For Coding Agents

Start here:
1. Read `agent-tasks/BACKLOG.md` for prioritized work
2. Read the most recent `reviews/` file for full context
3. Read relevant `insights/` files before touching any affected code
4. When done: write an `adjustments/` file documenting what changed and why
5. Move the task file from `agent-tasks/active/` to `agent-tasks/done/`

When writing code: run `make test` before finishing. The goal is working, tested changes — not plans.

---

## For Analysis Agents

You can trigger a new review cycle by running:
```bash
cd /Users/blake/Repo/prediction-market-weather
PYTHONPATH=src .venv/bin/python -m weatherlab.cli.chief calibration
PYTHONPATH=src .venv/bin/python scripts/morning_scan.py
```

Then write a new `reviews/YYYY-MM-DD-review.md` using the output + any insights from recent settled bets.

---

## Review Cadence

| Trigger | Action |
|---|---|
| Every 10 settled bets | Write a new `reviews/` file |
| Any systematic loss pattern | Write an `insights/` file immediately |
| Any code change from learning | Write an `adjustments/` file |
| New external strategy idea | Write an `inspiration/` file |
| Before transitioning paper → real money | Full review required |
