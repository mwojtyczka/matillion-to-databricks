# AGENTS.md

Instructions for AI agents working on this repository.

## What this repo is

This repo **is a skill**, not an application. It is a self-contained pack of
Markdown that teaches an agent (or a person) to migrate **Matillion** ETL
pipelines (`*.orch.yaml` / `*.tran.yaml`) to **Databricks** (Jobs with SQL /
notebook tasks, Lakeflow pipelines only when justified, DQX for data quality).

The deliverable is the *documentation*, so "correctness" means: the guidance is
internally consistent, and any code it tells the agent to emit actually deploys
and runs. Code snippets in the docs are copied verbatim by users — treat them
with the same care as production code.

```
SKILL.md            ← entry point: the workflow + the "two decisions" guide
references/          ← per-component + cross-cutting reference docs
examples/demo/       ← one worked before/after example (matillion/ → databricks/)
README.md            ← human-facing overview + install instructions
```

## Core model (do not contradict it)

Every migration is **two nested decisions**, and the whole skill is built on
keeping them straight — see `SKILL.md` → "The two decisions of every migration":

1. **The shell is always a Databricks Job.** Control flow (ordering, branching,
   loops, retries, schedules, parameters) can only live in a Job. Not a
   judgment call.
2. **The task type** — how each step runs *inside* the Job — is picked per task
   via the ladder **SQL task → notebook → Lakeflow pipeline**. Lakeflow is the
   escape hatch (incremental/streaming or multi-table lineage only), never the
   default.

Two things are deliberately **off** that ladder:
- **Data quality → DQX.** Matillion `Assert` / reject-filter logic becomes a
  **DQX notebook** quality-gate task (DQX is a PySpark library, so it needs
  Python — never a plain SQL task) placed *after* the checked table. It never
  dictates the transform's task type or forces Lakeflow. See
  `references/data-quality.md`.
- **Secrets → Databricks secret scopes**, never bundle variables or job
  parameters (those are plaintext). See `references/secrets.md`.

## Working conventions

- **Use the term "task type", never "executor"** for decision 2 — "executor"
  collides with Spark executors. (The Matillion component named `sql-executor`
  keeps its name; that's the source-side artifact.)
- **Consistency across docs is the main hazard.** A single concept is described
  in `SKILL.md`, `references/mapping-cheatsheet.md`, the relevant
  `references/**/*.md`, `README.md`, and `examples/demo/README.md`. When you
  change guidance, grep for every place that states it and update all of them,
  or they drift. After any change, `grep -ri "<old phrasing>"` to confirm no
  stale copies remain.
- **Cross-reference by path, not by `@`** in these docs (e.g.
  `` `references/data-quality.md` ``). Reserve `@`-links for AGENTS/CLAUDE files.
- **Never hardcode a UC namespace.** Catalog/schema are always bundle variables
  passed as SQL-task parameters (`USE CATALOG IDENTIFIER(:catalog)`) or notebook
  widgets. The committed `databricks.yml` ships placeholders, not real hosts/IDs.
- **Code snippets must be runnable as written.** A `.py` shown as a Databricks
  notebook needs the `# Databricks notebook source` first line and `# MAGIC`
  for magics; SQL must use the parameter pattern, not `${var.x}` inside `.sql`;
  no unfillable literals like `/Workspace/.../`. Run `python3
  scripts/check_examples.py` before committing — CI runs it too
  (`.github/workflows/ci.yml`): it parses all YAML and lints/compiles the
  notebook sources under `examples/**/src/`.

## Editing the skill (this is TDD for docs)

This skill was authored under `superpowers:writing-skills`. Before editing
`SKILL.md` or a reference, **invoke that skill** and follow its RED→GREEN→
REFACTOR discipline — decide what failure the change addresses, then match the
guidance form to it. Don't add untested prose "for completeness".

- Keep `SKILL.md` the concise entry point; push detail into `references/`.
- The DQX check *syntax* is owned by the external DQX skills
  (`dqx-define-checks`, `dqx-apply-checks`, `dqx-storage`, …) and its docs. This
  repo covers only *how DQX fits a migration* — link out, don't duplicate the API.
- Keep `examples/demo/` in sync with the guidance. If a rule changes such that
  the demo would now be emitted differently, update the demo too.

## Git

- Don't commit or push unless asked. If asked while on `main`, branch first.
- End commit messages with the `Co-authored-by` trailer, per the harness rules.
