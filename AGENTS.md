# AGENTS.md

Instructions for AI agents working on this repository.

## What this repo is

This repo **is a skill**, not an application. It is a self-contained pack of
Markdown that teaches an agent (or a person) to migrate **Matillion** ETL
pipelines to **Databricks** (Jobs with SQL / notebook tasks, Lakeflow pipelines
only when justified, DQX for data quality). It handles a source along **two
independent axes** (don't conflate them):
- **Export format** — newer per-pipeline **YAML** (`*.orch.yaml` / `*.tran.yaml`)
  or older single-file **JSON** (all jobs in one `.json`). Governs *parsing*:
  `references/classic-json-format.md`.
- **Source warehouse backend** — Databricks (already Spark SQL) or a non-Databricks
  warehouse (Snowflake, Redshift, …) whose SQL is translated. Governs *dialect*:
  `references/snowflake-sql.md`.
A JSON export can be Databricks-backed and a Snowflake project can be YAML — the
classic-format JSON files are Snowflake only by coincidence.

The deliverable is the *documentation*, so "correctness" means: the guidance is
internally consistent, and any code it tells the agent to emit actually deploys
and runs. Code snippets in the docs are copied verbatim by users — treat them
with the same care as production code.

```
SKILL.md            ← entry point: the workflow + the "two decisions" guide
references/          ← per-component + cross-cutting reference docs
examples/
  databricks-source/  ← worked example: DPC/YAML source (Databricks-backed)
  snowflake-source/   ← worked example: classic JSON source (Snowflake-backed)
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
  `references/**/*.md`, `README.md`, and **both** demo READMEs
  (`examples/databricks-source/README.md`, `examples/snowflake-source/README.md`). When you
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
  notebook sources under `examples/**/src/`, and compiles the helper scripts.
- **`scripts/check_setup_coverage.py <bundle-dir>` is a generation-time tool**, not a
  repo-CI check: run it against a *generated* bundle to prove the synthetic-data setup
  notebook produces every column the transforms read (the dominant setup failure). It's
  static (no workspace), and referenced from `SKILL.md` Step 5c + the verification
  checklist. CI only compiles it so it can't rot.
- **Helper scripts have unit tests under `tests/`** (`pytest`, config in `pyproject.toml`,
  a second CI job runs `python3 -m pytest`). The setup-coverage parser is regex-based and
  its correctness is the whole point — if you change its parsing, add/adjust a fixture in
  `tests/test_check_setup_coverage.py`. Tests are pure Python (no workspace/credentials).

## Editing the skill (this is TDD for docs)

This skill was authored under `superpowers:writing-skills`. Before editing
`SKILL.md` or a reference, **invoke that skill** and follow its RED→GREEN→
REFACTOR discipline — decide what failure the change addresses, then match the
guidance form to it. Don't add untested prose "for completeness".

- Keep `SKILL.md` the concise entry point; push detail into `references/`.
- **Externally-owned APIs are linked, never duplicated here.** The DQX check
  *syntax* is owned by the external DQX skills (`dqx-define-checks`,
  `dqx-apply-checks`, `dqx-storage`, …) and its docs; the **dbldatagen** API (for
  the setup-notebook synthetic data) is owned by the `databricks-data-generation`
  skill + <https://databrickslabs.github.io/dbldatagen/>. This repo covers only
  *how each fits a migration* — link out, don't hardcode their option names/API
  (a copied option list goes stale and invites wrong-name bugs).
- Keep **both** demos (`examples/databricks-source/`, `examples/snowflake-source/`) in
  sync with the guidance. If a rule changes such that a demo would now be emitted
  differently, update it too.
- The `examples/snowflake-source/matillion/*.json` source is **synthetic**, distilled to
  mirror the real classic-format structure. Don't commit real customer exports.

## Git

- Don't commit or push unless asked. If asked while on `main`, branch first.
- End commit messages with the `Co-authored-by` trailer, per the harness rules.
