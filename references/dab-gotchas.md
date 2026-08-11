# DAB emit gotchas — get the bundle right *by construction*

Mistakes that make a generated Databricks Asset Bundle **fail to deploy or run**, even
though the prose guidance was followed. These are subtle YAML/wiring errors, so they slip
through unless you emit the bundle correctly in the first place.

**Who can catch these, and how — this drives everything below:**

- **If you (the agent) can run a shell/CLI** (e.g. Claude Code): run **`databricks bundle
  validate -t dev`** on the generated bundle and fix every error **before** handing it off
  or deploying. This catches the path and `run_if`/`outcome` errors below automatically.
  Do this as a required step, not an afterthought.
- **If you are Genie in-workspace:** the structural bugs in this file are the ones you
  **cannot** fix in-loop. Genie has a **job-scoped** CLI (trigger/inspect Job runs), which
  is enough to iterate on *transform-content* bugs against an already-deployed Job — but
  **`bundle validate`/`deploy` are outside its allow-list**, so a `resources/*.yml` mistake
  can be neither caught by `validate` nor fixed by a redeploy without going back to the
  user. That makes every rule below **mandatory and correct-by-construction** on the Genie
  path, not a safety net. (Most of the bugs this file documents were shipped by an
  in-workspace generator that couldn't validate.)

Either way: **emit it right.** The rules below are cheap to follow and expensive to debug
after the fact.

## Resource file paths are relative to the resource file, not the bundle root

A job/pipeline definition under `resources/` references source files with a path that is
resolved **relative to that YAML file's own location** (`resources/`), *not* the bundle
root. So a task pointing at `src/...` resolves to `resources/src/...` and fails with
`notebook <...> not found`.

```yaml
# resources/<job>.job.yml  — src/ lives one level UP from resources/
tasks:
  - task_key: risks
    notebook_task:
      notebook_path: ../src/notebooks/risks.py     # ✅ ../  climbs out of resources/
      # notebook_path: src/notebooks/risks.py       # ❌ resolves to resources/src/... → not found
  - task_key: seed
    sql_task:
      file:
        path: ../src/sql/merge.sql                  # ✅ same rule for sql_task file paths
```

**Rule: every `notebook_path` / `file.path` in a `resources/*.yml` starts with `../`.**

## Failure/success routing: `run_if`, never `depends_on … outcome:`

`outcome:` on a `depends_on` entry is **only** legal when depending on an **if/else
condition task**. On a normal task dependency it fails at deploy:

> `The dependency on a non-if/else condition "<task>" specifies an outcome "failed".
> Outcomes can only be specified for if/else condition dependencies.`

To route a task on whether upstream tasks **succeeded or failed**, use the task-level
**`run_if`** field instead (this is the correct translation of the Matillion
failure-counting pattern — see `references/mapping-cheatsheet.md`):

```yaml
# success notification — runs only if everything it depends on succeeded
- task_key: notify_success
  depends_on:
    - task_key: incidents
  run_if: ALL_SUCCESS            # ✅  (not: depends_on … outcome: succeeded)
  notebook_task: { ... }

# failure notification — runs if any upstream dependency failed
- task_key: notify_failure
  depends_on:
    - task_key: incidents
  run_if: AT_LEAST_ONE_FAILED    # ✅  (not: depends_on … outcome: failed)
  notebook_task: { ... }
```

`run_if` values: `ALL_SUCCESS` (default), `ALL_DONE` (run regardless), `NONE_FAILED`,
`AT_LEAST_ONE_FAILED`, `ALL_FAILED`, `AT_LEAST_ONE_SUCCESS`.

## The committed `databricks.yml` ships placeholders — deploy needs real values

`workspace.host` is a placeholder (`https://<your-workspace>...`) and `warehouse_id` may
be empty on purpose, so no environment values are baked in. A bare `databricks bundle
deploy` therefore fails — the host must be set (or a `--profile` that points at the right
workspace) and `warehouse_id` supplied via `--var` (empty → `is not a valid endpoint id`).
See `references/deploy-and-validate.md` for the ready-to-run command.

## The Job doesn't create its schema or its source tables

- **Schema:** the first SQL/setup task must `CREATE SCHEMA IF NOT EXISTS
  IDENTIFIER(:schema)` (the catalog must already exist). Otherwise the first write fails
  with `SCHEMA_NOT_FOUND` on a fresh catalog. See `references/gotchas.md`.
- **Source/input tables:** tables the transforms read but no task produces don't exist on a
  fresh workspace → `TABLE_OR_VIEW_NOT_FOUND`. The synthetic-data setup notebook
  (`SKILL.md` → Step 5c) creates them; it is a **manual pre-step**, run once before the
  first `bundle run`.

## Output table names must be sanitized identifiers, not Matillion component labels

A Matillion "write"/rewrite component's **display name** (e.g. `Records in Both IM_EP and
IM_FP`) is a human label, not a table name. Emitting it verbatim yields invalid SQL like
`CREATE OR REPLACE TABLE ..Records in Both IM_EP and IM_FP AS …` (`Syntax error at or
near '.'`). Every emitted table/view identifier must be a **valid, qualified UC name**:
`{catalog}.{schema}.SNAKE_CASE_NAME` — strip leading punctuation, replace spaces/`&`/etc.
with `_`, and keep it unqualified-safe. Grep the generated SQL for
`CREATE (OR REPLACE )?(TABLE|VIEW) ` and confirm each target is a clean identifier.

## Notebook source files must be notebook-source format

A `.py` used as a `notebook_task` must start with `# Databricks notebook source` and put
magics on `# MAGIC` lines (`# MAGIC %pip install …`). A bare `%pip` line or a missing first
line makes the file not a notebook. See `references/data-quality.md` for the DQX example.

## Use serverless — don't emit a classic `new_cluster`

Emit tasks on **serverless** compute (the recommended default): a `notebook_task` with **no**
`job_cluster_key`, `new_cluster`, or `existing_cluster_id` runs serverless automatically.
Do **not** generate a `job_clusters:` block with a `new_cluster` (`spark_version`,
`node_type_id`, `num_workers`, single-node `spark_conf`) and point tasks at it — that pins
the Job to a classic cluster with slower startup and node-type config to maintain.

```yaml
# ✅ serverless: notebook task, no cluster reference at all
- task_key: risks
  notebook_task:
    notebook_path: ../src/notebooks/risks.py

# ❌ classic cluster — don't emit this for a migration
#   job_cluster_key: shared_cluster        # on the task
#   ...
# job_clusters:                            # + this block
#   - job_cluster_key: shared_cluster
#     new_cluster: { spark_version: ..., node_type_id: ..., num_workers: 1 }
```

SQL tasks run on the SQL warehouse (`warehouse_id`), not a cluster — already serverless-ish.
To convert an existing bundle to serverless: delete every `job_cluster_key:` line and the
whole `job_clusters:` block.

## Deploy is CLI-only, and runs from a *local* directory

`databricks bundle deploy` has no SDK/REST equivalent and runs from a local bundle
directory. If the generated bundle lives in the workspace, it must be **downloaded first**
(`databricks workspace export-dir "<workspace path>" ./bundle` then `cd ./bundle`) before
deploy. See `references/deploy-and-validate.md`.

## Don't hand-hardcode externally-owned APIs

The DQX check API and the `dbldatagen` API are owned upstream (their skills + docs), not by
this repo — an option name copied from memory goes stale and errors (`dbldatagen` rejects
an unknown option with `DataGenError(msg='invalid column option …')`). Author against the
`databricks-data-generation` / DQX skills and their docs, not a remembered list. See
`SKILL.md` → Step 5c and `references/data-quality.md`.
