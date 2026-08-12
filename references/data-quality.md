# Data quality → DQX

Data quality in a migrated bundle is handled by **[DQX](https://databrickslabs.github.io/dqx/)** — the Databricks data-quality framework, and **not** by hand-rolled `WHERE` filters.

**The key fact that shapes every decision below:** DQX is a standalone PySpark library. Because it's Python, it needs **Python execution — a notebook task** (or code inside a Lakeflow pipeline); it **cannot** run in a plain SQL task. But it can check a table produced by *any* task type (SQL task, notebook, or pipeline) — and it does **not** require a Lakeflow pipeline of its own. So the DQX check is always a notebook task, but it stays **decoupled from the transform's task type**: you pick the transform's task type on its own merits (even a SQL task), and add DQX as a separate notebook quality-gate task afterward. **"This transform needs data-quality checks" is *not* a reason to promote the transform to Lakeflow.**

## Use the DQX skills for the actual check syntax

This file covers **how DQX fits into a Matillion migration**. It deliberately does **not** duplicate DQX's own API — for defining, applying, storing, and generating checks, use DQX's dedicated skills (or the canonical docs if the skills aren't installed):

| Task | DQX skill | Docs |
|---|---|---|
| Define quality rules (row/dataset/foreach, YAML or classes) | `dqx-define-checks` | [quality_checks_definition](https://databrickslabs.github.io/dqx/docs/guide/quality_checks_definition) |
| Apply rules to a DataFrame/table (annotate or split) | `dqx-apply-checks` | [quality_checks_apply](https://databrickslabs.github.io/dqx/docs/guide/quality_checks_apply) |
| Apply + save valid/quarantine in one call (read → check → write) | `dqx-end-to-end` | [quality_checks_apply](https://databrickslabs.github.io/dqx/docs/guide/quality_checks_apply) |
| Profile a table and generate candidate rules | `dqx-profile-and-generate` | [profiling](https://databrickslabs.github.io/dqx/docs/guide/profiling) |
| Load/save checks (YAML / volume / workspace / Delta) | `dqx-storage` | [checks storage](https://databrickslabs.github.io/dqx/docs/guide/quality_checks_storage) |

DQX skills live at <https://github.com/databrickslabs/dqx/tree/main/skills>. When you need to write a check, invoke the relevant DQX skill rather than guessing the API. **If the DQX skills aren't installed in your runtime** (they're a one-time user install, not something to set up mid-migration), read the linked docs instead — never hardcode the API from memory. You may suggest the user install the DQX skills for a smoother pass.

## When a migration needs DQX

Add DQX wherever the Matillion project **validates, rejects, or gates on data quality**, or wherever the user asks for quality enforcement. Signals in the source pipelines:

- **Assert components** (`Assert Scalar`, `Assert Row Count`, `Assert Table Metadata`, `Assert External`) — Matillion's explicit validation steps. Each becomes one or more DQX checks (row-level or dataset-level aggregate).
- **Filter / reject logic** — a `filter` component or a `WHERE` that drops "bad" rows (nulls, out-of-range, unknown codes) so they never reach the output. In DQX this becomes an `error`-criticality check that **quarantines** those rows instead of silently discarding them, so you keep a record of what failed and why.
- **`If` / conditional transitions that branch on a data condition** (row count = 0, checksum mismatch) — model the condition as a DQX aggregate check; branch the Job on the quarantine count if control flow must react.
- **A `run-transformation` output the user wants monitored** — completeness, uniqueness of a key, referential integrity, freshness. Profile it (`dqx-profile-and-generate`) to bootstrap candidate rules, then curate.

If the Matillion project has none of these and the user doesn't ask for quality gates, **emit no DQX** — same discipline as not emitting a Lakeflow pipeline you don't need.

## How DQX integrates into the bundle

DQX runs as a **notebook (Python) task in the Job**, placed **immediately after the task that produces the table** it checks. It reads the produced table, applies the checks, and writes two outputs: the **valid** rows (the real output) and a **quarantine** table of failing rows annotated with `_errors` / `_warnings`.

```
Databricks Job
├─ task: run_transformation      (SQL task → writes catalog.schema.sales_summary)
├─ task: dq_sales_summary        (notebook task → DQX: reads sales_summary,
│                                  writes sales_summary_valid + sales_summary_quarantine)
└─ task: downstream              (depends_on dq_sales_summary; reads the *_valid table)
```

- Keep the DQX check as its **own task** (don't fold it into the transform) — you get per-step observability, a retryable quality gate, and a clear quarantine boundary in the graph.
- Point downstream tasks at the **valid** output, not the raw table, when the intent is "bad rows must not flow downstream."
- **`error`** criticality = quarantine the row (it must not pass); **`warn`** = keep the row but flag it. Map a Matillion hard-reject to `error`, a soft/monitoring assertion to `warn`.

### Where the checks live

Author checks as **metadata (YAML/JSON)** stored in the bundle (`src/dq/<table>.checks.yml`) and load them at runtime, or keep them in a Delta table for cross-job sharing — see `dqx-storage`. Metadata scales better than inline classes across a migration and lets non-authors review the rules. Parameterize the target `catalog`/`schema` the same way every other task does (SQL task parameters / notebook widgets — see `references/variables.md`); never hardcode the namespace into checks or the DQX notebook.

### Minimal task shape

The check-authoring detail belongs to the DQX skills; here is only the migration wiring — a notebook task that applies stored checks and splits valid/quarantine:

```yaml
# resources/<job>.job.yml — a DQX quality gate task
- task_key: dq_sales_summary
  depends_on:
    - task_key: run_transformation
  notebook_task:
    notebook_path: ../src/dq/dq_sales_summary.py
    base_parameters:
      catalog: ${var.catalog}
      schema: ${var.schema}
      checks_path: ${workspace.file_path}/src/dq/sales_summary.checks.yml
```

This is a **Databricks notebook-source** `.py` file — the first line must be the
`# Databricks notebook source` magic marker, and `%pip` / other magics go on
`# MAGIC` lines. Without both, the file isn't recognized as a notebook and the
task fails to parse. Pass the checks-file location as a fourth widget so nothing
in the body is a non-resolving literal:

```python
# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig
from databricks.sdk import WorkspaceClient

# see the dqx-apply-checks / dqx-storage skills for the full API
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("checks_path", "")   # absolute workspace path to the .checks.yml
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
checks_path = dbutils.widgets.get("checks_path")

dq = DQEngine(WorkspaceClient())
# checks authored per `dqx-define-checks`, stored per `dqx-storage`
checks = dq.load_checks(config=WorkspaceFileChecksStorageConfig(location=checks_path))
df = spark.read.table(f"{catalog}.{schema}.sales_summary")
valid_df, quarantine_df = dq.apply_checks_by_metadata_and_split(df, checks)
valid_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.sales_summary_valid")
quarantine_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.sales_summary_quarantine")
```

## DQX inside a Lakeflow pipeline

If a transform *independently* lands on Lakeflow (because it's incremental/streaming — see `references/orchestration/run-transformation.md`), you can still use DQX inside the pipeline instead of native `EXPECT` expectations — DQX gives you the same quarantine/annotation model with richer built-in checks and reusable, storable rule sets. Lakeflow expectations remain a valid option there, but **DQX is the default quality mechanism across the whole migration**, so the checks look the same whether the checked table was produced by a SQL task, a notebook, or a pipeline. (The DQX code itself always runs as Python — a notebook task, or inside the pipeline — never in a SQL task.)

## DQX also powers gold reconciliation (SKILL.md Step 6b)

Beyond quality *gates*, DQX does **dataset comparison**: its dataset-level **`compare_datasets`**
check (in `databricks.labs.dqx.check_funcs`) row-matches a Job output against a **golden**
table/DataFrame by primary key and reports per-row `row_missing` / `row_extra` and per-column
`changed` detail, with `check_missing_records` and numeric `abs_tolerance` / `rel_tolerance`.
That is the mechanism `SKILL.md` **Step 6b** uses to reconcile migrated output against expected
output (only meaningful against real, not synthetic, source data). It's applied via `DQEngine`
like any other check — **get its exact signature from the DQX docs / `dqx-apply-checks`; don't
hardcode the API** (it isn't in the DQX skills' examples yet).

## Gotchas

- **Don't silently drop rows.** A Matillion filter that rejects bad data becomes an `error` DQX check that **quarantines** — preserve the rejected rows in a quarantine table, don't just `WHERE` them away. The whole point is an auditable record of what failed.
- **Don't reinvent checks in SQL.** DQX has built-ins for null/empty, range, set membership, regex, referential, aggregate, uniqueness, schema, and freshness. Search `check_funcs` (via `dqx-define-checks`) before writing a custom `sql_expression`.
- **Validate checks before the run.** `DQEngine.validate_checks(checks)` catches malformed rules without touching data — cheaper than a failed job task.
- **Parameterize the namespace.** The DQX notebook reads `catalog`/`schema` from task parameters/widgets like every other task — never bake `main.demo` into the DQX source or the checks file.
- **DQX is a library dependency.** Ensure `databricks-labs-dqx` is available to the task (a `# MAGIC %pip install databricks-labs-dqx` line in the notebook, or an environment/cluster library). Note this when handing off the bundle. Pin an explicit version (e.g. `databricks-labs-dqx==<version>`) so runs are reproducible; bump it deliberately rather than tracking latest.
