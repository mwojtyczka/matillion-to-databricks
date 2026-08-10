# Demo: Snowflake-backed Matillion (classic JSON export) → Databricks

A complete worked example of the older
Matillion export format — a **single JSON file** bundling every job — from a project
that ran on **Snowflake**. Contrast with `examples/databricks-source/`, which shows the
newer per-pipeline **YAML** (Data Productivity Cloud) format.

Two **independent** things differ from the databricks-source demo, and the skill handles
each separately (they don't imply each other — a JSON export can be Databricks-backed, and
a Snowflake project can be YAML; this demo just happens to vary both at once):
1. **Export format** — classic single-file JSON (numeric `implementationID`, `connectors`
   lists, slot-based `parameters`) instead of per-pipeline `*.orch.yaml` / `*.tran.yaml`.
   Governs *parsing*. See `references/classic-json-format.md`.
2. **Source warehouse backend** — the SQL is **Snowflake** dialect and the goal is to
   migrate *off* Snowflake onto Databricks. Governs *SQL translation*. See
   `references/snowflake-sql.md`.

```
snowflake-source/
├─ matillion/     ← BEFORE: the original Matillion export
│  └─ acme_sales.json     (one file: orchestration "Load Sales Summary" + transformation "sales_by_region")
└─ databricks/    ← AFTER: the converted Databricks Asset Bundle
   ├─ databricks.yml
   ├─ resources/
   │  └─ job.yml            (Job built from the orchestration — no pipeline resource needed)
   └─ src/
      └─ setup/             (all steps -> Job SQL tasks)
         ├─ 01_seed_raw.sql             (demo-only: seeds the RAW.* fixtures the source assumed)
         ├─ 02_dimension_tables.sql     (sql-executor "Create Dimension Tables")
         ├─ 03_fact_sales.sql           (sql-executor "Load Fact Data")
         └─ 04_sales_by_region.sql      (the transformation, consolidated into one SQL task)
```

> **Note on the source file:** `acme_sales.json` is a small, **synthetic** example built to
> mirror the structure of real classic-format exports (which bundle many jobs and can be
> hundreds of KB). It is not a real customer project.

## Pipeline shape (before / after)

**Before — Matillion (classic JSON, Snowflake).** One `.json` bundles both jobs. The
orchestration "Load Sales Summary" is a control-flow DAG wired by `connectors`; the
`run-transformation` step calls the "sales_by_region" transformation (its own dataflow
DAG). SQL is Snowflake dialect on `${v_e_sales_db}.SALES.*`:

```mermaid
flowchart TD
    subgraph orch["orchestration: Load Sales Summary"]
        S([Start]) --> CD[sql-executor<br/>Create Dimension Tables]
        CD --> LF[sql-executor<br/>Load Fact Data]
        LF --> RT[run-transformation<br/>Run Sales By Region]
        RT --> E([End Success])
    end
    subgraph tran["transformation: sales_by_region"]
        TF[table-input<br/>FACT_SALES] --> JN[join<br/>Join Regions]
        TR[table-input<br/>DIM_REGIONS] --> JN
        JN --> AG[aggregate<br/>Aggregate By Region] --> TO[table-output<br/>SALES_BY_REGION]
    end
    RT -.calls.-> tran
```

**After — Databricks Job** (`resources/job.yml`), running on Unity Catalog with the
Snowflake SQL translated to Databricks dialect. Each step is a SQL task; the transformation
DAG collapses into the single `sales_by_region` SQL task (the `seed_raw` task is demo-only):

```mermaid
flowchart TD
    subgraph job["Job: load_sales_summary_job"]
        SR[SQL task<br/>seed_raw<br/><i>demo fixtures only</i>] --> DT[SQL task<br/>dimension_tables]
        DT --> FS[SQL task<br/>fact_sales]
        FS --> SB[SQL task<br/>sales_by_region<br/><i>consolidated CTE query</i>]
    end
```

## How the classic JSON format is read

Unlike the YAML format, this single `.json` encodes job identity and the step graph
structurally — components keyed by numeric `implementationID`, the graph in separate
`connectors` lists, slot-numbered parameters, and all jobs under
`orchestrationJobs` / `transformationJobs` + `jobsTree`. The decoding rules and the
`implementationID` → component-type map are in **`references/classic-json-format.md`**.

## What maps to what

| Matillion (before) | Databricks (after) | Why |
|---|---|---|
| `acme_sales.json` → orchestration "Load Sales Summary" | **Job** `load_sales_summary_job` (`resources/job.yml`) | Control flow → Job |
| `Start` / `End Success` | *(no task — graph boundaries)* | Boundaries carry no work |
| `sql-executor` "Create Dimension Tables" | SQL task → `src/setup/02_dimension_tables.sql` | Seed/DDL setup |
| `sql-executor` "Load Fact Data" | SQL task → `src/setup/03_fact_sales.sql` | Seed/DDL setup |
| `run-transformation` "Run Sales By Region" | **SQL task** → `src/setup/04_sales_by_region.sql` | Pure full-refresh SQL, single output → SQL task (not Lakeflow) |
| transformation "sales_by_region" (`table-input`/`join`/`aggregate`/`table-output`) | one consolidated query (CTE) in that SQL file | Whole chain → one `CREATE OR REPLACE TABLE`, not one dataset per component |

> The `seed_raw` task (`01_seed_raw.sql`) has **no row above** because it has no
> counterpart in the Matillion source — the source assumes the `RAW.*` tables already
> exist. It's added purely so the demo runs standalone; ignore it when comparing this
> demo's before/after against the databricks-source demo.

## Snowflake → Databricks conversions

The dialect-translation rules (namespace, `::` casts, `IFF`, `QUALIFY`, `GRANT`, quoted
identifiers, Snowpark/Python UDFs, …) live in **`references/snowflake-sql.md`**. Rather
than repeat them here, each `databricks/src/setup/*.sql` file has a header comment
annotating the specific translations applied to that step — read those alongside the
reference to see the rules in action.

## Try it yourself

Point the skill at `matillion/` and compare its output to `databricks/`:

> "Using the matillion-to-databricks skill, convert the Matillion project in
> `examples/snowflake-source/matillion/`."

## Deploy (optional)

```bash
cd databricks
databricks bundle deploy -t dev --var warehouse_id=<id>
databricks bundle run load_sales_summary_job -t dev
```

> Illustrative output for learning the mapping. Review and adjust (warehouse ID,
> catalog/schema, host) before running against a real workspace.
