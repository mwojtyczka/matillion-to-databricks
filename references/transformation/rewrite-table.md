# Matillion `rewrite-table-dl` → the transformation's target table

## What it does in Matillion

Writes (fully replaces) the transformation's output table. Key parameters:
- `sources` — the single upstream component whose rows are written.
- `catalog` / `schema` / `table` — the target location.

"Rewrite" = full overwrite each run (not append/merge).

## Databricks equivalent

The transformation's **output table**. "Rewrite" = full overwrite, which maps to:
- **SQL task (default):** `CREATE OR REPLACE TABLE catalog.schema.table AS SELECT ...`
- **Lakeflow pipeline (only if the task-type ladder lands there):** `CREATE OR REFRESH MATERIALIZED VIEW ... AS SELECT ...`, recomputed each run. Use a `STREAMING TABLE` instead only if the upstream is append-only and you want incremental processing (the sample's `offsetType: "None"` reads argue for full refresh, i.e. a plain table / MV).

Which one you emit is the task-type decision in `references/orchestration/run-transformation.md` — most transformations are a SQL task. **Never** map "rewrite" to `INSERT INTO` (that appends).

## Consolidate the chain — one query, not one dataset per component

This is the key transformation decision. The upstream `table-input` / `join` / `aggregate` components are **not** each a separate table/view. A 1:1 mapping is faithful to Matillion but materializes every intermediate as its own object — recomputed on every run (and in Lakeflow, storage-backed). For a linear chain producing one output, that is pure waste.

**Default: collapse the whole chain into the single target statement, using CTEs for the intermediate components.** In a SQL task that's one `CREATE OR REPLACE TABLE ... AS WITH ... SELECT`; in Lakeflow it's one `CREATE OR REFRESH MATERIALIZED VIEW`. Below the example is written for Lakeflow, but the CTE body is identical in a SQL task — only the leading DDL differs. Identical result, one object to store and refresh instead of N.

```sql
-- The whole sales-by-category-region.tran.yaml chain as a single target MV.
CREATE OR REFRESH MATERIALIZED VIEW my_catalog.my_schema.sample_sales_summary AS
WITH join_products AS (        -- join "Join Products"
  SELECT s.sale_id, s.product_id, s.region_id, s.quantity, s.revenue,
         p.product_name, p.category
  FROM my_catalog.my_schema.sample_sales s
  INNER JOIN my_catalog.my_schema.sample_products p
    ON `s`.`product_id` = `p`.`product_id`
),
join_regions AS (              -- join "Join Regions"
  SELECT sp.sale_id, sp.quantity, sp.revenue, sp.category, r.region_name
  FROM join_products sp
  INNER JOIN my_catalog.my_schema.sample_regions r
    ON `sp`.`region_id` = `r`.`region_id`
)
SELECT category, region_name,  -- aggregate "Aggregate"
       SUM(revenue) AS revenue, SUM(quantity) AS quantity, COUNT(sale_id) AS sale_id
FROM join_regions
GROUP BY category, region_name;
```

**Keep a component as its own materialized view only when it earns it:**
- it is **reused** — more than one downstream dataset reads it (materialize once, not per-consumer),
- it needs independent monitoring or its own quality gate (materialize it so a **DQX** task can check it — see `references/data-quality.md`), or
- it is a genuine **branch/fan-out point** in the DAG (not a link in a linear chain).

**Middle ground — lineage without the storage cost:** declare the intermediates as non-materialized `VIEW`s (`CREATE OR REFRESH VIEW`) and materialize only the target. You keep per-step nodes in the pipeline graph and can inspect them, but Lakeflow doesn't persist them.

## Multiple sinks — one output per write node

The consolidation above assumes a **single** output. Real transformations often have
**several sink nodes** (several write components — recall a sink is any node with incoming
but no outgoing connectors; see `references/classic-json-format.md` for why the role is
position-, not type-, dependent). When there are N sinks, emit **one
`CREATE OR REPLACE TABLE` per sink**, each fed by the branch of the DAG that flows into it:

- **Notebook task** — natural: one `spark.sql("CREATE OR REPLACE TABLE ... AS ...")` call
  per sink, in dependency order. Shared upstream branches become temp views (next section)
  so they're computed once and reused by multiple sinks.
- **SQL task** — chain the statements in one `.sql` file, in order (shared upstream as a
  `CREATE OR REPLACE TEMP VIEW` first, then each target `CREATE OR REPLACE TABLE`).

Don't force a genuinely multi-output transformation into one statement — that's the mirror
of over-consolidation: keep each real output its own table.

## What decides consolidation: DAG *shape*, not component count

The signal for "one CTE query vs. temp-view-per-component" is **the shape of the DAG, not
its size**. A long chain is still one query; a small diamond may need temp views.

- **Linear chain (each component feeds exactly one downstream) → one CTE query, regardless
  of length.** A 33-component chain still consolidates to a single
  `CREATE OR REPLACE TABLE ... AS WITH … SELECT` — one CTE per component, in order. Length
  is not a reason to split; real migrations have consolidated 30+ linear components into one
  query. This runs fine as a SQL task or inside a notebook.
- **Diamond / fan-out (a component's output feeds *two+* downstream components that later
  re-join), or multiple sinks → temp views.** A CTE can't be referenced twice without being
  recomputed, and you can't cleanly express two outputs in one statement. Use a **notebook**
  that emits one `CREATE OR REPLACE TEMP VIEW` per shared/branch component (computed once,
  reused by each consumer), then the sink(s) write tables.

So the test is: *does any component have more than one outgoing edge into paths that
reconverge, or are there multiple sinks?* If no → CTEs. If yes → temp views for the shared
nodes.

```python
# diamond DAG: `base` feeds two branches that re-join -> materialize base once as a temp view
spark.sql("CREATE OR REPLACE TEMP VIEW base AS SELECT ... FROM src")          # shared node
spark.sql("CREATE OR REPLACE TEMP VIEW branch_a AS SELECT ... FROM base ...")
spark.sql("CREATE OR REPLACE TEMP VIEW branch_b AS SELECT ... FROM base ...")
spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{schema}.final AS "
          "SELECT ... FROM branch_a JOIN branch_b USING (k)")
```

Temp views are session-scoped and unmaterialized (no storage, dropped at session end), so
this preserves per-component lineage and avoids recomputing shared branches. A transform
that's purely linear needs none of this no matter how many components it has — reach for
temp views only for the reconvergence/multi-sink cases. See
`references/orchestration/run-transformation.md` for SQL-task vs. notebook.

## Repeated sub-graphs across transformations — extract, don't duplicate

Real projects often repeat an **identical sub-graph in several transformations** — e.g. a
staff/org-structure lookup (dedupe a dimension by `full_name` via `ROW_NUMBER`, again by
`alt_name`, `LEFT JOIN` both, `COALESCE`, derive a key, fill unmatched with fallbacks)
appearing in 5 of 11 transformations. Translating it independently each time copies ~40
lines of identical CTE logic into every output — a maintenance trap (fix a bug in one, miss
the other four).

**Extract the shared logic once and reuse it:**
- **Materialize it as a shared table/view** the transforms read — a `CREATE OR REPLACE VIEW
  <catalog>.<schema>.staff_structure AS …` in its own setup step (or a small Lakeflow MV if
  it's genuinely reused and worth maintaining incrementally), then each transform just joins
  to it. Best when several *separate* Jobs/tasks need it.
- **Or a shared source file** — put the sub-query in `src/shared/staff_structure.sql` and
  have each SQL task `include` / each notebook read it, so the CTE is defined in one place.

Recognize it while parsing (Step 3): if the same component chain — same inputs, same
dedupe/join/`COALESCE` shape — recurs across transformations, flag it as a shared
component and emit it once. This is the transformation-level counterpart of "give a
component its own dataset when it's **reused**" above.

> Migration tip: during initial cutover it's fine to emit the 1:1 mapping so each dataset cross-references its Matillion component for validation. Once outputs are reconciled against the source, consolidate. **Note:** Lakeflow does not drop datasets you remove from a pipeline — after consolidating, manually `DROP` the now-orphaned intermediate MVs or they linger in the schema.

## Worked example (from sales-by-category-region.tran.yaml)

`Write Output` writes the `Aggregate` result to `sample_sales_summary` — the transformation's single output. The chain (`Sales`/`Products`/`Regions` → `Join Products` → `Join Regions` → `Aggregate` → `Write Output`) is linear and yields just this one table, so the reference implementation in the repo (`examples/databricks-source/databricks/src/setup/03_sales_by_category_region.sql`) consolidates it into the **single** query above rather than seven. None of the intermediates are reused or need their own quality gate, so materializing them separately would only add storage and refresh cost.

## Gotchas

- Resolve `[Environment Default]` to the real target catalog/schema before emitting.
- One `.tran.yaml` typically has one `rewrite-table-dl` = one pipeline target. Multiple write components = multiple targets in the same pipeline.
- "Rewrite" semantics = full refresh. Do not translate to `INSERT INTO` (that would append).
