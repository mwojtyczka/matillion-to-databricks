# Reading the classic Matillion export (single-file JSON)

Matillion has two export shapes, and the skill handles both:

- **DPC / YAML** — one file per pipeline (`*.orch.yaml`, `*.tran.yaml`), component
  `type:` as a string, graph via inline `transitions` / `sources`. This is the format
  the rest of the references and `examples/databricks-source/` use.
- **Classic / JSON (this doc)** — **one JSON file for the whole project**, bundling every
  job. Older Matillion versions export this way. See
  `examples/snowflake-source/matillion/acme_sales.json` in the repo for a worked example.

The migration *target* is identical either way (a Databricks Job + tasks — "the two
decisions" are unchanged). Only **parsing the source** differs. This doc is the decoder.

> **Format ≠ backend.** The export format (this doc) and the source warehouse dialect
> (`references/snowflake-sql.md`) are **independent** axes. A classic JSON export can be
> backed by any warehouse (Snowflake, Redshift, BigQuery, Databricks, …), and a
> Snowflake-backed project can equally be exported as YAML. Read the `dbEnvironment` field
> to learn the backend — don't infer it from the fact that the file is JSON. This doc
> handles *parsing*; dialect translation is a separate step.

## Top-level shape

```jsonc
{
  "dbEnvironment": "snowflake",       // source backend (any warehouse) — non-Databricks => translate SQL, see references/snowflake-sql.md
  "version": "1.78.10",
  "jobsTree": { ... },                // folder tree + job names/descriptions/types
  "orchestrationJobs": [ { ... } ],   // the control-flow jobs (become Databricks Jobs)
  "transformationJobs": [ { ... } ],  // the dataflow jobs (become Job tasks)
  "variables": [ ... ],               // project variables (see references/variables.md)
  "environments": [ ... ]             // per-environment db/schema/warehouse defaults
}
```

- **`dbEnvironment`** tells you the source backend, hence the SQL dialect. Anything other
  than `databricks` (e.g. `snowflake`, `redshift`, `bigquery`) means the SQL needs dialect
  translation — see `references/snowflake-sql.md` (Snowflake is the worked example, but the
  detect-then-translate approach is the same for any backend). Don't assume the SQL is
  already Databricks-compatible.
- **`jobsTree`** carries the human job names, descriptions, and `type`
  (`ORCHESTRATION` / `TRANSFORMATION`) in nested `children[].jobs[]`. Use it to name the
  Databricks Jobs and to map which transformation a `run-transformation` step calls.
- `orchestrationJobs` / `transformationJobs` are **lists** (a project has many). Inventory
  every entry — one orchestration → one Databricks Job; each transformation → a task.

## A job = components + connectors (not `transitions`)

Each job object holds:

```jsonc
{
  "id": 90001,
  "components": { "1002": { ...one component... }, "1003": { ... } },  // keyed by numeric id
  "successConnectors":       { "2002": {"id":2002,"sourceID":1002,"targetID":1003} },
  "failureConnectors":       { ... },
  "unconditionalConnectors": { "2001": {"id":2001,"sourceID":1001,"targetID":1002} },
  "trueConnectors": {}, "falseConnectors": {}, "iterationConnectors": {},
  "variables": {}, "grids": {}
}
```

- **The step graph lives in the connector lists**, *not* inside each component. Each
  connector is `{sourceID, targetID}` referencing component ids. Build the DAG by walking
  them — this is the classic-format equivalent of YAML `transitions`:
  | Connector list | YAML equivalent | Databricks |
  |---|---|---|
  | `unconditionalConnectors` | `unconditional` transition | task `depends_on` |
  | `successConnectors` | `success` transition | `depends_on` (default `run_if: all_success`) |
  | `failureConnectors` | `failure` transition | failure-condition task (`run_if: all_failed`) |
  | `trueConnectors` / `falseConnectors` | `If` branches | `depends_on` + `run_if` |
  | `iterationConnectors` | grid/loop iterator | `for_each` task |
- **Transformation jobs use a single `connectors` list** (dataflow edges), analogous to
  YAML `sources`. Same source→target decoding.

## Identifying a component's type

Classic JSON has **no `type:` string**. A component carries a numeric
`implementationID`. The reliable signal is the **parameter-name signature** (the set of
`parameters[].name` values) — `implementationID` numbers are internal and not guaranteed
stable across Matillion versions, so match on the signature and use the number only as a
hint. Common mappings seen in real exports:

| `implementationID` | Parameter-name signature | Matillion component | Reference |
|---|---|---|---|
| `444132438` | `Start` | `start` | `orchestration/start-end.md` |
| `-1946388514` | `Name` | `end-success` | `orchestration/start-end.md` |
| `515156205` | `Name` | `end-failure` | `orchestration/start-end.md` (→ failure-condition terminal, or just the Job's failure state) |
| `-1343684451` | `Name` | `and`/`or` gate (convergence) | usually **collapses** — see the failure-counting pattern below |
| `-798585337` | `Name`, `SQL Script` | `sql-executor` | `orchestration/sql-executor.md` |
| `-1773186829` | `Name`, `Script`, `Interpreter`, `Timeout`, `User` | `python-script` | `orchestration/python-script.md` |
| `1785813072` | `Name`, `Orchestration Job`, …, `Set Scalar/Grid Variables` | `run-orchestration` | `orchestration/run-orchestration.md` |
| `1896325668` | `Name`, `Transformation Job`, …, `Set Scalar/Grid Variables` | `run-transformation` | `orchestration/run-transformation.md` |
| `-1266674941` / `-1032749985` | `Name`, `SQL Query`, … | query → scalar / table (also seen splitting rows via `LATERAL VIEW explode`) | `orchestration/sql-executor.md` |
| `1744268877` | `Name`, `Columns` | distinct | `transformation/aggregate.md` (`SELECT DISTINCT`) |
| `1838652813` | `Name`, `Conversions` | convert-type | `transformation/rewrite-table.md` (`CAST` in the projection) |
| `1006021671` | `Name`, `Incoming Webhook URL`, `Payload …` | API/webhook step | `orchestration/webhook.md` |
| `335239555` | `Name`, `Target Table`, `Database`, `Schema`, `Warehouse`, `Order By` | `table-input` | `transformation/table-input.md` |
| `-629958239` | `Name`, `Main Table`, `Main Table Alias`, `Joins`, `Join Expressions`, `Output Columns` | `join` | `transformation/join.md` |
| `1701703136` | `Name`, `Groupings`, `Aggregations`, `Grouping Type` | `aggregate` | `transformation/aggregate.md` |
| `1354890871` | `Name`, `Target Table`, `Column Names`, `Schema`, `Database`, `Offset` | table output (rewrite) | `transformation/rewrite-table.md` |
| `-1760161015` | `Name`, `Filter Conditions`, `Combine Conditions` | `filter` | (see data-quality if it rejects rows) |
| `-1357378929` | `Name`, `Mode`, `Condition`, … | `if` / conditional | `mapping-cheatsheet.md` (Job `run_if`) |
| `1716658327` | `Name`, `Include Input Columns`, `Calculations` | calculator | `transformation/aggregate.md` (SELECT expr) |
| `128170095` | `Name`, `Column Mapping`, `Include Input Columns` | rename/map | `transformation/table-input.md` (projection) |
| `-1841822228` | `Name`, `Method`, `Cast Types`, … | unite/convert-type | `transformation/join.md` (UNION) |
| `-1935486466` | `Name`, `Grouping Columns`, `Ordering within partitions`, … | rank/window | `transformation/aggregate.md` (window fn) |

**This table is not exhaustive — expect to hit IDs not listed here** (real projects use
dozens of component types). When you do, **don't guess from the number**: dump the
component's `parameters[].name` list and match by *signature* + its SQL/behavior to the
closest Matillion component, then translate that. A quick way to enumerate what a project
uses:

```python
sigs = {}
for jl in ("orchestrationJobs", "transformationJobs"):
    for job in export[jl]:
        for c in job.get("components", {}).values():
            sigs.setdefault(c["implementationID"],
                            tuple(p["name"] for p in c["parameters"].values()))
for impl, names in sorted(sigs.items()):
    print(impl, names)   # match each against the table above by its param-name signature
```

Flag any you had to infer in the migration notes so the table can be extended.

### In a transformation, the component's ROLE is position-dependent, not type-fixed

**A component's source-vs-sink role in a transformation is set by its DAG position (its
connectors), not by its `implementationID`.** The names in the table above ("table-input",
"table output") describe the *typical* role, but the same impl plays the opposite role
depending on wiring — confirmed common in real exports (28 `rewrite`-as-read and 18
`table-input`-as-write in one project):

| Component | With **no incoming** connectors | With **incoming** connectors |
|---|---|---|
| `rewrite`/table-output (`1354890871`) | acts as a **table read** — `SELECT <Column Names> FROM <Database.Schema.Target Table>` (a *source* feeding the DAG) | acts as a **table write** — `CREATE OR REPLACE TABLE … AS <upstream>` (a *sink*) |
| `table-input` (`335239555`) | acts as a **table read** (the usual case) — `FROM <Database.Schema.Target Table>` | acts as a **table write** — writes its input to `Target Table` (a *sink*) |

The JSON even encodes this: a read node has `inputCardinality: ZERO` / `outputCardinality: MANY`, a write node the reverse — but **derive the role from the connector graph**, not the name. Concretely: **a node with no incoming edges is a source (read); a node with no outgoing edges is a sink (write).** Get this wrong and you invert the whole dataflow. See `references/transformation/rewrite-table.md` for multi-sink handling.

## Reading a component's parameters (slots)

Parameters are slot-numbered and nested `parameters` → `elements` → `values`. A
single-value parameter:

```jsonc
"2": { "slot": 2, "name": "SQL Script",
  "elements": { "1": { "slot": 1,
    "values": { "1": { "slot": 1, "type": "STRING", "value": "CREATE OR REPLACE TABLE ..." } } } } }
```

- **Slot 1 is almost always `Name`** — the component's display name (use it in task keys
  and comments, like the YAML `id`).
- **Single value:** `parameters[slot].elements["1"].values["1"].value`.
- **Grid/multi-row parameter** (e.g. a `join`'s `Joins`, an `aggregate`'s `Groupings`):
  multiple `elements` (rows), each with multiple `values` (columns). Example — one `Joins`
  row is `[target_table, alias, join_type]`; one `Aggregations` row is `[column, function]`.
  Iterate `elements` then `values` to reconstruct the grid.

A tiny helper to pull a named parameter's first value:

```python
def get_param(component, name):
    for p in component["parameters"].values():
        if p.get("name") == name:
            # first row (don't assume it's keyed "1"), first value; guard empties
            for elem in p.get("elements", {}).values():
                for val in elem.get("values", {}).values():
                    return val.get("value")
            return None
    return None
```

## Workflow adjustment for classic JSON

The Step 1–6 workflow in `SKILL.md` is unchanged; only how you *read* the source shifts:

1. **Inventory** — instead of `find *.orch.yaml`, open the one JSON and walk `jobsTree` +
   `orchestrationJobs` / `transformationJobs`. Read `dbEnvironment`; if it's not
   `databricks`, plan for SQL dialect translation (`references/snowflake-sql.md`).
2. **Parse the orchestration graph** — build the DAG from the connector lists (above),
   not `transitions`.
3. **Parse each transformation** — build the dataflow DAG from that job's `connectors`.
4. **Map each component** — identify by parameter-name signature (table above), then use
   the same per-component reference as the YAML format.
5–6. **Assemble the bundle / deploy** — identical to the YAML path.

Worked end-to-end example (in the repo): `examples/snowflake-source/`.
