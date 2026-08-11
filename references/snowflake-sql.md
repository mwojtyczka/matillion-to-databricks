# Source SQL dialect → Databricks SQL (worked example: Snowflake)

## The backend axis (independent of the export format)

Matillion runs *on top of* a data warehouse, and the SQL inside a project is written in
**that warehouse's dialect**. The skill migrates the pipeline onto Databricks, so any SQL
not already in Databricks dialect must be translated. The source backend is a **separate
axis from the export format**:

- **Export format** (YAML vs classic JSON) decides how you *parse* the project — see
  `references/classic-json-format.md`. It says nothing about the SQL dialect.
- **Source backend** decides which SQL dialect you *translate*. A backend of **Databricks**
  needs little or none; **Snowflake / Redshift / BigQuery / Synapse / …** each need
  dialect fixes.

These are orthogonal: a Snowflake-backed project can be exported as YAML, and a classic
JSON export can be Databricks-backed. Don't infer the backend from the format.

**How to detect the backend:**
- **Classic JSON:** the top-level `dbEnvironment` field (`"snowflake"`, `"redshift"`,
  `"databricks"`, …).
- **DPC / YAML:** there's no single field — infer it from the project's connection/
  environment config and the SQL idioms themselves (three-part names, `::` casts, etc.).
- Databricks-native projects use `catalog`/`schema` and Spark SQL already — little to do.

**Snowflake is the worked example below** because it's the most common non-Databricks
source, but the same detect-then-translate approach applies to any backend: identify the
dialect, translate each construct to its Spark SQL equivalent. This is a per-statement
translation, orthogonal to the "two decisions" (the *task type* is still chosen by the
ladder). Do it as you map each component.

## Snowflake → Databricks

When the source backend is **Snowflake**, every SQL string — inside `sql-executor` steps,
`table-input` sources, join expressions, calculator formulas — is **Snowflake dialect** and
must be translated to Databricks SQL. Don't carry Snowflake-only syntax across untranslated.

**Snowflake code isn't only SQL, though.** The same steps can embed **Python that runs
*inside* Snowflake** — Snowpark stored procedures and Python UDFs
(`CREATE … LANGUAGE PYTHON`, `import snowflake.snowpark`). That's a different translation
target (→ a Databricks **notebook**, not a SQL task) and is covered in its own section
below. Don't confuse it with Matillion's `python-script` component, which is Python running
on the *Matillion agent* (backend-agnostic) — that's `references/orchestration/python-script.md`.
Three distinct things, three homes:

| Where the code runs | How it shows up | Databricks target | Reference |
|---|---|---|---|
| **Snowflake SQL** | SQL strings in any step | SQL task / CTE query | this doc (below) |
| **Inside Snowflake, as Python** | `CREATE PROCEDURE/FUNCTION … LANGUAGE PYTHON`, Snowpark | **notebook task** (PySpark) | "Snowpark & Python UDFs" below |
| **On the Matillion agent** | `python-script` component (`context.*`, external SDKs) | notebook task (by intent) | `orchestration/python-script.md` |

## Namespace: three-part `db.schema.table` → UC `catalog.schema.table`

Snowflake names are `DATABASE.SCHEMA.TABLE`, and the database is usually a variable
(`${v_e_sales_db}.SALES.DIM_PRODUCTS`). On Databricks:

- The Snowflake **database → a Unity Catalog `catalog`**; the **schema → schema**.
- **Never hardcode the namespace.** Map the Snowflake database variable to the `catalog`
  bundle variable and the schema to `schema`, pass them as SQL task parameters, and set
  the namespace once so tables are unqualified — exactly the pattern in
  `references/variables.md` / `references/gotchas.md`:

  ```sql
  USE CATALOG IDENTIFIER(:catalog);
  USE SCHEMA  IDENTIFIER(:schema);
  CREATE OR REPLACE TABLE dim_products AS SELECT ...;   -- unqualified
  ```
- Surface the Snowflake **warehouse** name (e.g. `REPORTING_WH`, a `table-input`'s
  `Warehouse` param) as a hardcoded value → it maps to a Databricks SQL `warehouse_id`
  bundle variable, not inline SQL. See `references/hardcoded-values.md`.

## `${var}` interpolation

Snowflake-via-Matillion uses `${var}` **string interpolation** inside the SQL text. That
is Matillion-side substitution, not Databricks syntax — and `${var.x}` does **not** work
inside a `.sql` file a SQL task runs (`references/gotchas.md`). Map each `${var}`:

| `${var}` role | Databricks target |
|---|---|
| Database/schema namespace (`${v_e_sales_db}`) | `catalog`/`schema` **SQL task parameter** → `IDENTIFIER(:catalog)` |
| Per-run value (date, mode) | job parameter → `:marker` (`IDENTIFIER()` only if it's an object name) |
| Per-environment config | bundle variable → SQL task parameter |
| Secret/credential | Databricks secret scope (never a parameter) — `references/secrets.md` |

## Function & syntax translation

Only the idioms actually seen in real exports are listed; search the SQL for others.

| Snowflake | Databricks | Notes |
|---|---|---|
| `expr::TYPE` (e.g. `id::NUMBER`, `x::FLOAT`) | `CAST(expr AS TYPE)` | `::` cast is Snowflake shorthand. `NUMBER`→`BIGINT`/`DECIMAL`, `FLOAT`→`DOUBLE`, `VARCHAR`→`STRING`. Databricks also accepts `::` but prefer explicit `CAST`. |
| `IFF(cond, a, b)` | `IF(cond, a, b)` | direct rename |
| `NVL(a, b)` / `NVL2` | `coalesce(a, b)` / `IF` | `NVL` works but `coalesce` is idiomatic |
| `QUALIFY <window predicate>` | `QUALIFY …` | **supported on Databricks SQL** — carries over unchanged |
| `CURRENT_TIMESTAMP()` | `current_timestamp()` | fine; Databricks also accepts no parens |
| `TO_VARCHAR(x)` / `TO_NUMBER(x)` | `CAST(x AS STRING)` / `CAST(x AS ...)` | |
| `LISTAGG(...)` | `array_join(collect_list(...), sep)` or `concat_ws` | check ordering semantics |
| `LATERAL FLATTEN(input => arr)` | `LATERAL VIEW explode(arr)` / `explode()` | for VARIANT/array unnesting |
| `VARIANT` / `OBJECT` / `PARSE_JSON` | `VARIANT` (or `STRING` + `from_json`/`:` access) | Databricks has a native `VARIANT` type; simple cases can stay `STRING` |
| `SNOWFLAKE.CORTEX.AI_COMPLETE(model, prompt)` / `COMPLETE(...)` | `ai_query('<endpoint>', prompt)` (or `ai_gen(prompt)` for a quick default) | Cortex LLM calls → Databricks AI Functions. `ai_query` targets a served model/FM API endpoint; the Snowflake `model` string maps to your chosen endpoint, not 1:1. |
| `SNOWFLAKE.CORTEX.SENTIMENT/SUMMARIZE/TRANSLATE/EXTRACT_ANSWER` | `ai_analyze_sentiment` / `ai_summarize` / `ai_translate` / `ai_extract` | Databricks has task-specific AI SQL functions mirroring each Cortex task |
| `` "Quoted UPPER" `` identifiers | `` `back-ticked` `` / unquoted | see below |
| `NUMBER(38,0)` DDL type | `DECIMAL(38,0)` or `BIGINT` | |
| sequences / `seq.NEXTVAL` | `GENERATED ALWAYS AS IDENTITY` column | model as a Delta identity column |

## Quoted, upper-cased identifiers

Snowflake folds unquoted identifiers to **UPPERCASE** and stores them that way; exported
SQL is often full of `"SCHEMA"."TABLE"."COLUMN"` in caps. Databricks identifiers are
**case-insensitive** and quoted with **backticks**, not double quotes. Translate:

- `"d"."FULL_DATE"` → `` `d`.`full_date` `` (or just `d.full_date`).
- Keep table/column *spelling* consistent with what you emit in DDL; don't mix a
  double-quoted `"MyCol"` (case-sensitive in Snowflake) assumption into Databricks.

## `GRANT` / role management

Snowflake exports frequently embed `GRANT … TO ROLE …` inside an `sql-executor` step
(seen ~20× in real exports). **Do not run Snowflake `GRANT` syntax on Databricks** and
don't fold it into a transform:

- Unity Catalog grants use `GRANT <priv> ON <securable> TO \`<group>\`` and are a
  **governance concern managed separately** (Terraform/bundle `grants`, or a one-off), not
  part of an ETL job. Snowflake **roles** map to **UC groups**, not 1:1.
- Surface each grant in the migration notes (like any hardcoded value) and confirm how the
  user wants it handled — typically dropped from the job and applied via UC governance.

## Snowpark & Python UDFs (Python running *inside* Snowflake)

A Snowflake-backed project may define **stored procedures or UDFs written in Python**,
submitted as SQL DDL but executing Python on Snowflake's engine:

```sql
CREATE OR REPLACE PROCEDURE ${v_e_sales_db}.SALES.BUILD_INTERVALS()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'process_data'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
def process_data(session: snowpark.Session) -> str:
    session.sql("CREATE OR REPLACE TABLE ... AS ...").collect()
    df = session.sql("SELECT DISTINCT site_group FROM ...")
    for row in df.collect():
        session.sql(f"CREATE OR REPLACE VIEW ...").collect()
    return "ok"
';
```

This is **not** a SQL task — it's imperative Python, so on Databricks it becomes a
**notebook task** (per the task-type ladder). Translate the Snowpark idioms to PySpark:

| Snowpark (in Snowflake) | Databricks notebook (PySpark) |
|---|---|
| `CREATE PROCEDURE … LANGUAGE PYTHON … HANDLER='f' AS '…'` | the handler body *is* the notebook; drop the DDL wrapper |
| `def process_data(session: snowpark.Session)` | no session param — `spark` is the ambient entry point |
| `session.sql("…").collect()` | `spark.sql("…")` — DDL/CTAS (`CREATE`, `INSERT`) executes eagerly, so no action is needed; for a `SELECT` you get a lazy DataFrame, add `.collect()` only to pull rows driver-side |
| `session.table("db.schema.t")` | `spark.table("catalog.schema.t")` (namespace via widgets — never hardcode) |
| `session.create_dataframe(...)` | `spark.createDataFrame(...)` |
| `PACKAGES = ('snowflake-snowpark-python')` | dropped — Spark is built in; add real deps via `# MAGIC %pip` |
| `RETURNS STRING` / `return "ok"` status string | drop; a notebook task signals success by completing |
| `EXECUTE AS OWNER` / `EXECUTE AS CALLER` | not applicable — task runs as the job's run-as identity |

- The **SQL strings inside** the Python still need the dialect translation from the rest of
  this doc (`::`→`CAST`, `IFF`→`IF`, three-part names → parameterized namespace, `''`
  doubled-quote escaping unwound to normal string literals, etc.).
- **Python UDFs** (`CREATE FUNCTION … LANGUAGE PYTHON`, scalar/table) → a Databricks
  Python UDF, or better, rewrite as native Spark SQL / built-in functions when the logic
  is expressible that way (UDFs block Catalyst optimization).
- A procedure that just loops issuing DDL (like the example) is usually clearer rewritten
  as ordinary notebook cells or a parameterized loop — migrate by **intent**, not a
  line-by-line port of the Snowpark scaffolding.

## Other Snowflake-isms to watch

- **`CREATE OR REPLACE TABLE … CLONE`** — zero-copy clone; on Databricks use
  `CREATE TABLE … DEEP CLONE` / `SHALLOW CLONE` (Delta), or a plain `CREATE OR REPLACE
  TABLE … AS SELECT` if a clone isn't actually needed.
- **`MERGE`** — syntax is close; verify `WHEN NOT MATCHED BY SOURCE` clauses.
- **Semi-structured `col:field.subfield`** access → Databricks `col.field.subfield` or
  `from_json` + struct access.
- **`ALTER SESSION SET …`** / warehouse hints → drop; not applicable on Databricks.
- **Time-travel `AT(TIMESTAMP => …)`** → Delta `VERSION AS OF` / `TIMESTAMP AS OF`.

## Worked example

The repo's `examples/snowflake-source/` migrates a Snowflake-backed classic-JSON project. See its
`README.md` → "Snowflake → Databricks conversions worth noting" and the per-file headers
in `databricks/src/setup/*.sql`, which annotate each translation (`::`→`CAST`,
`IFF`→`IF`, `QUALIFY` kept, `${v_e_sales_db}`→parameter, `GRANT` dropped).
