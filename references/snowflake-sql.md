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
| `TO_VARCHAR(x)` / `TO_NUMBER(x, p, s)` | `CAST(x AS STRING)` / `CAST(x AS DECIMAL(p,s))` | `TO_NUMBER`'s precision/scale args become the `DECIMAL(p,s)` in the CAST |
| `TO_CHAR(x)` (1-arg, no format) | `CAST(x AS STRING)` | Databricks `to_char` **requires** a format arg (`WRONG_NUM_ARGS`); for a plain string conversion use `CAST`. `TO_CHAR(x, fmt)` → `to_char(x, fmt)` |
| `REGEXP_SUBSTR(col, pat, 1, 1, 'i')` | `regexp_extract(col, pat, 0)` | Snowflake's trailing position/occurrence/flag args have no Databricks equivalent — drop them; `regexp_extract`'s 3rd arg is the capture-group index (0 = whole match) |
| `regexp_replace(col, pat, repl, 1, 0, 'i')` | `regexp_replace(col, pat, repl)` | drop the Snowflake position/occurrence/flag trailing args |
| regex backreferences `\1`, `\2` (in a replacement) | `$1`, `$2` | Databricks uses `$N`, not `\N`; and an apostrophe inside a SQL literal is `''`, not `\'` |
| `LISTAGG(...)` | `array_join(collect_list(...), sep)` or `concat_ws` | check ordering semantics |
| `SPLIT_TO_TABLE(col, sep)` (in `LATERAL`) | `LATERAL VIEW explode(split(col, sep)) AS value` | Snowflake table function → Databricks `explode(split(...))`; the exploded column is aliased (`AS value`) |
| `SPLIT_PART(col, sep, n)` | `split_part(col, sep, n)` | **carries over unchanged** (Databricks has `split_part`, 1-indexed, same semantics) |
| `DATEDIFF(day, start, end)` / `DATEDIFF('day', …)` | `datediff(end, start)` | Databricks `datediff(endDate, startDate)` takes **no unit arg** and returns days; drop the `day` part and swap to (end, start) order. For other units use `datediff(unit, start, end)` **only** via the 3-arg TIMESTAMP variant, or `months_between`/arithmetic |
| `DATEADD(day, n, d)` | `date_add(d, n)` / `d + INTERVAL n DAY` | similar — no leading unit arg in `date_add` |
| `LATERAL FLATTEN(input => arr)` | `LATERAL VIEW explode(arr)` / `explode()` | for VARIANT/array unnesting |
| `VARIANT` / `OBJECT` / `PARSE_JSON` | `VARIANT` (or `STRING` + `from_json`/`:` access) | Databricks has a native `VARIANT` type; simple cases can stay `STRING` |
| `SNOWFLAKE.CORTEX.AI_COMPLETE(model, prompt)` / `COMPLETE(...)` | `ai_query('<endpoint>', prompt)` or `ai_gen(prompt)` | Cortex LLM calls → Databricks AI Functions. **In a SQL warehouse task use `ai_gen` / `ai_query`** — `ai_generate_text` is **notebook/interactive-only** and errors in a SQL task (`AI function ai_generate_text is only available in Interactive…`). `ai_query` targets a served/FM endpoint; the Snowflake `model` maps to your chosen endpoint, not 1:1. |
| `SNOWFLAKE.CORTEX.SENTIMENT/SUMMARIZE/TRANSLATE/EXTRACT_ANSWER` | `ai_analyze_sentiment` / `ai_summarize` / `ai_translate` / `ai_extract` | Databricks has task-specific AI SQL functions mirroring each Cortex task |
| `` "Quoted UPPER" `` identifiers | `` `back-ticked` `` / unquoted | see below |
| `NUMBER(38,0)` DDL type | `DECIMAL(38,0)` or `BIGINT` | |
| `CAST(x AS VARCHAR)` (no length) | `CAST(x AS STRING)` | bare `VARCHAR` fails on Databricks (`DATATYPE_MISSING_SIZE` — needs `VARCHAR(n)`); use `STRING`. Same for `CHAR`/`TEXT`. |
| sequences / `seq.NEXTVAL` | `GENERATED ALWAYS AS IDENTITY` column | model as a Delta identity column |
| `TIMESTAMPADD('minute', n, ts)` / `TIMESTAMPDIFF('minute', a, b)` | `timestampadd(MINUTE, n, ts)` / `timestampdiff(MINUTE, a, b)` | Databricks *has* both functions, but the unit is an **unquoted keyword** (`MINUTE`, `HOUR`, …) — the Snowflake **quoted** `'minute'` errors. (Note `date_trunc('minute', ts)` keeps its quoted unit — different function.) |
| `TRY_TO_DOUBLE(x)` / `TRY_TO_NUMBER(x)` | `try_cast(x AS DOUBLE)` / `try_cast(x AS DECIMAL(p,s))` | non-throwing cast |
| `SELECT * EXCLUDE (c)` / `p.* EXCLUDE (c)` | `SELECT * EXCEPT (c)` / `p.* EXCEPT (c)` | Snowflake spells the star-minus-columns `EXCLUDE`; Databricks is `EXCEPT` (and only remove columns that exist — see the semantic note below) |
| `CREATE OR REPLACE TRANSIENT TABLE t` / `TEMPORARY TABLE t` | `CREATE OR REPLACE TABLE t` (or `TEMPORARY VIEW` for session scratch) | `TRANSIENT` is Snowflake-only; Databricks has **no session temp *table*** — use a plain managed table you `DROP` after, or a `CREATE OR REPLACE TEMPORARY VIEW` |
| `SELECT … FROM DUAL` | `SELECT …` (no `FROM`) | Databricks has no `DUAL`; a constant-only SELECT needs no FROM |
| `WITH RECURSIVE … (generate a series)` | `SELECT explode(sequence(start, stop, step)) AS x` | Databricks recursive CTEs are limited/newer; for a time-interval or number series use `sequence()`+`explode` (see Snowpark section) |

## Semantic differences (not just function renames)

These bite even when every function name is right — they're behavioral, and Matillion
"calculator"/"rewrite" components produce them constantly:

- **`SELECT *, <expr> AS <existing_col>` — Snowflake replaces the column; Databricks
  errors** (`[COLUMN_ALREADY_EXISTS]`). A calculator that recomputes a column emits
  `SELECT t.*, <expr> AS COL` where `COL` is already in `t.*`. Databricks won't let a
  result have two `COL`s. Fix with **`SELECT t.* EXCEPT (COL), <expr> AS COL`** (list every
  re-derived column in the `EXCEPT`). This is one of the most common failures on a real
  migration — expect it wherever a transform overwrites an input column.
- **The mirror bug: `SELECT * EXCEPT (col), <expr> AS col` where `col` does NOT yet exist.**
  `EXCEPT` only removes columns *already in the FROM relation*. When a calculator adds a
  **brand-new** column (or one created later in the CTE chain — a `TIMESTAMPADD`-derived
  bucket, a window column, an org-structure key), listing it in `EXCEPT` fails with
  `UNRESOLVED_COLUMN` on the `EXCEPT` list itself. Rule: **new column → plain `SELECT *,
  <expr> AS newcol` (no `EXCEPT`); overwrite of an existing column → `SELECT * EXCEPT (col),
  <expr> AS col`.** Over-applying `EXCEPT` defensively is the most common slip when
  translating a chain of "calculator" components that each *add* columns — it breaks every
  such step.
- **`UPDATE … SET … FROM <other_table>` → `MERGE`.** Snowflake (and Matillion `sql-executor`
  steps) do correlated cross-table updates with `UPDATE t SET … FROM s WHERE t.k = s.k`.
  Databricks **does not support `UPDATE … FROM`** (`Syntax error at or near 'FROM'`) — rewrite
  as `MERGE INTO t USING s ON t.k = s.k WHEN MATCHED [AND <cond>] THEN UPDATE SET …`.
- **The target of a `MERGE`/`UPDATE` must already exist.** A persistent lookup/output table
  the pipeline writes across runs won't exist on a fresh workspace — add `CREATE TABLE IF
  NOT EXISTS <t> (…)` before the first `MERGE` into it (the setup notebook only creates
  *source* tables, not derived outputs).
- **Spark can't reference a sibling SELECT alias; Snowflake can.** Snowflake allows a
  later expression in the same `SELECT` to reference a column *aliased earlier in that same
  `SELECT`* (lateral alias). Spark/Databricks does **not** — `SELECT datediff(...) AS days,
  CASE WHEN days > 30 …` fails with `UNRESOLVED_COLUMN: days`. Matillion "calculator"
  components that build a value and then bucket it in one step produce exactly this. Fix by
  computing the alias in an inner layer (CTE / subquery) and referencing it from the outer
  `SELECT`.
- **Matillion inter-component placeholders (`$T{Component Name}`, `${var}`) must be fully
  resolved.** These are Matillion-runtime references to another component's output or a
  variable — they are **not** SQL. `$T{Fill Null Values}` → the name of the temp
  view/table that component became (e.g. `fill_null_values`); `${var}` → a bundle
  variable / parameter. A leftover `$T{...}` both fails SQL parsing and (inside a Python
  f-string) breaks the notebook — grep for `$T{` / `${` in generated SQL before shipping.

## Quoted, upper-cased identifiers

Snowflake folds unquoted identifiers to **UPPERCASE** and stores them that way; exported
SQL is often full of `"SCHEMA"."TABLE"."COLUMN"` in caps. Databricks identifiers are
**case-insensitive** and quoted with **backticks**, not double quotes. Translate:

- `"d"."FULL_DATE"` → `` `d`.`full_date` `` (or just `d.full_date`).
- Keep table/column *spelling* consistent with what you emit in DDL; don't mix a
  double-quoted `"MyCol"` (case-sensitive in Snowflake) assumption into Databricks.

**This is one of the most pervasive and most-missed translations** — a real export has
hundreds of `"COL"` tokens, and on Databricks a double-quoted string is a **string
literal**, not an identifier, so a leftover `"KID"` fails with `Syntax error at or near
'"KID"'`. Convert **every** double-quoted identifier (columns *and* aliases like `"r"`,
`"s"`) to backticks. When doing this programmatically, scope the replacement to the SQL
text only (not surrounding Python), and convert double-quoted tokens only — leave
**single-quoted** string literals (`'Draft'`, `'<None>'`) untouched.

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

### Dynamic-SQL procedures: discovery + pivot loops

Real Snowpark procedures often **introspect the catalog and build SQL dynamically** — e.g.
"find every raw table matching a pattern, union them, pivot per group". Port the *loop* to
Python that emits `spark.sql(...)`, and translate these constructs (all seen in a real
migration):

- **Recursive-CTE series → `sequence()` + `explode()`.** A `WITH RECURSIVE … TIMESTAMPADD(…)`
  time-interval (or number) generator becomes one statement:
  ```python
  spark.sql(f"""CREATE OR REPLACE TABLE {IQL}.intervals AS
      SELECT explode(sequence(to_timestamp('2021-10-07 00:00:00'),
             date_trunc('DAY', current_timestamp()) - INTERVAL 15 MINUTES,
             INTERVAL 15 MINUTES)) AS time_interval""")
  ```
- **`information_schema` discovery must fold case.** UC stores unquoted names **lower-case**,
  so a Snowflake `WHERE table_name LIKE 'ABERFELDY_%'` (or the big `CASE WHEN table_name
  LIKE 'X_%'` that derives a site group) matches **nothing** and the lookup comes back
  empty. Wrap the compared column in **`UPPER(table_name)`** everywhere the pattern is
  upper-case. `information_schema` is **per-catalog**: `FROM IDENTIFIER(:catalog ||
  '.information_schema.tables') WHERE table_schema = :schema`.
- **Snowflake `WHERE startswith(TABLE_NAME, "ALIAS")` self-reference → outer query.** When the
  filter references a column *aliased in the same SELECT* (the lateral-alias rule, but in
  `WHERE`), wrap the derivation in an inner subquery and filter in the outer.
- **Non-materialized output `VIEW` → keep its backing tables.** If the procedure ends by
  `CREATE VIEW … AS <join over a generated interval table>`, do **not** drop that interval
  table in cleanup — a view with a dropped dependency fails at query time with
  `UC_DEPENDENCY_DOES_NOT_EXIST`. (Materialized `CREATE TABLE AS` outputs can drop their
  staging freely.)
- Everything inside the dynamically-built SQL still needs the dialect fixes above
  (`TIMESTAMPADD` keyword unit, `TRY_TO_DOUBLE`→`try_cast`, `* EXCLUDE`→`* EXCEPT`, `''`→`'`).

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
