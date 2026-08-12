# Pre-handoff verification checklist

**Run this against the generated bundle before deploying or handing it off.** Every item
here is a bug that has shipped in a real migration despite the guidance existing — the
rules are only useful if you *check* the output against them. Translating from memory and
skipping this step is the single biggest source of broken bundles.

If you can run the CLI, also run `databricks bundle validate` (catches the structural
ones). This checklist catches the SQL/semantic ones that `validate` cannot.

## How to use

For **each** item: run the grep, **read every match**, and fix or justify it. A match is
not automatically a bug (some are in comments/strings), but every match must be looked at.
Do not skip an item because "it probably doesn't apply" — that assumption is how these
ship.

```bash
# run from the bundle root; scan generated source
SRC="src"
```

**Scan `.py` notebooks too, not just `.sql` files.** Every SQL dialect check below applies
equally to SQL embedded in notebook `spark.sql("…")` strings — the same overwrite,
double-quote, `datediff`, and lateral-alias bugs ship inside notebooks and are missed if you
only grep `src/sql/`. `$SRC="src"` covers both; don't narrow it.

## SQL dialect (non-Databricks source → Databricks) — `references/snowflake-sql.md`

- [ ] **Double-quoted identifiers** → backticks. `grep -rnE '"[A-Z_][A-Z0-9_]*"' $SRC` — inside SQL these are string literals on Databricks and fail (`Syntax error at or near '"X"'`). Must be `` `X` ``. (Exclude Python/widget strings.)
- [ ] **`SELECT *, <expr> AS <existing_col>`** (column overwrite) → `SELECT * EXCEPT (col), <expr> AS col`. `grep -rnE 'SELECT (t\.)?\*,' $SRC` (catches both `SELECT *,` and `SELECT t.*,`) then check each alias isn't already an upstream column (`COLUMN_ALREADY_EXISTS`).
- [ ] **`* EXCEPT (col)` where `col` does NOT exist upstream** (the mirror bug — over-applied `EXCEPT`) → drop it from the `EXCEPT`. `grep -rnE '\* EXCEPT \(' $SRC` and, for **each** listed column, confirm it's a real column of the FROM relation. A column that is *created in this same SELECT* (a new derived alias) or created *later in the chain* must **not** be in `EXCEPT` — `EXCEPT` only removes columns to be overwritten. Listing a not-yet-existing column fails with `UNRESOLVED_COLUMN` on the `EXCEPT` list itself. (This is the single most common generation slip when a calculator adds brand-new columns: use plain `SELECT *, <expr> AS newcol` — no `EXCEPT` — for new columns; reserve `EXCEPT` for genuine overwrites.)
- [ ] **`UPDATE … SET … FROM`** → `MERGE`. `grep -rniE 'UPDATE .* SET' $SRC` — Databricks has no `UPDATE…FROM`.
- [ ] **`MERGE`/`UPDATE` target exists** → `CREATE TABLE IF NOT EXISTS` before it for persistent output tables. `grep -rniE 'MERGE INTO' $SRC`.
- [ ] **Sibling SELECT-alias reference** (Snowflake lateral alias) → move to a CTE. Look for `<expr> AS \`X\`` and a later expression in the *same* SELECT referencing `` `X` `` (`UNRESOLVED_COLUMN`). **This includes a window `OVER (PARTITION BY \`X\`)` that references an alias `X` created in the same SELECT** — that fails with a *different*, easily-missed error: `UNSUPPORTED_FEATURE.LATERAL_COLUMN_ALIAS_IN_WINDOW`. Same fix: compute `X` in an inner CTE, apply the window in the outer. `grep -rniE 'PARTITION BY' $SRC` and confirm each partition/order key is a real upstream column, not one aliased in that SELECT.
- [ ] **`datediff(unit, a, b)` / `dateadd(unit, n, d)`** → `datediff(end, start)` / `date_add(d, n)` (no unit arg). `grep -rniE 'datediff\(|dateadd\(' $SRC`.
- [ ] **`to_char(x)` (1-arg)** → `CAST(x AS STRING)`; **`REGEXP_SUBSTR(…,1,1,'i')`** → `regexp_extract(…,0)`; **5-arg `regexp_replace(…,1,0,'i')`** → drop trailing args; **regex backrefs `\1`** → `$1`. `grep -rniE "to_char\(|REGEXP_SUBSTR|, 1, 0, 'i'\)|\\\\[0-9]" $SRC`.
- [ ] **`COUNT DISTINCT(x)`** → `COUNT(DISTINCT x)`. `grep -rniE 'COUNT DISTINCT\(' $SRC`.
- [ ] **`SPLIT_TO_TABLE` / `LATERAL FLATTEN`** → `LATERAL VIEW explode(split(...))`. `grep -rniE 'SPLIT_TO_TABLE|FLATTEN' $SRC`.
- [ ] **`IFF`/`NVL`/`::`/`NUMBER`/`VARCHAR`** handled per `snowflake-sql.md` (some carry over, some don't).
- [ ] **Snowflake DDL/table-op keywords with no Databricks equivalent.** `grep -rniE 'TRANSIENT TABLE|TEMPORARY TABLE|\* EXCLUDE|TRY_TO_DOUBLE|TRY_TO_NUMBER|FROM DUAL' $SRC`. `CREATE … TRANSIENT TABLE` / `TEMPORARY TABLE` → plain `CREATE OR REPLACE TABLE` or `CREATE OR REPLACE TEMPORARY VIEW` (Databricks has no session temp *table*); `SELECT * EXCLUDE (c)` → `* EXCEPT (c)`; `TRY_TO_DOUBLE(x)`/`TRY_TO_NUMBER(x)` → `try_cast(x AS DOUBLE/DECIMAL)`; `… FROM DUAL` → drop the `FROM DUAL` (Databricks `SELECT` needs no FROM).
- [ ] **`TIMESTAMPADD`/`TIMESTAMPDIFF` with a *quoted* unit** → unquoted keyword unit. `grep -rniE "timestampadd\(\s*'|timestampdiff\(\s*'" $SRC` — Snowflake's `TIMESTAMPADD('minute', n, ts)` fails on Databricks; use `timestampadd(MINUTE, n, ts)` / `timestampdiff(MINUTE, a, b)` (bare keyword). (`date_trunc('minute', ts)` is different — that quoted unit is correct.)
- [ ] **`information_schema` discovery must fold case + resolve the WHERE self-reference.** If any SQL introspects `information_schema.tables` (dynamic table discovery), `grep -rniE 'information_schema' $SRC` and confirm: (1) the compared column is wrapped in `UPPER(table_name)` for every `LIKE`/`CASE` against UPPER-case patterns — UC stores unquoted names **lower-case**, so bare `table_name LIKE 'ABC_%'` silently matches nothing → empty lookup; (2) a Snowflake `WHERE startswith(TABLE_NAME, "ALIAS")` that references a **column aliased in the same SELECT** is moved to an **outer query** (Databricks can't reference a sibling alias in `WHERE`); (3) `information_schema` is per-catalog — `IDENTIFIER(:catalog || '.information_schema.tables')`, filtering `table_schema = :schema`.
- [ ] **`WITH RECURSIVE` series generators** → `sequence()` + `explode`. `grep -rniE 'WITH RECURSIVE|RECURSIVE ' $SRC` — Snowflake recursive CTEs (e.g. a time-interval or number series) should become `SELECT explode(sequence(start, stop, step))`. Recursive CTEs exist only on newer DBR and are easy to get wrong; prefer `sequence`. If you keep one, confirm the target runtime supports it.
- [ ] **A `VIEW`'s backing table must not be dropped.** `grep -rniE 'CREATE (OR REPLACE )?VIEW' $SRC` — for every non-materialized view, confirm no later `DROP TABLE` in the same notebook removes a table the view reads (a Snowpark proc that builds a view over a generated interval/staging table, then "cleans up" that table, leaves a dangling view → `UC_DEPENDENCY_DOES_NOT_EXIST` at query time). Keep the backing table (refresh via `CREATE OR REPLACE`), or materialize the output as `CREATE OR REPLACE TABLE … AS`.
- [ ] **Mangled date literals** e.g. `'date'2024-06-03''` → `DATE '2024-06-03'`. `grep -rnoE "'date'[0-9]" $SRC`.
- [ ] **`SNOWFLAKE.CORTEX.*` / `ai_generate_text`** → `ai_gen` / `ai_query` (note: `ai_generate_text` is notebook-only, fails in a SQL warehouse task).
- [ ] **`UNION` branches align — by *name*, not just count.** `grep -rniE 'UNION ALL' $SRC`. `UNION`/`UNION ALL` combines branches **positionally**, ignoring column names. The dangerous case (Matillion "unite"/pivot components produce it constantly): each branch computes a *different single metric* — `SELECT key, count AS RISKS …` in one branch, `SELECT key, count AS AUDITS …` in the next — so a downstream `SUM(\`AUDITS\`)`/`GROUP BY` fails with `UNRESOLVED_COLUMN` because position 2 is sometimes RISKS, sometimes AUDITS. Fix: **every branch must expose the full metric set in the same order**, its own metric populated and the rest zero-padded: `SELECT key, count AS RISKS, CAST(0 AS BIGINT) AS AUDITS, …` / `SELECT key, CAST(0 AS BIGINT) AS RISKS, count AS AUDITS, …`. Verify each branch's SELECT list is identical column-name-by-column-name. **A branch reading a table that lacks one of the metric/attribute columns must pad it with a *typed* null (`CAST(NULL AS STRING) AS col`)** — e.g. an org-structure UNION where one source (a trend table) has `level_1..10` but not `level_1_kid..` → pad the missing `*_kid` with `CAST(NULL AS STRING)`, or the branch fails `UNRESOLVED_COLUMN`.
- [ ] **Untyped `NULL` columns become `VOID` — a later write/merge into them fails.** `grep -rnE '\bNULL AS [a-z_]+' $SRC` (SQL) and, in setup/PySpark, any column that is `None` in **every** row. A bare `NULL AS event_title` creates a column of type **VOID**; a downstream `MERGE`/`UPDATE`/`INSERT` that puts a real `STRING` into it fails with `[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] cannot cast "STRING" to "VOID"`. **Type every placeholder null:** SQL → `CAST(NULL AS STRING) AS event_title`; PySpark `spark.createDataFrame` on Python rows where a column is all-`None` → Spark can't infer the type (`[CANNOT_DETERMINE_TYPE]`), so give it an explicit schema, or build the DataFrame without that column and add it back with `.withColumn(c, F.lit(None).cast("date"))`. Applies especially to SCD "current row" markers (`to_date` always NULL) and empty title/flag placeholders.

## Half-translated components — the "gave up but shipped it anyway" class

**The single most dangerous failure: a component the generator couldn't translate, emitted
as a literal placeholder, and left in the file.** Every one is a guaranteed parse/analysis
error, and they cluster in the hardest transforms (multi-step joins, aggregates, filters).
A transform is **not done** until zero of these remain — a leftover marker means that
component was never actually translated, so re-derive it from the source component, don't
just delete the marker.

- [ ] **No `TODO` / `translate` / `unknown_` markers.** `grep -rniE 'TODO|/\* *translate|unknown_[0-9-]+' $SRC` — the generator writes these when it can't handle a component. Each one is an untranslated component; translate it properly.
- [ ] **No empty filter predicates.** `grep -rnE 'WHERE\s*(/\*|$|\))' $SRC` — a `WHERE /* TODO: \`AND\` */` (or bare `WHERE`) means a Matillion **filter** component's condition was dropped. Recover the predicate from the source `filter`/`WHERE` slot.
- [ ] **No empty/placeholder aggregates.** `grep -rnE '\(`Group By`\)|AS ``|GROUP BY\s*$' $SRC` — `(\`Group By\`) AS \`\`` is an untranslated **aggregate** component: fill in the real grouping keys and aggregate expressions (`COUNT(...)`, `SUM(...)`) from the source.
- [ ] **No untranslated join conditions.** `grep -rniE '= *[a-z]\.(Left|Right|Inner|Outer)\b|ON .*\b(Left|Inner)\b' $SRC` — `ON l.X = r.Left` means a **join** component's key mapping wasn't translated; write the real `ON a.key = b.key`.
- [ ] **No doubled alias prefixes.** `grep -rnE '`[a-z]\.[A-Za-z_]+\.' $SRC` — `r.\`r.RISK_ID\`` (alias baked *into* the backticked name) is a broken column reference; it must be `r.\`RISK_ID\``.
- [ ] **`decode(...)` translated** → `CASE`/`map`. `grep -rniE '\bdecode\(' $SRC` — Snowflake/Oracle `decode` isn't Databricks SQL.

## Matillion leftovers

- [ ] **No `$T{…}` / `${…}` placeholders** in emitted SQL. `grep -rnE '\$T?\{' $SRC` — inter-component/variable refs must be resolved to a view/table name or a `:param`.
- [ ] **`${var}` not used inside `.sql` files — this includes `${catalog}.${schema}.TABLE` namespaces.** SQL tasks do **not** interpolate `${var}` (`[PARSE_SYNTAX_ERROR] at or near '$'`); a `.sql` file with `CREATE TABLE ${catalog}.${schema}.T` fails on every run. `grep -rnE '\$\{' $SRC`. Fix: reference each object as `IDENTIFIER(:catalog || '.' || :schema || '.TABLE_NAME')` and pass `catalog`/`schema` (plus any extra namespaces like `catalog_adl`, `schema_mgmt`) as the SQL task's `parameters:`. `USE CATALOG/SCHEMA IDENTIFIER(:x)` works only when there's a **single** namespace; a transform that reads from several (main data + org-structure + mgmt) needs the per-object `IDENTIFIER(:cat || '.' || :sch || '.T')` form.

## DAB structure — `references/dab-gotchas.md`

- [ ] **Resource paths start with `../`** (`notebook_path` / `file.path` in `resources/*.yml`). `grep -rnE 'notebook_path:|path:' resources/`.
- [ ] **`run_if`, not `depends_on … outcome:`** for success/failure routing. `grep -rn 'outcome:' resources/`.
- [ ] **First task creates the schema** (`CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema)`).
- [ ] **Serverless** — no `job_clusters:` / `new_cluster`. `grep -rnE 'job_cluster|new_cluster' resources/`.
- [ ] **Notebook-source format** — every `notebook_task` `.py` starts with `# Databricks notebook source`; magics on `# MAGIC` lines.
- [ ] **`# MAGIC` lines only inside a magic cell (preceded by `# COMMAND ----------`).** `grep -rnE '^# MAGIC' src/` — a stray `# MAGIC %md`/`%pip` line dropped into an ordinary Python cell (no `# COMMAND ----------` separators around it) executes as an invalid line magic at run time: `UsageError: Line magic function %md not found`. Either give the notebook real `# COMMAND ----------` cell boundaries around every `# MAGIC` block, or (for purely decorative markdown) delete the `# MAGIC` lines and use plain `#` comments.
- [ ] **No `${var}` inside a YAML flow-map** in `resources/*.yml`. `grep -rnE '\{[^}]*\$\{var' resources/` — `parameters: { catalog: ${var.catalog} }` fails to parse (`did not find expected ',' or '}'`): the `}` in `${var.x}` closes the flow-map early. Use **block style** (`parameters:` then indented `catalog: ${var.catalog}`).
- [ ] **Bundle files actually sync** — if the bundle dir is under a git-ignored path (`git check-ignore <bundle>/databricks.yml`), add a `sync: { include: [src/**] }` or nothing uploads (`bundle deploy` warns "no files to sync", tasks fail with `notebook … not found`).

## Setup notebook (synthetic data) — `SKILL.md` Step 5c

- [ ] **`%pip install dbldatagen` + `dbutils.library.restartPython()`** in the first cells (before imports).
- [ ] **Column coverage is COMPLETE — verify mechanically, do not eyeball.** Every column every transform reads from a source table must be generated. This is *the* dominant setup failure and it is not grep-able by eye (a real project reads 100+ columns across many tables). **Run `python3 scripts/check_setup_coverage.py <bundle-dir>`** (see `SKILL.md` Step 5c): it parses every read-node (`SELECT … FROM <source_table>`) across all transforms, diffs against what each `DataGenerator`/setup block generates, and lists the missing columns per table. It must report **zero missing** before you run. A missing column surfaces one-at-a-time as `UNRESOLVED_COLUMN` across many job runs otherwise. Include hyphenated/digit-prefixed names (`` `445_YYYY-MM` ``) — they need backticks in the generator's `AS`. (Runs anywhere Python runs: a shell for Claude Code, `executeCode` for Genie — the skill install ships `scripts/`, so it's available on both paths.) **Read its output, don't just its exit code:** notebook-heavy setups use column-declaration and read styles the static parser can't always see, so it prints a `WARNING: matched FROM <table> but extracted 0 columns` for any read it couldn't parse — those tables are *unverified*, not confirmed-covered, so eyeball them.
- [ ] **Date-dimension tables get real date math, not random values.** A calendar/date-dimension source (columns like `FULL_DATE`, `MONTH_NAME`, `WEEK_COMMENCING`, `445_QUARTER`, `PREVIOUS_WORKING_DATE`, `*_FLAG`) must be generated by `explode(sequence(start, end))` over dates + Spark date functions (`date_format`, `weekofyear`, `date_trunc`, `last_day`, `quarter`, `extract(YEAROFWEEK …)`), **not** `dbldatagen` random values — transforms join/filter on its real semantics (e.g. `WHERE FULL_DATE >= …`, `PARTITION BY YEAR_MONTH`). Random values technically satisfy column-coverage but produce garbage results and can still fail type-sensitive predicates.
- [ ] **No all-`None` column in a `spark.createDataFrame(rows, [...])` from Python tuples.** If a column (commonly the SCD `to_date` / `leaving_date` marker) is `None` in every row, Spark can't infer its type → the whole setup notebook dies with `[CANNOT_DETERMINE_TYPE] Some of types cannot be determined after inferring`. Give the DataFrame an explicit schema, or build it without that column and add it back typed (`.withColumn("to_date", F.lit(None).cast("date"))`). `grep -nE ', None,|None\)' $SRC/setup/*.py` and check each all-None column. (dbldatagen setups don't hit this — they declare types per `withColumn`.)
- [ ] **dbldatagen options valid** — `percentNulls` (not `nullProbability`); `DateType` `begin/end` are date-only, `TimestampType` full `YYYY-MM-DD HH:MM:SS`.
- [ ] **Every `DataGenerator(...)` sets `seedColumnName="_seq"`.** `grep -c 'dg\.DataGenerator(' $SRC/setup/*.py` must equal `grep -c 'seedColumnName' $SRC/setup/*.py`. The implicit `id` seed collides with any `ID`/`id` column → `AMBIGUOUS_REFERENCE: Reference \`ID\` is ambiguous`. Set it on every block unconditionally (harmless when there's no `ID`) — don't decide per-table.
- [ ] **FK ranges ⊆ PK ranges** so joins match (mind the seed base value).

## Final

- [ ] `databricks bundle validate -t dev` passes (if CLI available).
- [ ] All notebook `.py` files compile.
- [ ] After a test run, no task failed with any error class above.

Every failure class here maps to a real run-time error we've hit; a clean pass on this
checklist is the difference between "translated" and "actually runs".
