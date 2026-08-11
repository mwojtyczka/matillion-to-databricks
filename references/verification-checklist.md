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

## SQL dialect (non-Databricks source → Databricks) — `references/snowflake-sql.md`

- [ ] **Double-quoted identifiers** → backticks. `grep -rnE '"[A-Z_][A-Z0-9_]*"' $SRC` — inside SQL these are string literals on Databricks and fail (`Syntax error at or near '"X"'`). Must be `` `X` ``. (Exclude Python/widget strings.)
- [ ] **`SELECT *, <expr> AS <existing_col>`** (column overwrite) → `SELECT * EXCEPT (col), <expr> AS col`. `grep -rnE 'SELECT (t\.)?\*,' $SRC` (catches both `SELECT *,` and `SELECT t.*,`) then check each alias isn't already an upstream column (`COLUMN_ALREADY_EXISTS`). Only `EXCEPT` columns that truly exist upstream (else `UNRESOLVED_COLUMN` — a column *created* in this SELECT, or one only referenced later in the same SELECT, must NOT be in `EXCEPT`).
- [ ] **`UPDATE … SET … FROM`** → `MERGE`. `grep -rniE 'UPDATE .* SET' $SRC` — Databricks has no `UPDATE…FROM`.
- [ ] **`MERGE`/`UPDATE` target exists** → `CREATE TABLE IF NOT EXISTS` before it for persistent output tables. `grep -rniE 'MERGE INTO' $SRC`.
- [ ] **Sibling SELECT-alias reference** (Snowflake lateral alias) → move to a CTE. Look for `<expr> AS \`X\`` and a later expression in the *same* SELECT referencing `` `X` `` (`UNRESOLVED_COLUMN`).
- [ ] **`datediff(unit, a, b)` / `dateadd(unit, n, d)`** → `datediff(end, start)` / `date_add(d, n)` (no unit arg). `grep -rniE 'datediff\(|dateadd\(' $SRC`.
- [ ] **`to_char(x)` (1-arg)** → `CAST(x AS STRING)`; **`REGEXP_SUBSTR(…,1,1,'i')`** → `regexp_extract(…,0)`; **5-arg `regexp_replace(…,1,0,'i')`** → drop trailing args; **regex backrefs `\1`** → `$1`. `grep -rniE "to_char\(|REGEXP_SUBSTR|, 1, 0, 'i'\)|\\\\[0-9]" $SRC`.
- [ ] **`COUNT DISTINCT(x)`** → `COUNT(DISTINCT x)`. `grep -rniE 'COUNT DISTINCT\(' $SRC`.
- [ ] **`SPLIT_TO_TABLE` / `LATERAL FLATTEN`** → `LATERAL VIEW explode(split(...))`. `grep -rniE 'SPLIT_TO_TABLE|FLATTEN' $SRC`.
- [ ] **`IFF`/`NVL`/`::`/`NUMBER`/`VARCHAR`** handled per `snowflake-sql.md` (some carry over, some don't).
- [ ] **Mangled date literals** e.g. `'date'2024-06-03''` → `DATE '2024-06-03'`. `grep -rnoE "'date'[0-9]" $SRC`.
- [ ] **`SNOWFLAKE.CORTEX.*` / `ai_generate_text`** → `ai_gen` / `ai_query` (note: `ai_generate_text` is notebook-only, fails in a SQL warehouse task).
- [ ] **`UNION` branches align** — every branch selects the same columns in the same order (pad missing metrics with `CAST(0 AS BIGINT)`/`NULL`). `grep -rniE 'UNION ALL' $SRC`.

## Matillion leftovers

- [ ] **No `$T{…}` / `${…}` placeholders** in emitted SQL. `grep -rnE '\$T?\{' $SRC` — inter-component/variable refs must be resolved to a view/table name or a `:param`.
- [ ] **`${var}` not used inside `.sql` files** — SQL tasks read params via `IDENTIFIER(:name)` + `USE`, not `${var}` (which doesn't interpolate). `grep -rnE '\$\{' $SRC`.

## DAB structure — `references/dab-gotchas.md`

- [ ] **Resource paths start with `../`** (`notebook_path` / `file.path` in `resources/*.yml`). `grep -rnE 'notebook_path:|path:' resources/`.
- [ ] **`run_if`, not `depends_on … outcome:`** for success/failure routing. `grep -rn 'outcome:' resources/`.
- [ ] **First task creates the schema** (`CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema)`).
- [ ] **Serverless** — no `job_clusters:` / `new_cluster`. `grep -rnE 'job_cluster|new_cluster' resources/`.
- [ ] **Notebook-source format** — every `notebook_task` `.py` starts with `# Databricks notebook source`; magics on `# MAGIC` lines.

## Setup notebook (synthetic data) — `SKILL.md` Step 5c

- [ ] **`%pip install dbldatagen` + `dbutils.library.restartPython()`** in the first cells (before imports).
- [ ] **Every column every transform reads** from a source table is generated (union across all transforms; include hyphenated names like `` `445_YYYY-MM` ``). Missing → `UNRESOLVED_COLUMN` at run time.
- [ ] **dbldatagen options valid** — `percentNulls` (not `nullProbability`); `DateType` `begin/end` are date-only, `TimestampType` full `YYYY-MM-DD HH:MM:SS`; distinct seed column if a real `ID` column exists.
- [ ] **FK ranges ⊆ PK ranges** so joins match (mind 0-based `id`).

## Final

- [ ] `databricks bundle validate -t dev` passes (if CLI available).
- [ ] All notebook `.py` files compile.
- [ ] After a test run, no task failed with any error class above.

Every failure class here maps to a real run-time error we've hit; a clean pass on this
checklist is the difference between "translated" and "actually runs".
