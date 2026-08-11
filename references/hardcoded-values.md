# Surfacing hardcoded values — variable, secret, or leave inline?

Matillion pipelines are full of literals baked into component parameters and
`python-script` / `sql-executor` bodies: catalog/schema names, warehouse and host
identifiers, file paths, bucket names, connection strings, passwords, API tokens,
thresholds, dates. A faithful migration doesn't copy these through blindly — it
**surfaces every hardcoded value, classifies it, and lets you decide the target**, with
a recommended default.

**Rule: don't silently carry a literal across. For each one, surface it, propose a target
(bundle variable / job parameter / Databricks secret / leave inline), explain why, and
confirm with the user before wiring it.**

## Step 1 — Sweep for hardcoded values

Scan every component parameter and every inline SQL/Python body. Usual suspects:

- catalog / schema / table names, `[Environment Default]` placeholders
- warehouse IDs, workspace hosts, cluster/endpoint identifiers
- storage locations — bucket names, mount paths, `s3://` / `abfss://` URLs, volume paths
- connection details — hostnames, ports, database names, account identifiers
- **credentials** — passwords, API tokens, keys, SAS tokens, client secrets
- tuning / business literals — batch sizes, thresholds, cutoff dates, region lists

## Step 2 — Classify each value and pick the target

| Kind of value | Recommended target | Why |
|---|---|---|
| **Credential / secret** (password, token, key, client secret, SAS) | **Databricks secret** (`references/secrets.md`) | Must never be plaintext. Not a variable. |
| **Per-environment config** (catalog, schema, warehouse id, host, bucket/path, connection host/port) | **DAB bundle variable** `${var.x}` (per-target default) | Changes between dev/prod; set once per environment. |
| **Per-run input** (as-of date, run mode, a filter the caller chooses) | **Job parameter** `{{job.parameters.x}}` | Supplied at launch; varies per run. |
| **Computed mid-run** (a value one step writes for a later step) | **task value** (`dbutils.jobs.taskValues`) | Databricks params are immutable within a run. See `references/variables.md`. |
| **True constant** (a fixed business rule, a stable enum, a column list) | **leave inline** (optionally a documented constant) | Parameterizing it adds indirection with no benefit. |

Decision shortcuts:
- **Is it sensitive?** → secret. Always. (When unsure whether something is sensitive,
  treat it as a secret.)
- **Does it change between environments?** → bundle variable.
- **Does it change between runs?** → job parameter.
- **Does it never change?** → leave it inline; don't over-parameterize.

## Cross-database sources — mapping multiple source namespaces

Real projects often read from **several source databases** — e.g. a Snowflake project with
`${v_e_prisma_dm}.PRISMA.*` and `${v_e_adl_dm}.AD_ODA.*`, or a Matillion project spanning
multiple databases/schemas. Databricks UC is a 3-level namespace (`catalog.schema.table`),
and Snowflake's `database.schema.table` maps onto it — but *how* you fold multiple source
databases in is a **decision to put to the user**, not to assume. Three options:

| Option | When it fits | Shape |
|---|---|---|
| **(a) one catalog, multiple schemas** | the source DBs are really one logical domain; you want them co-located and governed together | `${var.catalog}` + a bundle variable per source schema (`prisma_schema`, `adl_schema`); each source DB → a schema |
| **(b) multiple catalogs** | the source DBs map to genuinely separate governance/isolation boundaries (prod vs. reference data, different owners) | a bundle variable per catalog (`prisma_catalog`, `adl_catalog`); each source DB → its own catalog |
| **(c) collapse to one schema** | quick demo/dev where isolation doesn't matter | everything into one `${var.catalog}.${var.schema}`; simplest, but loses the source's separation |

Whichever is chosen, surface **each distinct source database variable** as its own bundle
variable (per the table above) and map every three-part reference in the SQL through it —
don't hardcode. Confirm the mapping with the user; (a) is a sensible default for a
lift-and-shift, (c) for a throwaway test.

## Step 3 — Surface and confirm

Present the findings as a table — *value (or a redacted placeholder for secrets), where
it's used, proposed target, reason* — and let the user accept or override before you wire
anything. Example:

| Found value | Location | Proposed target | Why |
|---|---|---|---|
| `marcin_demo.default` | `python-script` SQL | bundle variable `catalog`/`schema` | per-environment |
| `wh-abc123` | connection | bundle variable `warehouse_id` | per-environment |
| `snowflake_password=…` | connection | **secret** `snowflake_password` | credential |
| `as_of_date='2025-01-01'` | filter | job parameter `as_of_date` | per-run input |
| `region_list IN ('NA','EU')` | filter | leave inline | stable business rule |

Never print a real secret value in this table — show the key name / a placeholder only.

## Gotchas

- **Don't over-parameterize.** Turning genuine constants into variables just adds
  indirection. Parameterize what varies (by environment or run) and what's sensitive.
- **Secrets are never variables.** If a value is a credential, the only correct target is
  a Databricks secret — even if it's "just for dev." See `references/secrets.md`.
- **Watch for the same literal in several places.** Matillion often repeats a
  catalog/schema or host across components and inline scripts (the samples hardcode
  `marcin_demo.default` in one place and use `[Environment Default]` in another). Map all
  occurrences to the **one** variable/secret so they stay consistent.
- **Confirm, don't assume.** The recommended target is a default, not a decision — the
  user may know a "config" value is actually a fixed constant, or that a constant should
  be parameterized for a planned multi-env rollout.
- **Wire it through to the code, not just the bundle.** Declaring a bundle variable isn't
  enough — the SQL/notebook must actually *read* it, or the literal is still hardcoded in
  the file. In a **SQL task**, pass it as a task parameter and reference it with a `:name`
  marker (for catalog/schema: `USE CATALOG IDENTIFIER(:catalog)` + unqualified tables). In
  a **notebook**, use `dbutils.widgets` + `base_parameters`. See
  `references/variables.md` → "Parameterizing catalog/schema in a SQL task".
