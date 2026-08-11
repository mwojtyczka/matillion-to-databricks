# Matillion → Databricks Migration Skill

A **skill** — a self-contained pack of instructions and reference docs — that guides an
AI agent (and you) through converting **Matillion** ETL pipelines into **Databricks**
Jobs (with SQL / notebook tasks, and Lakeflow Declarative Pipelines where they're
actually needed), with data quality handled by DQX.

It's written as plain Markdown, so it works with any AI coding tool that can read a
project's files — **Databricks Genie / Assistant**, **Claude Code**, or other
AI assistants. It is **not** specific to any one tool; the install steps below just
show where the two most common ones expect it. Even without an agent, the files are a
readable, worked migration guide you can follow by hand.

It turns Matillion's two kinds of job into their Databricks equivalents:

| Matillion job | Databricks target |
|---|---|
| orchestration (control flow) | **Databricks Job** (Workflow) — always the shell |
| transformation (dataflow) | a **task in that Job** — SQL task (default), notebook, or a Lakeflow pipeline only when incremental/streaming is needed |

It handles a source along **two independent axes** (neither implies the other):

- **Export format** — how the project is parsed:
  - **DPC / YAML** (newer) — one file per pipeline: `*.orch.yaml` + `*.tran.yaml`.
  - **Classic / JSON** (older) — a single `.json` bundling every job. Decoded via `references/classic-json-format.md`.
- **Source warehouse backend** — which SQL dialect gets translated: **Databricks** (already Spark SQL) or a non-Databricks warehouse like **Snowflake / Redshift / BigQuery**, whose SQL is translated to Databricks dialect (`references/snowflake-sql.md`).

The two worked examples pair one point on each axis: `examples/databricks-source/` is YAML + Databricks-backed; `examples/snowflake-source/` is classic JSON + Snowflake-backed. (They deliberately migrate the *same* pipeline so you can diff format-and-dialect against an otherwise-identical example.)

Cutting across both, Matillion **data-quality logic** (`Assert` components,
reject/filter steps) migrates to a **DQX** quality-gate **notebook** task (Python),
placed after the checked table to split valid rows from a quarantine table — see
design principle 8 below and `references/data-quality.md`.

The skill carries per-component references (joins, aggregates, SQL executors,
nested orchestrations, variables, …), a mapping cheatsheet, a decision guide for
picking each task's type (SQL task → notebook → Lakeflow), and a bank of
hard-won gotchas.

---

## Design decisions & principles

These are the deliberate choices the skill makes. They favour **simple, idiomatic,
debuggable Databricks** over a mechanical 1:1 translation of Matillion. Understanding
them explains most of what the skill produces.

1. **The orchestration is always a Databricks Job.** Control flow — ordering,
   `success`/`failure` branching, loops, retries, schedules, parameters — can only live
   in a Job. This part isn't a judgment call.

2. **Prefer the simplest task type for each step: SQL task → notebook → Lakeflow.** For
   every step/transformation, walk that ladder and stop at the first fit:
   - **SQL task** for pure, batch/full-refresh SQL (the common case) — warehouse-native,
     no cluster, cheapest.
   - **Notebook** when there's imperative logic, mixed SQL + Python, or Python, or you just want a
     faithful, steppable landing for a migration.
   - **Lakeflow only when it earns it**. A Lakeflow Declarative Pipeline is a
   separate resource with its own compute and deploy surface. Reach for it *only* when
   you actually use what it provides: incremental/streaming (CDC) or automatic multi-table lineage. 
   A single full-refresh transform uses none of that, so wrapping it in a pipeline is just a SQL task carrying overhead.

3. **Consolidate the transformation dataflow — don't mirror Matillion's step division.**
   Matillion splits a transform into many explicit components (`table-input` → `join` →
   `join` → `aggregate` → `rewrite-table-dl`). Translating each into its own
   table/view/task is faithful but redundant — every intermediate becomes an object that
   gets recomputed each run. Instead, **collapse a linear
   chain that yields one output into a single query using CTEs.** In the demo, 7 Matillion
   components became **one** `CREATE OR REPLACE TABLE … WITH … SELECT` — identical result,
   one object instead of seven. Keep a step separate only when it's genuinely *reused*,
   needs its own *quality gate*, or is a real *branch point*.

4. **Preserve the orchestration graph; don't over-consolidate control flow.** Collapsing
   *dataflow* (point 3) is good; collapsing *control flow* is not. Keep **one Job task per
   Matillion step** so you retain per-step retries, repair-runs, observability, and
   parallelism, as well as easier human reasoning during the migration. 
   Choose *how* each step runs — don't fold the whole pipeline into one
   opaque task/notebook.

5. **Surface every hardcoded value; you choose the target.** No literal is carried
   across blindly — `[Environment Default]` placeholders, catalog/schema names,
   warehouse/host IDs, paths, connection details, credentials, and tuning constants are
   all surfaced. Each is classified with a **recommended** target — a **secret**
   (credentials), a **bundle variable** (per-environment config), a **job parameter**
   (per-run input), or **left inline** (true constants) — and you confirm or override
   before it's wired. See `references/hardcoded-values.md`.

6. **Secrets go to Databricks secrets — never to variables or code.** Credentials the
   Matillion project uses (connection passwords, API tokens, storage keys, or values
   sourced from a cloud secret manager) are migrated into **Databricks secret scopes**
   and referenced at runtime (`{{secrets/scope/key}}` / `dbutils.secrets.get` / a Unity
   Catalog connection). They are **never** turned into bundle variables or job
   parameters (those are plaintext) or written into source files. See
   `references/secrets.md`.

7. **Migrate by intent, not line-by-line.** `python-script` steps that call
   Matillion-runtime APIs (`context.cursor()`, `subprocess`, …) are translated to their
   real payload (usually SQL via `spark.sql(...)`); the Matillion specific runtime plumbing is dropped.

8. **Data quality goes to DQX, decoupled from the transform's task type.** Matillion
   `Assert` components and reject/filter logic migrate to **DQX** (the Databricks data
   quality framework). DQX is a PySpark library, so it runs as a **notebook** task
   (Python) — or inside a Lakeflow pipeline — never a plain SQL task. But it can check a
   table produced by *any* task type, so it doesn't dictate how the transform runs: pick
   the transform's task type on its own merits (even a SQL task), then add a separate DQX
   notebook task after it that splits valid rows from a **quarantine** table (so rejects
   are auditable, not silently `WHERE`d away). "This transform needs quality checks"
   therefore never forces the transform itself to Lakeflow. Check syntax comes from DQX's
   own skills; see `references/data-quality.md`.

> The full rationale lives in `SKILL.md` → *"The two decisions of every migration"* and
> the transformation references. This list is the summary.

---

## What's in this folder

```
SKILL.md                     ← the skill entry point (workflow + decision guide)
references/                  ← per-component + cross-cutting reference docs
  ├─ mapping-cheatsheet.md
  ├─ gotchas.md
  ├─ variables.md
  ├─ secrets.md
  ├─ hardcoded-values.md
  ├─ data-quality.md            ← DQX quality gates (Assert / reject → DQX)
  ├─ classic-json-format.md     ← reading the older single-file JSON export
  ├─ snowflake-sql.md           ← Snowflake → Databricks SQL translation
  ├─ deploy-and-validate.md
  ├─ transformation/         ← table-input, join, aggregate, rewrite-table
  └─ orchestration/          ← start-end, sql-executor, run-transformation,
                                run-orchestration, python-script
examples/
  ├─ databricks-source/      ← worked example, DPC/YAML source (*.orch.yaml + *.tran.yaml)
  └─ snowflake-source/       ← worked example, classic JSON source (Snowflake-backed)
       ├─ matillion/         ← BEFORE: the single-file JSON export
       └─ databricks/        ← AFTER: the converted DAB
README.md                    ← this file
```

See each demo's `README.md` (`examples/databricks-source/`, `examples/snowflake-source/`)
for the full before/after mapping.

**Required for the skill to work:** `SKILL.md` + the `references/` folder.
The `examples/` before/after walkthroughs are helpful (and referenced by the docs) — keep them.
Anything else you received (`docs/`, `.superpowers/`, `.claude/`, `.git/`) is
build/scratch and can be deleted.

---

## Prerequisites

- An **AI coding assistant that can read your project's files** — e.g. Databricks
  Genie / Assistant, Claude Code, or similar. (You can also just read the files
  yourself and follow them by hand.)
- For the deploy/validate step: the **Databricks CLI** authenticated to your
  workspace. The skill delegates deployment to Databricks' own tooling; you'll need
  access to a Unity Catalog workspace to actually run the migrated pipelines.

You can use the skill purely to *generate and understand* the converted code without
a workspace; you only need Databricks access for Step 6 (deploy & validate).

---

## Install / make it available

A skill is just a folder of Markdown (`SKILL.md` + `references/`). "Installing" it
means putting it somewhere your AI tool will read. Two common setups:

### Databricks Genie / Assistant

Databricks' assistant **auto-discovers** skills from a dedicated `.assistant/skills/`
folder — each skill lives in its own subfolder named after the skill, containing
`SKILL.md`. There are two locations
([docs](https://docs.databricks.com/aws/en/genie-code/skills)):

- **User skills** (just you): `/Users/{username}/.assistant/skills/`
- **Workspace skills** (shared): `/Workspace/.assistant/skills/`

The subfolder name should match the skill's `name:` (`matillion-to-databricks`). So the
target layout is:

```
/Users/<you>/.assistant/skills/matillion-to-databricks/
  ├─ SKILL.md
  └─ references/ …
```

**Upload just the two skill files — not the whole repo.** The skill *is* `SKILL.md` +
`references/`; everything else in the repo (`examples/`, `scripts/`, `.github/`, `.git/`,
any local Matillion input) is for developing the skill, not running it. Don't
`import-dir .` — it recursively uploads *everything* (there's no `--exclude` flag) into
what may be a shared path. Upload the two parts explicitly:

```bash
# From the repo root. $ME resolves to your workspace username.
ME=$(databricks current-user me -o json | jq -r .userName)
DEST="/Users/$ME/.assistant/skills/matillion-to-databricks"   # or /Workspace/.assistant/skills/... to share

# 1) SKILL.md — a single file, so use `import` with --format RAW
#    (RAW stores the Markdown as-is; without it the file is treated as a notebook)
databricks workspace import "$DEST/SKILL.md" \
  --file SKILL.md --format RAW --overwrite

# 2) references/ — all Markdown, nothing else, so upload the folder wholesale
databricks workspace import-dir references "$DEST/references" --overwrite
```

Add `-p <profile>` to either command if you use a named CLI profile.

> **Why not the examples?** The worked demos under `examples/` are illustrative and live
> in the repo (browse them on GitHub); the skill doesn't need them installed to run a
> migration, and `examples/**/.databricks/` would otherwise leak your workspace host +
> Terraform state into the skills path. The references mention the examples as *repo*
> pointers, not in-skill links.

(In the UI you can instead **Import** → *File/Folder*, selecting `SKILL.md` and the
`references/` folder.)

**Genie picks it up automatically the next time you use it** (start a new chat thread
after adding or changing a skill). Invoke it by describing a Matillion migration, or
`@`-mention it directly — see
[How to run a conversion in Genie](#how-to-run-a-conversion-in-genie) below.

### Claude Code

Copy the folder into its skills directory so it loads automatically:

*macOS / Linux*
```bash
mkdir -p ~/.claude/skills
cp -R matillion-to-databricks ~/.claude/skills/matillion-to-databricks
```

*Windows (PowerShell)*
```powershell
New-Item -ItemType Directory -Force "$HOME\.claude\skills"
Copy-Item -Recurse .\matillion-to-databricks "$HOME\.claude\skills\matillion-to-databricks"
```

The final layout should be:
```
~/.claude/skills/matillion-to-databricks/
  ├─ SKILL.md
  └─ references/ ...
```

Then start (or restart) Claude Code and confirm it's loaded with `/skills` — you
should see **matillion-to-databricks** in the list.

> **Project-only install (alternative):** to scope the skill to a single repo
> instead of your whole machine, copy the folder to `.claude/skills/` inside that
> repo instead of `~/.claude/skills/`.

---

## How to run a conversion in Genie

Once the skill is installed under `.assistant/skills/matillion-to-databricks/` (above),
the flow is: **upload your Matillion files, start a fresh chat, then prompt.**

1. **Put your Matillion project somewhere in the Workspace** the assistant can read —
   e.g. `/Workspace/Users/<you>/matillion-migration/source/`. Upload every `*.orch.yaml`
   and `*.tran.yaml` you want to migrate, keeping the original folder structure so nested
   `run-orchestration` / `run-transformation` references still resolve:

   ```bash
   # From your local Matillion export (the folder with *.orch.yaml / *.tran.yaml)
   ME=$(databricks current-user me -o json | jq -r .userName)
   databricks workspace import-dir . \
     "/Workspace/Users/$ME/matillion-migration/source" \
     --overwrite
   ```

2. **Start a new chat** (skills are picked up when a thread starts) and prompt. Because
   the skill is auto-discovered you don't have to point at its path — just describe the
   task, or `@`-mention it. A prompt you can copy and edit:

   > **@matillion-to-databricks — migrate the Matillion pipelines in
   > `/Workspace/Users/<you>/matillion-migration/source/` to Databricks, and write the
   > resulting Databricks Asset Bundle into
   > `/Workspace/Users/<you>/matillion-migration/output/`.**

   The skill drives the rest — you don't need to spell out the details in the prompt.

3. **Answer its questions.** The skill will ask for the things it shouldn't guess — the
   target Unity Catalog `catalog.schema` (Matillion `[Environment Default]` has no
   Databricks equivalent), the SQL `warehouse_id`, the Job name, and any other config
   values — plus how to handle each hardcoded value / secret it surfaces. Answer with a
   namespace you have write access to and your preferred names.

4. **Deploy it yourself with the CLI.** Genie *generates* the bundle in your Workspace,
   but it can't deploy it — deploying runs the Databricks CLI (`databricks bundle
   deploy`), which the in-workspace assistant does not execute for you. Pull the
   generated bundle to a machine with the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/)
   and deploy from there:

   ```bash
   # Download the generated bundle from the Workspace to your machine
   ME=$(databricks current-user me -o json | jq -r .userName)
   databricks workspace export-dir \
     "/Workspace/Users/$ME/matillion-migration/output" ./migrated-bundle

   # Set the target host/warehouse in databricks.yml, then deploy + run
   cd ./migrated-bundle
   databricks bundle deploy -t dev
   databricks bundle run <job_name> -t dev
   ```

   You need a Unity Catalog workspace and permission to create the Job (and pipeline, if
   one was emitted). See `references/deploy-and-validate.md`.

5. **Validate.** Genie *can* run SQL, so you can ask it in-chat to run the validation
   checklist (tables exist, row counts sane, an aggregate spot-check) against the
   deployed tables — or run the queries yourself. See `references/deploy-and-validate.md`.

> **Tip:** if the assistant doesn't seem to be using the skill, confirm it's under
> `.assistant/skills/matillion-to-databricks/` with `SKILL.md` at the top, and start a
> **new** chat thread (skill changes only take effect in a fresh thread).

---

## How to start a conversion (Claude Code)

1. Put the Matillion pipeline files you want to migrate somewhere Claude Code can
   read them — easiest is to `cd` into a folder that contains your `*.orch.yaml`
   and `*.tran.yaml` files (or copy them in).
2. Launch Claude Code in that folder:
   ```bash
   cd /path/to/your/matillion-export
   claude
   ```
3. Ask it to convert, e.g.:

   > **"Migrate these Matillion pipelines to Databricks."**

   or point it at specific files:

   > **"Convert `daily_load.orch.yaml` and the transformations it calls into a
   > Databricks Job (SQL/notebook tasks; a Lakeflow pipeline only if one is needed)."**

The skill triggers on Matillion-migration requests and walks the workflow:
**inventory → parse the orchestration/transformation graphs → map each component →
assemble a Databricks Asset Bundle (`databricks.yml`) → deploy & validate.**

If you don't have your own files yet, try it on the included demo and compare its
output to the converted code already in `examples/databricks-source/databricks/`:

> **"Using the matillion-to-databricks skill, convert the pipelines in
> `examples/databricks-source/matillion/`."**

---

## What you get out

- A **Databricks Asset Bundle** (`databricks.yml`) with a **Job** per orchestration
  pipeline; each transformation becomes a task in that Job — a **SQL task** by default,
  a **notebook** where imperative logic is involved, or a **Lakeflow pipeline** only
  when one is actually warranted.
- The generated **SQL / Python** source for each task (SQL by default; Python
  only where a component needs it).
- Matillion **variables** mapped to bundle variables / Job parameters / task values.
- Matillion **secrets** migrated to **Databricks secret scopes**, referenced at runtime
  (never inlined or turned into variables).
- Matillion **data-quality gates** (`Assert` / reject logic) migrated to **DQX** tasks
  that split valid rows from a quarantine table (only when the project has them).
- A **generated `README.md`** at the bundle root that documents the migration: source-project
  summary, bundle layout, a **before/after** Mermaid DAG (Matillion source graph vs. the
  Databricks Job, so you can see what got consolidated), the key translations, variables,
  secrets, a synthetic-data summary, deploy commands, a post-migration checklist, and the
  source file list.
- A **setup notebook** (`src/setup/00_generate_source_data.py`) you run **manually once**
  before the first test run — it fabricates any missing source/input tables with
  **synthetic data** (via `dbldatagen`) so the converted project runs without wiring real
  sources. It's kept out of the Job graph and guarded (`IF NOT EXISTS`), so against a
  workspace that already has the real sources it no-ops.
- A **validation checklist** (tables exist, row counts sane, an aggregate spot-check).
  Deployment itself is a CLI step (`databricks bundle deploy`) you run — in Claude Code
  the agent runs it for you; in Genie you run it from a machine with the CLI (see the
  Genie deploy step above).

---

## Tips & limitations

- **Read the decision guide.** `SKILL.md` → *"The two decisions of every migration"*
  explains the two calls that most affect the result: (1) the orchestration always
  becomes the **Job** (control flow — conditions, loops, failure branching, side
  effects — can only live there); (2) each transformation task picks a task type via
  the ladder **SQL task → notebook → Lakeflow** (Lakeflow only for incremental/
  streaming, not by default).
- **Placeholders need resolving.** Matillion `[Environment Default]` catalog/schema
  values have no Databricks equivalent — you'll be asked for real Unity Catalog
  names. See `references/gotchas.md`.
- **Data quality uses DQX.** `Assert` components and reject/filter logic become DQX
  quality-gate tasks, and the DQX task needs the
  `databricks-labs-dqx` library. See `references/data-quality.md`.
- **Custom `python-script` logic** that uses Matillion-runtime APIs
  (`context.cursor()`, `subprocess`, …) is translated by intent, not line-by-line —
  review it.
- **Coverage grows by component type.** The references cover the components seen so
  far. If your pipelines use a component type that isn't documented, the agent will
  do its best and flag it — send those cases back so the skill can be extended.
- Always review the generated code before deploying to a production workspace.

---

## Feedback

Found a component that wasn't handled well, or a mapping that's off? Note the
Matillion component type and what you expected, and send it back so the relevant
reference can be improved.

---

## License

Released under the [MIT License](LICENSE).
