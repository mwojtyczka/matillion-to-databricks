# Matillion → Databricks Migration Skill

A [Claude Code](https://docs.claude.com/en/docs/claude-code) **skill** that guides an
AI agent (and you) through converting **Matillion** ETL pipelines into **Databricks**
Jobs and Lakeflow Declarative Pipelines.

It turns Matillion's two pipeline file types into their Databricks equivalents:

| Matillion pipeline | Databricks target |
|---|---|
| `*.orch.yaml` — orchestration pipeline (control flow) | **Databricks Job** (Workflow) |
| `*.tran.yaml` — transformation pipeline (dataflow) | **Lakeflow Declarative Pipeline** |

The skill carries per-component references (joins, aggregates, SQL executors,
nested orchestrations, variables, …), a mapping cheatsheet, a decision guide for
Job-vs-pipeline, and a bank of hard-won gotchas.

---

## What's in this folder

```
SKILL.md                     ← the skill entry point (workflow + decision guide)
references/                  ← per-component + cross-cutting reference docs
  ├─ mapping-cheatsheet.md
  ├─ gotchas.md
  ├─ variables.md
  ├─ deploy-and-validate.md
  ├─ transformation/         ← table-input, join, aggregate, rewrite-table
  └─ orchestration/          ← start-end, sql-executor, run-transformation,
                                run-orchestration, python-script
create-maia-demo-data.orch.yaml    ← real sample pipelines (used in the worked examples)
sales-by-category-region.tran.yaml
README.md                    ← this file
```

**Required for the skill to work:** `SKILL.md` + the `references/` folder.
The two `*.yaml` samples are helpful examples (referenced by the docs) — keep them.
Anything else you received (`docs/`, `.superpowers/`, `.claude/`, `.git/`) is
build/scratch and can be deleted.

---

## Prerequisites

- **Claude Code** installed — https://docs.claude.com/en/docs/claude-code/setup
- For the deploy/validate step: the **Databricks CLI** authenticated to your
  workspace. The skill delegates deployment to Databricks' own tooling; you'll need
  access to a Unity Catalog workspace to actually run the migrated pipelines.

You can use the skill purely to *generate and understand* the converted code without
a workspace; you only need Databricks access for Step 6 (deploy & validate).

---

## Install

A skill is just a folder containing a `SKILL.md`. Put this folder in your personal
Claude Code skills directory.

**macOS / Linux**
```bash
mkdir -p ~/.claude/skills
cp -R matillion-to-databricks ~/.claude/skills/matillion-to-databricks
```

**Windows (PowerShell)**
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

Then start (or restart) Claude Code. Confirm it's loaded:
```
/skills
```
You should see **matillion-to-databricks** in the list.

> **Project-only install (alternative):** to scope the skill to a single repo
> instead of your whole machine, copy the folder to `.claude/skills/` inside that
> repo instead of `~/.claude/skills/`.

---

## How to start a conversion

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
   > Databricks Job and Lakeflow pipelines."**

The skill triggers on Matillion-migration requests and walks the workflow:
**inventory → parse the orchestration/transformation graphs → map each component →
assemble a Databricks Asset Bundle (`databricks.yml`) → deploy & validate.**

If you don't have your own files yet, try it on the included samples:

> **"Using the matillion-to-databricks skill, convert
> `create-maia-demo-data.orch.yaml` and `sales-by-category-region.tran.yaml`."**

---

## What you get out

- A **Databricks Asset Bundle** (`databricks.yml`) with a **Job** per orchestration
  pipeline and a **Lakeflow pipeline** per transformation pipeline.
- The generated **SQL / Python** source for each pipeline (SQL by default; Python
  only where a component needs it).
- Matillion **variables** mapped to bundle variables / Job parameters / task values.
- Deployment via the Databricks CLI and a **validation checklist** (tables exist,
  row counts sane, an aggregate spot-check).

---

## Tips & limitations

- **Read the decision guide.** `SKILL.md` → *"When to use a Databricks Job vs. a
  Lakeflow pipeline"* explains why control flow (conditions, loops, failure
  branching, side effects) becomes a Job and pure dataflow becomes a pipeline. This
  is the call that most affects the result.
- **Placeholders need resolving.** Matillion `[Environment Default]` catalog/schema
  values have no Databricks equivalent — you'll be asked for real Unity Catalog
  names. See `references/gotchas.md`.
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
