# Matillion → Databricks Migration Skill

A **skill** — a self-contained pack of instructions and reference docs — that guides an
AI agent (and you) through converting **Matillion** ETL pipelines into **Databricks**
Jobs (with SQL / notebook tasks, and Lakeflow Declarative Pipelines where they're
actually needed).

It's written as plain Markdown, so it works with any AI coding tool that can read a
project's files — **Databricks Genie / Assistant**, **Claude Code**, or other
AI assistants. It is **not** specific to any one tool; the install steps below just
show where the two most common ones expect it. Even without an agent, the files are a
readable, worked migration guide you can follow by hand.

It turns Matillion's two pipeline file types into their Databricks equivalents:

| Matillion pipeline | Databricks target |
|---|---|
| `*.orch.yaml` — orchestration pipeline (control flow) | **Databricks Job** (Workflow) — always the shell |
| `*.tran.yaml` — transformation pipeline (dataflow) | a **task in that Job** — SQL task (default), notebook, or a Lakeflow pipeline only when incremental/streaming or managed data-quality is needed |

The skill carries per-component references (joins, aggregates, SQL executors,
nested orchestrations, variables, …), a mapping cheatsheet, a decision guide for
picking each task's executor (SQL task → notebook → Lakeflow), and a bank of
hard-won gotchas.

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
examples/demo/               ← a complete before/after worked example
  ├─ matillion/              ← BEFORE: the original Matillion pipelines (.yaml)
  └─ databricks/             ← AFTER: the converted DAB (a Job with SQL + notebook tasks; no Lakeflow pipeline needed)
README.md                    ← this file
```

See `examples/demo/README.md` for the full before/after mapping.

**Required for the skill to work:** `SKILL.md` + the `references/` folder.
The `examples/demo/` before/after walkthrough is helpful (and referenced by the docs) — keep it.
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

Databricks' assistant has no dedicated "skills" folder — it reads the files in your
**Workspace**. So "installing" the skill just means placing its folder in the
Workspace and pointing the assistant at it:

1. In the Databricks UI, go to **Workspace** and pick (or create) a folder to work in,
   e.g. `/Workspace/Users/<you>/matillion-migration/`.
2. Put the **skill** there — upload the whole folder so you have
   `matillion-migration/skill/SKILL.md` and `matillion-migration/skill/references/…`.
   You can drag-and-drop in the UI (**Import** → *File/Folder*), clone it from a Git
   folder, or upload it with the Databricks CLI. **From the skill folder** (the one
   containing `SKILL.md`):

   ```bash
   # Uploads the folder recursively into your home Workspace directory.
   # $ME resolves to your workspace username (e.g. you@company.com).
   ME=$(databricks current-user me -o json | jq -r .userName)
   databricks workspace import-dir . \
     "/Workspace/Users/$ME/matillion-migration/skill" \
     --overwrite
   ```

   Add `-p <profile>` to any command if you use a named CLI profile. The `.md` files
   import as plain workspace files (only notebook extensions get stripped). If you run
   it from a full repo clone rather than a trimmed copy, build/scratch dirs
   (`.git/`, `.databricks/`, …) upload too — harmless, but a clean copy keeps the
   Workspace tidy.
3. Put your **Matillion project files** in a sibling subfolder — either drag them in, or
   upload your local export the same way:

   ```bash
   # Run from your local Matillion export (the folder with *.orch.yaml / *.tran.yaml)
   databricks workspace import-dir . \
     "/Workspace/Users/$ME/matillion-migration/source" \
     --overwrite
   ```

   See [How to run a conversion in Genie](#how-to-run-a-conversion-in-genie) below.

The assistant can now read `SKILL.md` and the references as context when you reference
that path in your prompt. No skills registry required.

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

The assistant works from files in your Workspace, so the flow is: **place the files,
then prompt.**

1. **Place your Matillion project in the Workspace.** Create a folder for the source
   pipelines next to the skill (from the install step), and upload every `*.orch.yaml`
   and `*.tran.yaml` you want to migrate — keep the original folder structure so nested
   `run-orchestration` / `run-transformation` references still resolve. For example:

   ```
   /Workspace/Users/<you>/matillion-migration/
     ├─ skill/                      ← SKILL.md + references/ (this repo)
     ├─ source/                     ← your Matillion export goes here
     │    ├─ daily_load.orch.yaml
     │    └─ transforms/…​.tran.yaml
     └─ output/                     ← (created by the conversion)
   ```

2. **Open the assistant** (the Assistant panel in a notebook, or Genie) and give it a
   prompt that (a) points at the skill, (b) points at your source folder, and (c) says
   where to write the result. A prompt you can copy and edit:

   > **Use the migration skill in `/Workspace/Users/<you>/matillion-migration/skill/`
   > (read `SKILL.md` and the `references/` docs, and follow that workflow) to migrate
   > the Matillion pipelines in `/Workspace/Users/<you>/matillion-migration/source/`
   > to Databricks. Write the resulting Databricks Asset Bundle (a Job per
   > orchestration pipeline, with SQL/notebook tasks — and a Lakeflow pipeline only
   > where one is actually needed) into
   > `/Workspace/Users/<you>/matillion-migration/output/`. Before writing any code,
   > follow the skill's decision ladder and gotchas, and ask me for the real Unity
   > Catalog `catalog.schema` to replace any Matillion `[Environment Default]`
   > placeholders.**

3. **Answer the placeholder question.** The skill will ask for a real Unity Catalog
   namespace (Matillion `[Environment Default]` has no Databricks equivalent) — give it
   `catalog.schema` you have write access to.

4. **Deploy & validate.** Once the bundle is generated, ask it to deploy: *"deploy this
   bundle to my workspace and run the validation checklist."* On Databricks it uses the
   CLI/bundle tooling; you need a Unity Catalog workspace and permission to create the
   Job (and pipeline, if one was emitted). See `references/deploy-and-validate.md`.

> **Tip:** if the assistant doesn't seem to be reading the skill, paste the contents of
> `SKILL.md` directly into the conversation (or open it in the editor first) so it's in
> context, then repeat the prompt.

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
output to the converted code already in `examples/demo/databricks/`:

> **"Using the matillion-to-databricks skill, convert the pipelines in
> `examples/demo/matillion/`."**

---

## What you get out

- A **Databricks Asset Bundle** (`databricks.yml`) with a **Job** per orchestration
  pipeline; each transformation becomes a task in that Job — a **SQL task** by default,
  a **notebook** where imperative logic is involved, or a **Lakeflow pipeline** only
  when one is actually warranted.
- The generated **SQL / Python** source for each task (SQL by default; Python
  only where a component needs it).
- Matillion **variables** mapped to bundle variables / Job parameters / task values.
- Deployment via the Databricks CLI and a **validation checklist** (tables exist,
  row counts sane, an aggregate spot-check).

---

## Tips & limitations

- **Read the decision guide.** `SKILL.md` → *"The two decisions of every migration"*
  explains the two calls that most affect the result: (1) the orchestration always
  becomes the **Job** (control flow — conditions, loops, failure branching, side
  effects — can only live there); (2) each transformation task picks an executor via
  the ladder **SQL task → notebook → Lakeflow** (Lakeflow only for incremental/
  streaming or managed data-quality/lineage, not by default).
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
