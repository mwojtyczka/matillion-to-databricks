---
name: palantir-to-dbx
description: Guide for migrating a Palantir Foundry Workshop to a Databricks App. Trigger when user wants to migrate a Workshop, build a Databricks App from a HAR/Stemma source, or implement any of the Workshop components (filters, charts, table, detail panel, assign). Contains hard-won lessons from a real migration — consult before implementing each component.
---

# Palantir → Databricks Migration Guide

A practical guide built from a real migration. The primary purpose is to prevent repeating known mistakes. After identifying what to build, consult the relevant component reference before writing any code.

---

## Step 1 — Identify layout and components

Before writing any code, analyze the Workshop (from HAR or screenshots) and list:

1. **Layout**: how many columns, which panels are where
2. **Components present**: check each one below

**Layout**

| Topic | Reference |
|-------|-----------|
| Two-column flex structure, Blueprint colors, header | `references/layout/app-layout.md` |

**Widgets** — UI component behavior and known mistakes

| Component | Reference |
|-----------|-----------|
| Pie chart (status distribution) | `references/widgets/pie-chart.md` |
| Stacked bar chart (days × status) | `references/widgets/bar-chart.md` |
| Object/orders table | `references/widgets/orders-table.md` |
| Property detail panel | `references/widgets/property-detail.md` |
| Assign action (button + combobox) | `references/widgets/assign-feature.md` |
| Metric card (custom function result) | `references/widgets/metric-card.md` |

**Data wiring** — backend, SQL, and API connection mistakes

| Topic | Reference |
|-------|-----------|
| DB layer, SP permissions, result cache | `references/data/backend.md` |
| Filter params wired to SQL WHERE clause | `references/data/filter-panel.md` |

Write down the component list and layout before proceeding. Start with layout, then implement each component in the order listed.

---

## Custom Functions (Stemma Repositories) — STOP and ask before implementing

When analyzing the HAR, look for calls to:
- `/function-registry/api/functions/batch/resolve`
- `/function-executor/api/functions/{rid}/versions/{ver}/execute`

If found, **do not proceed** until you have the function source code. These are custom Python functions hosted in Palantir Stemma repositories. The HAR shows the inputs and one observed output, but not the full logic.

### What to extract from the HAR

From the `batch/resolve` response:
- `sourceProvenance.stemma.repositoryRid` — the Stemma repo RID
- `moduleName` and `functionName` — e.g. `python_functions.my_function` / `example_addition_function`

From the `execute` request/response:
- Parameter names and types (e.g. `a: integer, b: integer`)
- The observed return value (e.g. `"The sum of 900 and 400 is 1300."`)

### What to ask the user

> "The HAR contains a call to a custom Palantir function: `{functionName}` in module `{moduleName}` from Stemma repo `{repositoryRid}`.
>
> To migrate it accurately I need the source code. You can clone it from your Foundry instance:
> ```
> git clone https://<your-foundry-host>/stemma/api/git/{repositoryRid} _stemma_clone
> ```
> Please drop the cloned folder into the project directory as `_stemma_clone/`."

### Once you have the repo

1. Find the function file matching `moduleName` (e.g. `python_functions/my_function.py`)
2. Copy the function logic into a backend endpoint: `GET /api/function?a=<val>&b=<val>`
3. Add the MetricCard UI component to the right sidebar (see `references/widgets/metric-card.md`)

### If the repo is not available

Implement using the observed behavior from the HAR as a fallback, and leave a clear `# TODO: replace with actual function logic from {repositoryRid}` comment in the code.

---

## Step 2 — After each component, check its reference file

Each reference file documents:
- What the component should do
- Mistakes made during the real migration
- The correct implementation pattern

Read the reference **before** implementing, not after something breaks.

---

## Step 3 — Deploy and validate

Use the `fe-databricks-tools:databricks-apps` skill for all scaffolding, deployment, and app management. Invoke it instead of manually constructing `app.yaml` or deploy commands — it knows the correct conventions, auth patterns, and `databricks sync` + `databricks apps deploy` workflow.

Trigger it with: "use the databricks-apps skill to deploy this app"

The skill covers:
- `app.yaml` structure and env var injection
- Dual-mode auth (local CLI profile vs app service principal)
- Build → sync → deploy sequence
- Checking app status and logs after deploy

After deploy: check each component works end-to-end before calling done.

---

## Deployment gotchas (learned from real migration)

### SQL wait_timeout must be 0s or 5–50s
The Databricks SQL Statements API rejects `wait_timeout` values outside this range. Using `"60s"` returns `HTTP 400 INVALID_PARAMETER_VALUE`. Use `"50s"` as the maximum, and add a polling loop for statements that take longer:
```python
while data.get("status", {}).get("state") in ("RUNNING", "PENDING"):
    time.sleep(2)
    data = requests.get(f"{host}/api/2.0/sql/statements/{data['statement_id']}", ...).json()
```

### DATABRICKS_HOST in Databricks Apps has no https:// scheme
When running as a deployed Databricks App, `DATABRICKS_HOST` is set to just the hostname (e.g. `workspace.cloud.databricks.com`) with no scheme. Always normalise it before use:
```python
host = os.environ.get("DATABRICKS_HOST", "")
if host and not host.startswith("http"):
    host = f"https://{host}"
```
Without this, `requests` raises `MissingSchema: Invalid URL '...': No scheme supplied`.

### Use databricks-sdk for token in deployed app
Do NOT try to reach a local metadata service manually. Use the SDK which handles both local (CLI profile) and deployed (service principal) contexts:
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
auth_headers = w.config.authenticate()
token = auth_headers["Authorization"].replace("Bearer ", "")
```
Add `databricks-sdk>=0.20.0` to `requirements.txt`.

### Databricks Apps runs on port 8000 (not 8080)
Flask's default port is `5000`; many examples use `8080`. Databricks Apps expects the app to listen on **port 8000**. Always set `app.run(port=8000)` or pass `--port 8000` in the `app.yaml` command.

### Exclude large files from workspace sync
The `databricks sync` command uploads everything it finds. Files over 10 MB cause deployment to fail with `BAD_REQUEST: File size ... exceeded max size (10485760 bytes)`. Always exclude: `*.har`, `datasources/`, `.claude/`, `node_modules/`, `.venv/`, `__pycache__/`. If a large file was already uploaded in a prior sync, delete it from the workspace with `databricks workspace delete <path>` before redeploying.

### App SP needs USE CATALOG + USE SCHEMA + table privileges
Creating an app auto-creates a service principal (check `service_principal_client_id` in `databricks apps get`). That SP starts with no UC permissions. Grant all three layers before the app can query:
```sql
GRANT USE CATALOG ON CATALOG `<catalog>` TO `<sp-client-id>`;
GRANT USE SCHEMA ON SCHEMA `<catalog>`.`<schema>` TO `<sp-client-id>`;
GRANT SELECT, MODIFY ON TABLE `<catalog>`.`<schema>`.<table> TO `<sp-client-id>`;
```
Use the SP's `service_principal_client_id` (UUID format) as the grantee — not the display name or numeric ID, both of which fail with `PRINCIPAL_DOES_NOT_EXIST`.

### Data setup: load CSVs before first deploy
The app assumes the Delta table already exists. Run the data setup script locally before the first deploy:
```bash
DATABRICKS_HOST=https://... DATABRICKS_TOKEN=... DATABRICKS_WAREHOUSE_ID=... python setup_data.py
```
