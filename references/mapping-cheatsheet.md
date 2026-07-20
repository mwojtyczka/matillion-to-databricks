# Matillion → Databricks mapping cheatsheet

## File types

| Matillion | Databricks | Detail |
|---|---|---|
| `*.orch.yaml` (orchestration) | Databricks **Job** | `transitions` → task deps |
| `*.tran.yaml` (transformation) | Lakeflow **Declarative Pipeline** | `sources` → dataset chain |

## Transformation components (dataflow)

| Matillion type | Databricks | Reference |
|---|---|---|
| `table-input` | source read (explicit projection) | `transformation/table-input.md` |
| `join` | SQL `JOIN` | `transformation/join.md` |
| `aggregate` | `GROUP BY` | `transformation/aggregate.md` |
| `rewrite-table-dl` | `CREATE OR REFRESH MATERIALIZED VIEW` | `transformation/rewrite-table.md` |

## Orchestration components (control flow)

| Matillion type | Databricks | Reference |
|---|---|---|
| `start` / `end-success` | Job graph boundaries (no task) | `orchestration/start-end.md` |
| `sql-executor` | Job SQL task | `orchestration/sql-executor.md` |
| `run-transformation` | Job pipeline task | `orchestration/run-transformation.md` |
| `python-script` | Job notebook/SQL task | `orchestration/python-script.md` |

## Default choices

- Transformation code: **SQL** (`CREATE OR REFRESH MATERIALIZED VIEW`); Python only when SQL can't express it.
- Full-overwrite (`rewrite-table-dl`) → materialized view. Append-only incremental → streaming table.
