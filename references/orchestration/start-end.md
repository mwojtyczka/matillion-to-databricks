# Matillion `start` / `end-success` → Job graph boundaries

## What they do in Matillion

- `start` — the single entry component; its `transitions` name the first real step.
- `end-success` — a terminal marker reached on success.

## Databricks equivalent

No task of their own. They define the Job's boundaries:
- `start`'s transition target is the Job's first task (no `depends_on`).
- `end-success` is implicit — the Job succeeds when all tasks succeed.

## Worked example (from create-maia-demo-data.orch.yaml)

`Start` → (`unconditional`) → `Dimension Tables`. So `Dimension Tables` is the root Job task. `End Success` follows `Create Aggregation Table` and needs no task.

## Gotchas

- Do not emit a Databricks task for `start`/`end-success`.
- If an orchestration has multiple terminal branches, all must complete for the Job to be green.
