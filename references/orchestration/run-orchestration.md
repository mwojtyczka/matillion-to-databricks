# Matillion `run-orchestration` → nested Databricks Job

## What it does in Matillion

Invokes **another orchestration pipeline** from within the current one — Matillion's sub-pipeline / "shared job" pattern for reuse and composition. Key parameter:

- `orchestrationJob` — the `.orch.yaml` filename to run.
- `setScalarVariables` / `setGridVariables` (if populated) — values passed into the callee.

This is the edge that stitches one Matillion orchestration pipeline to another (contrast `run-transformation`, which calls a *transformation* pipeline).

## Databricks equivalent

A **`run_job_task`** in the calling Databricks Job that triggers the Job built from the callee `.orch.yaml`. Its `depends_on` mirrors the incoming `transitions`; passed variables become the child Job's parameters.

```yaml
# in databricks.yml — calling job's tasks
- task_key: run_child_orchestration
  depends_on:
    - task_key: previous_step
  run_job_task:
    job_id: ${resources.jobs.child_orchestration.id}
    job_parameters:
      as_of_date: "{{job.parameters.as_of_date}}"   # a setScalarVariables value passed down
```

Alternative — **inline the child's tasks** into the parent Job instead of nesting, when:
- the child is only ever called from one place (no reuse benefit), or
- you want a single flat run graph for observability.

Prefer `run_job_task` when the child orchestration is a genuine shared/reusable pipeline called from multiple parents.

## Worked example

The provided samples do not use `run-orchestration` (the sole cross-pipeline call is `Run Transformation`, a `run-transformation`). If a customer project nests orchestrations, each `run-orchestration` step becomes a `run_job_task` pointing at the Job built from its `orchestrationJob` target, with `depends_on` matching the step's incoming transition.

## Gotchas

- Match `orchestrationJob` to the Job resource built from that exact `.orch.yaml` — build the callee Job first.
- Watch for **cycles / deep nesting**: a chain of `run-orchestration` calls becomes a chain of `run_job_task`s. Databricks caps nested-job depth; flatten (inline) very deep chains.
- Variables passed via `setScalarVariables` / `setGridVariables` must be declared as **parameters** on the child Job, or the values have nowhere to land. See `references/variables.md`.
- A shared job called from many parents = one Job resource referenced by many `run_job_task`s. Do not duplicate it per caller.
