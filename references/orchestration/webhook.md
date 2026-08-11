# Matillion webhook / API step → notebook task

## What it does in Matillion

An outbound HTTP call — most often a **Teams / Slack / Power Automate notification** at the
end of a Job. In the classic JSON format its `implementationID` is `1006021671`; parameter
signature: `Name`, `Incoming Webhook URL`, `Payload Template`, `Payload`, `Payload Variables`.
The `Payload` is usually a JSON blob (a Microsoft **Adaptive Card** for Teams), often with
`${...}` variables interpolated in (job name, row counts, a failure flag).

## Databricks equivalent — a notebook task

There's no built-in "send webhook" task, so it becomes a **notebook task** that POSTs the
payload with `requests`:

- The **webhook URL is a credential** — it grants anyone who has it the ability to post to
  the channel. Put it in a **Databricks secret scope**, never inline or in a bundle
  variable (`references/secrets.md`), and read it at runtime.
- The **payload carries over almost unchanged** — keep the Adaptive Card / message JSON as
  is; only re-wire the `${...}` interpolations to task parameters / widgets / task values.
- Wire it into the Job graph where the Matillion step sat. For success/failure
  notifications this is usually the `run_if: ALL_SUCCESS` / `AT_LEAST_ONE_FAILED` pair from
  the failure-counting pattern (`references/mapping-cheatsheet.md` → "Failure-counting").

```python
# Databricks notebook source
# MAGIC %md Notification webhook — converted from a Matillion API/webhook step.

# COMMAND ----------
import json, requests

dbutils.widgets.text("job_name", "")
job_name = dbutils.widgets.get("job_name")

# URL is a secret, not inline (see references/secrets.md)
url = dbutils.secrets.get(scope="matillion_migration", key="teams_webhook_url")

# Adaptive Card payload carried over from the Matillion step; ${...} → widget values.
payload = {
    "type": "message",
    "attachments": [{
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {
            "type": "AdaptiveCard", "version": "1.4",
            "body": [{"type": "TextBlock", "text": f"{job_name} finished"}],
        },
    }],
}

resp = requests.post(url, json=payload, timeout=30)
resp.raise_for_status()
print("notification sent:", resp.status_code)
```

## Gotchas

- **The URL is a secret.** A leaked incoming-webhook URL lets anyone post to the channel —
  scope it, and rotate it if the Matillion export had it in plaintext.
- **`raise_for_status()`** so a failed POST fails the task (a silently-dropped notification
  is worse than a visibly-failed one).
- Add the library if needed — `requests` ships in Databricks Runtime, but a `# MAGIC %pip
  install requests` at the top makes a serverless/minimal environment safe.
- Don't fold the notification into an upstream task — keep it its own task so its
  `run_if` condition and success/failure are visible in the Job graph.
