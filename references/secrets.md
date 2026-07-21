# Matillion secrets → Databricks secrets

Matillion pipelines rarely hold credentials inline — they should **source** them: from
Matillion's own secret definitions / OAuth entries, from a cloud secret manager (AWS
Secrets Manager, Azure Key Vault, GCP Secret Manager), or from connection/profile
credentials (e.g. a Snowflake account password, an API token, a storage key). On
Databricks these all map to **Databricks secrets** — never to plaintext in code, bundle
variables, or job parameters.

**Rule: any value that was a secret in Matillion stays a secret in Databricks.** Migrate
it into a Databricks **secret scope**, and reference it at runtime — do not inline it and
do not turn it into a `${var.x}` bundle variable (bundle variables and job parameters are
**not** secret; their values are visible in the UI, run config, and logs).

## Step 1 — Find the secrets in the Matillion project

While inventorying (SKILL.md Step 1), flag every value that is a credential or is sourced
from a secret store. Common signals in the YAML / project:

| Where it shows up | What it is |
|---|---|
| Connection / profile passwords, account keys, tokens | DB or service credentials (Snowflake, JDBC, S3, …) |
| OAuth entries / Matillion "Secret Definitions" | tokens/client secrets managed by Matillion |
| `${secret_name}` or a param bound to a secret manager | a cloud-secret-manager lookup (AWS SM / Key Vault / GCP SM) |
| API keys in `python-script`, headers, or component params | service credentials |
| Storage account keys / SAS tokens / access keys | cloud storage credentials |

Write down, for each: a logical **name**, where it's used (which task), and its current
source (Matillion secret, which cloud manager, or a connection). Do **not** copy the
secret values into the migration notes or any file.

## Step 2 — Create Databricks secret scopes

Group secrets into a **secret scope** (a namespace), typically one per project/environment.

```bash
# Create a scope
databricks secrets create-scope matillion_migration

# Put each secret in it (value is read from a prompt / file / env — never hardcoded in scripts)
databricks secrets put-secret matillion_migration snowflake_password
databricks secrets put-secret matillion_migration api_token
```

- Prefer a **Databricks-backed** scope. An **Azure Key Vault-backed** scope is a good
  option on Azure if the secrets already live in Key Vault — the scope reads through to
  the vault, so you keep one source of truth.
- Use a consistent naming convention, e.g. `<system>_<purpose>` (`snowflake_password`,
  `s3_access_key`), so references are self-documenting.

> **Migrating from a cloud secret manager?** You don't have to move the value by hand —
> two options: (a) re-enter it into a Databricks-backed scope, or (b) on Azure, point an
> Azure Key Vault-backed scope at the existing vault. On AWS/GCP, either re-enter into a
> Databricks-backed scope, or (for compute that has an instance profile / service
> account) read from the manager's SDK at runtime. Prefer secret scopes for portability.

## Step 3 — Reference secrets at runtime (never inline)

How you read a secret depends on the executor (per the skill's ladder):

**Notebook / Python task** — `dbutils.secrets.get`:
```python
password = dbutils.secrets.get(scope="matillion_migration", key="snowflake_password")
# use it to build a connection; it is redacted in notebook output and logs
```

**SQL (federation / external connections)** — put the secret in a **Unity Catalog
connection**, which stores credentials securely, instead of embedding them in SQL:
```sql
CREATE CONNECTION snowflake_conn TYPE snowflake
OPTIONS (host '...', port '443', user 'svc', password secret('matillion_migration','snowflake_password'));
```

**Job / pipeline config** — reference the secret with the
`{{secrets/<scope>/<key>}}` syntax in a task's env vars or Spark conf, so the value is
injected at run time and redacted in logs:
```yaml
# in a job task (databricks.yml)
spark_env_vars:
  SNOWFLAKE_PASSWORD: "{{secrets/matillion_migration/snowflake_password}}"
```

**Lakeflow pipeline** — set the secret reference in the pipeline's `configuration`
(same `{{secrets/...}}` form) and read it via `spark.conf` in the pipeline code.

## Gotchas

- **Never** map a secret to a bundle variable (`${var.x}`) or job parameter — those are
  plaintext and appear in the UI, `bundle summary`, and run history. Secrets only go in
  secret scopes.
- **Never** write a secret into a `.sql` / `.py` / `.yml` source file, a comment, or the
  migration notes. If a Matillion export contains a plaintext credential, treat it as
  compromised — rotate it after migrating.
- **Grant read access**: the job/pipeline **run-as principal** needs `READ` on the secret
  scope, or tasks fail at runtime. Grant it as part of deploy (see
  `references/deploy-and-validate.md`).
- `dbutils.secrets.get` **redacts** the value in notebook output — don't defeat this by
  printing it, writing it to a table, or logging it.
- Match scope to blast radius: a **Workspace/Databricks-backed** scope is broadly
  useful; use per-team/per-environment scopes and ACLs when secrets shouldn't be shared.
