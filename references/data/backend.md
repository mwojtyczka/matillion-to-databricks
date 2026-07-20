# Backend Reference

## What it does
- `run_query(sql)` executes SQL against the Databricks SQL warehouse via REST API
- Routes: `GET /api/orders`, `GET /api/filters/options`, `GET /api/charts/*`, `PATCH /api/orders/{id}/assign`

## Mistakes and fixes

### SP missing MODIFY permission → PATCH returns 500
The service principal running the app has SELECT by default. Any UPDATE/INSERT/DELETE fails with `PERMISSION_DENIED`.

**Do this before implementing any write endpoint:**
```sql
GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.<table> TO `<sp-client-id>`;
```
Check app logs for `User does not have MODIFY on Table` to confirm this is the cause.

### Databricks SQL result cache returns stale data after DML
After a successful UPDATE, re-fetching via SELECT returns the old rows because the warehouse caches results.

**Never re-fetch from the API after a write.** Instead, update React local state directly:
```ts
setOrders(prev => prev.map(o =>
  o.order_id === updatedId ? { ...o, assignee, status: 'assigned' } : o
))
```

### SQL injection via string interpolation
All user-supplied values must be escaped before interpolating into SQL strings:
```python
safe = value.replace("'", "''")
```
Use this for every parameter that comes from a request body or query string.
