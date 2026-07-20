# Filter Panel Reference

## What it does
Sidebar with dropdowns for each filterable field (status, assignee, item_name, customer_name). Selecting a value re-fetches the orders table with that filter applied.

## Mistakes and fixes

### Only the first filter worked; the rest had no effect
The `_build_where()` helper on the backend was only wired for `status`. All other params were silently ignored.

**Wire every filter param explicitly:**
```python
def _build_where(status=None, assignee=None, item_name=None, customer_name=None):
    clauses = []
    if status:
        clauses.append(f"status = '{status.replace(chr(39), chr(39)*2)}'")
    if assignee:
        clauses.append(f"assignee = '{assignee.replace(chr(39), chr(39)*2)}'")
    if item_name:
        clauses.append(f"item_name = '{item_name.replace(chr(39), chr(39)*2)}'")
    if customer_name:
        clauses.append(f"customer_name = '{customer_name.replace(chr(39), chr(39)*2)}'")
    return ("WHERE " + " AND ".join(clauses)) if clauses else ""
```

## Correct pattern
- Dropdowns populated from `GET /api/filters/options` (distinct values per column)
- Use `<select>` elements; each `onChange` triggers a re-fetch with the new filter state
- Pass all active filters as query params on every request — do not track them independently
