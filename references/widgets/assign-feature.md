# Assign Feature Reference

## What it does
Assign button selects an assignee (from existing assignees or a new name) and updates the order in the database. Status changes to `assigned`.

## Mistakes and fixes

### Assign button always disabled (cursor: not-allowed)
The base CSS style had `cursor: not-allowed` set unconditionally. Even when `disabled={false}`, the cursor never changed.

**Correct pattern:** base style uses `cursor: pointer`; only override to `not-allowed` when actually disabled:
```tsx
style={{
  ...styles.assignBtn,
  ...(hasSelectedOrder ? {} : { opacity: 0.5, cursor: 'not-allowed' }),
}}
disabled={!hasSelectedOrder}
```

### Assignee input was a plain text box
The first implementation used `<input type="text">` with no suggestions. The original Workshop used a combobox (type or pick from list).

**Use `<input list>` + `<datalist>`:**
```tsx
<input list="assignee-list" value={value} onChange={e => setValue(e.target.value)} />
<datalist id="assignee-list">
  {assigneeOptions.map(a => <option key={a} value={a} />)}
</datalist>
```
`assigneeOptions` comes from `GET /api/filters/options` — the same list used in the filter panel.

### Status stayed "open" after assigning
The PATCH endpoint only updated `assignee`, not `status`. The table still showed "open".

**Always update both in one statement:**
```python
run_query(f"UPDATE orders SET assignee = '{assignee}', status = 'assigned' WHERE order_id = '{oid}'")
```

### Table didn't update after a successful assign
The component re-fetched orders from the API after the PATCH. The Databricks SQL warehouse result cache returned the old rows.

**Update local state directly — never re-fetch after a write:**
```ts
setOrders(prev => prev.map(o =>
  o.order_id === selectedOrder.order_id ? { ...o, assignee, status: 'assigned' } : o
))
setSelectedOrder(prev => prev ? { ...prev, assignee, status: 'assigned' } : null)
```

## Layout note
The Assign button and its panel belong in the **right column alongside the filters**, not in a separate modal page or above the table.
