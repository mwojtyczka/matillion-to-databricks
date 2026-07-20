# Bar Chart Reference

## What it does
Stacked bar chart: X axis = days_until_due, Y axis = count, series = status. Mirrors the original Workshop's "days until due × status" aggregate.

## Mistakes and fixes

### Bucketed ranges instead of individual day values
The first implementation grouped days into 5 buckets (0–15, 16–30, 31–45, 46–60, 60+). The original Workshop shows ~60 individual bars — one per day value.

**Use individual day values:**
```python
rows = run_query(f"""
    SELECT CAST(days_until_due AS INT) AS day_val, status, COUNT(*) AS cnt
    FROM orders {where}
    GROUP BY day_val, status
    ORDER BY day_val, status
""")
all_days = sorted({int(r["day_val"]) for r in rows})
statuses = sorted({r["status"] for r in rows})
data = {s: {d: 0 for d in all_days} for s in statuses}
for r in rows:
    data[r["status"]][int(r["day_val"])] = int(r["cnt"])
return {
    "days": [str(d) for d in all_days],
    "series": [{"name": s, "values": [data[s][d] for d in all_days]} for s in statuses],
}
```

## Correct pattern
- `tickangle: -45, automargin: true` on x-axis to prevent label overlap
- Status colors: assigned `#215DB0`, closed `#C2255C`, open `#72B219`
- Plotly `barmode: 'stack'`
