# Pie Chart Reference

## What it does
Full pie chart showing order count by status. Matches the original Workshop's status distribution aggregate.

## Correct pattern
- No donut hole: remove `hole: 0.3` — use a full pie
- `textposition: 'inside'` so labels don't overflow
- Legend: `{ orientation: 'v', x: 1, y: 0.5 }` (vertical, right side)
- Right margin `r: 80` to give legend room
- Status colors: assigned `#215DB0`, closed `#C2255C`, open `#72B219`

## SQL
```sql
SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status
```
