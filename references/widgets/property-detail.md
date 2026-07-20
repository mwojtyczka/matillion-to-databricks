# Property Detail Panel Reference

## What it does
Shows all fields of the selected order row as a key-value list. Appears in the right column when a row is selected in the table.

## Correct pattern
- Header: small blue 14×14 square icon + "Object Details" title, `#f5f8fa` background
- Labels (`dt`): uppercase, 11px, `#5c7080` color
- Values (`dd`): normal weight, `#182026`
- Borders: `#d8e1e8`
- Show a placeholder message ("Select a row to view details") when nothing is selected

## Field order
Match the column order from the orders table: customer_name, item_name, status, assignee, days_until_due, consolidated_customer_id, quantity, order_due_date, unit_price, order_id.
