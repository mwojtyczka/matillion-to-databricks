# Orders Table Reference

## What it does
Main data table showing all orders with 10 columns. Clicking a row selects it and populates the detail panel.

## Columns (in order)
customer_name, item_name, status, assignee, days_until_due, consolidated_customer_id, quantity, order_due_date, unit_price, order_id

## Correct pattern
- Status: colored 8×8 square dot + plain text (no pill/badge)
  - assigned `#215DB0`, closed `#C2255C`, open `#72B219`
- Item name: small blue 10×10 square icon + text (matches Palantir object type icon)
- Table headers: uppercase, `#f5f8fa` background, `#d8e1e8` border
- Selected row: highlighted background
- `overflowX: 'auto'` on the wrapper to handle wide content

## Notes
- The table should not have a fixed maxHeight that cuts it off — let the parent container control scroll
- All 10 columns must be explicitly defined; do not rely on dynamic key enumeration from API response (order is not guaranteed)
