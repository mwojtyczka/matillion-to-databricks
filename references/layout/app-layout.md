# App Layout Reference

## Structure
Two-column flex row:
- `<main>` — left/center: charts (pie + bar stacked vertically) + orders table below
- `<aside>` — right column (fixed 280px): filters, property detail, metric card, assign panel

```tsx
<div style={{ display: 'flex', flexDirection: 'row', height: '100vh' }}>
  <main style={{ flex: 1, padding: 16, minWidth: 0, display: 'flex', flexDirection: 'column', height: 800, overflow: 'hidden' }}>
    {/* charts + table */}
  </main>
  <aside style={{ width: 280, flexShrink: 0, background: '#ebf1f5', borderLeft: '1px solid #d8e1e8', padding: 12, display: 'flex', flexDirection: 'column', gap: 8, overflowY: 'auto' }}>
    {/* filters, detail, metric, assign */}
  </aside>
</div>
```

## Mistakes and fixes

### Attempted 2×2 grid — rejected
A 2×2 CSS grid (charts+filters top, table+detail bottom) was tried. User rejected it — the original Workshop uses a simple left-main / right-sidebar split, not a grid.

**Stick to the flex row layout above.**

### Assign panel placement
The assign button and input belong in the **right column (aside)**, not above the table or in a floating modal page.

## Blueprint color tokens
| Token | Value | Usage |
|-------|-------|-------|
| Page background | `#f5f8fa` | `<body>`, panel backgrounds |
| Border | `#d8e1e8` | All dividers and input borders |
| Sidebar bg | `#ebf1f5` | Right column `<aside>` |
| Primary text | `#182026` | Body text, values |
| Muted text | `#5c7080` | Labels, secondary text |
| Header bg | `#ffffff` | Top nav bar |

## Header
White background, dark title text, small blue 12×12 square icon. No heavy gradients or shadows.
