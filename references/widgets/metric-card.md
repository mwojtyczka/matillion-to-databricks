# Metric Card Reference

## What it does
Displays the string result of a custom Palantir function in a card in the right sidebar. The function takes integer inputs (wired from Workshop variables) and returns a formatted string.

## How to identify it in the HAR
Look for requests to:
- `/function-registry/api/functions/batch/resolve` — resolves the function RID to its spec
- `/function-executor/api/functions/{rid}/versions/{ver}/execute` — executes the function

The resolve response contains:
- `sourceProvenance.stemma.repositoryRid` — the Stemma repo containing the function code
- `moduleName` and `functionName` — the Python module and function to call
- Parameter names and types from the request body

**Always extract these before proceeding.**

## Correct pattern

### You need the repo
Do not guess the function logic from the HAR response alone. The observed output (e.g. `"The sum of 900 and 400 is 1300."`) tells you what it does in one case — not the full logic. Ask the user for the repository code:

> "The HAR shows a custom function `example_addition_function` from Stemma repo `ri.stemma.main.repository.<rid>`. Please provide the repository code so I can implement the function accurately."

### Backend endpoint
```python
@app.route("/api/function")
def run_function():
    a = int(request.args.get("a", 0))
    b = int(request.args.get("b", 0))
    # Paste the actual function logic from the repo here
    result = f"The sum of {a} and {b} is {a + b}."
    return jsonify({"result": result})
```

### Frontend component
Sits at the bottom of the right sidebar. Shows a labelled result value with a "Calculate" button that lets the user change inputs.

```tsx
function MetricCard() {
  const [a, setA] = React.useState(900);
  const [b, setB] = React.useState(400);
  const [result, setResult] = React.useState(null);
  const [loading, setLoading] = React.useState(false);

  async function calculate() {
    setLoading(true);
    const d = await apiFetch(`/api/function?a=${a}&b=${b}`);
    setResult(d.result);
    setLoading(false);
  }

  React.useEffect(() => { calculate(); }, []);

  return (
    <div style={{ border: '1px solid #d8e1e8', borderRadius: 3, padding: 10 }}>
      <div style={{ fontSize: 12, fontWeight: 700, marginBottom: 8 }}>Function Result</div>
      <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
        <input type="number" value={a} onChange={e => setA(Number(e.target.value))}
          style={{ width: 60, padding: '3px 6px', border: '1px solid #d8e1e8', borderRadius: 3 }} />
        <input type="number" value={b} onChange={e => setB(Number(e.target.value))}
          style={{ width: 60, padding: '3px 6px', border: '1px solid #d8e1e8', borderRadius: 3 }} />
        <button onClick={calculate} style={{ padding: '3px 8px', background: '#215DB0', color: 'white', border: 'none', borderRadius: 3, cursor: 'pointer' }}>
          Run
        </button>
      </div>
      <div style={{ fontSize: 13, color: '#182026' }}>
        {loading ? 'Running…' : (result ?? '—')}
      </div>
    </div>
  );
}
```

## Placement
Bottom of the right `<aside>` column, below the assign panel.
