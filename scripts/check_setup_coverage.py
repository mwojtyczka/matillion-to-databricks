#!/usr/bin/env python3
"""Verify the synthetic-data setup notebook generates every column the transforms read.

The dominant setup-notebook failure in a real migration is an *incomplete* source
table: a transform does `SELECT `COL_X`, … FROM `SOURCE_TABLE`` but the setup notebook
never generates `COL_X`, so the first job run dies with `UNRESOLVED_COLUMN` — and you
find them one painful column at a time across many runs. A real project reads 100+
columns across many source tables, so this is not something to eyeball.

This script makes the check mechanical and complete. It:
  1. Finds the source tables the setup notebook creates (`.saveAsTable(...SOURCE...)`
     or `table_name = f"{catalog}.{schema}.SOURCE"`).
  2. Parses every transform read-node — `SELECT `a`,`b`,… FROM <source_table>` — across
     all `src/**/*.sql` and `src/**/*.py`, where the source table is referenced either
     as a backticked name (``SOURCE``) or via `IDENTIFIER(:cat || '.' || :sch || '.SOURCE')`.
  3. Diffs the read columns against the columns each setup block generates
     (`.withColumn("X", …)` and `… AS X`/`… AS `X`` inside a `spark.sql`/`SELECT`).
  4. Prints missing columns per table and exits non-zero if any are missing.

It is deliberately a *static* check — no Spark, no workspace, no credentials. It cannot
prove the generated *types/values* are right (see the date-dimension and coded-column
guidance in references/verification-checklist.md), only that every needed column exists.

Usage:
    python3 scripts/check_setup_coverage.py <bundle-dir>
    python3 scripts/check_setup_coverage.py <bundle-dir> --setup src/setup/00_generate_source_data.py

Exit code 0 = every read column is generated; 1 = missing columns (printed).
"""
from __future__ import annotations

import argparse
import glob
import os
import re
import sys

# A source-table identifier (either UPPER_SNAKE classic exports or lower_snake).
_TBL = r"[A-Za-z][A-Za-z0-9_]*"
_COL = r"[0-9A-Za-z_-]+"

# SQL type keywords that can follow `AS` in a CAST — excluded from unquoted `AS COL`
# alias capture so a `CAST(x AS STRING)` doesn't get mistaken for a generated column.
_SQL_TYPES = {
    "STRING", "VARCHAR", "CHAR", "TEXT", "BINARY", "BOOLEAN",
    "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "LONG",
    "FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC",
    "DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "INTERVAL",
    "ARRAY", "MAP", "STRUCT", "VARIANT", "OBJECT",
}


def _source_tables(setup_text: str) -> set[str]:
    """Tables the setup notebook creates."""
    tabs: set[str] = set()
    # table_name = f"{catalog}.{schema}.SOURCE"   (any catalog/schema var)
    tabs |= set(re.findall(r'f"\{[a-z_]+\}\.\{[a-z_]+\}\.(' + _TBL + r')"', setup_text))
    # .saveAsTable(f"{catalog}.{schema}.SOURCE")
    tabs |= set(re.findall(r'saveAsTable\(f"\{[a-z_]+\}\.\{[a-z_]+\}\.(' + _TBL + r')"', setup_text))
    return tabs


def _cols_from_blob(cols_blob: str) -> list[str]:
    """Extract column names from a read node's SELECT list.

    Handles backticked (``COL``, ``r.`COL```) and bare (`COL`, `r.COL`) forms, stripping
    any leading `alias.` qualifier so `SELECT r.STATUS, r.FROM_DATE FROM src r` still
    yields `[STATUS, FROM_DATE]` (not silently zero — that silent skip would defeat the
    whole check).
    """
    cols: list[str] = []
    # backticked, optionally alias-qualified: `COL` or r.`COL`
    for c in re.findall(r"(?:[A-Za-z_][A-Za-z0-9_]*\.)?`(" + _COL + r")`", cols_blob):
        if c not in cols:
            cols.append(c)
    if cols:
        return cols
    # bare, comma-separated identifiers: COL or alias.COL
    for tok in cols_blob.split(","):
        tok = tok.strip()
        tok = re.sub(r"^[A-Za-z_][A-Za-z0-9_]*\.", "", tok)  # drop leading alias.
        if re.fullmatch(_COL, tok):
            cols.append(tok)
    return cols


def _read_columns(files: list[str], source_tables: set[str]) -> dict[str, list[str]]:
    """Columns read from each source table across all transform files.

    Matches the read node `SELECT `a`, `b`, … FROM <source>` where <source> is either
    a backticked name, a bare name (with optional alias), or an
    IDENTIFIER(:cat || '.' || :sch || '.NAME') expression.
    """
    read: dict[str, list[str]] = {}
    # SELECT <cols> FROM <source>  — source referenced as IDENTIFIER(...'.NAME'),
    # `NAME`, or a bare NAME; columns may be backticked or bare, alias-qualified or not.
    from_clause = (
        r"FROM\s+(?:"
        r"IDENTIFIER\([^)]*\.(" + _TBL + r")'\)"          # IDENTIFIER(... '.NAME')
        r"|`(" + _TBL + r")`"                              # `NAME`
        r"|(" + _TBL + r")\b"                              # bare NAME
        r")"
    )
    sel_re = re.compile(r"SELECT\s+(.+?)\s+" + from_clause, re.IGNORECASE | re.DOTALL)
    for f in files:
        text = _read(f)
        for m in sel_re.finditer(text):
            cols_blob = m.group(1)
            tbl = m.group(2) or m.group(3) or m.group(4)
            if tbl not in source_tables:
                continue
            # only trust an explicit column list (not `SELECT *` / expressions);
            # a read node enumerates its columns.
            if "*" in cols_blob or "(" in cols_blob:
                continue
            cols = _cols_from_blob(cols_blob)
            if not cols:
                # matched a read of a source table but couldn't extract columns — surface
                # it rather than silently treating the table as fully covered.
                print(
                    f"WARNING: matched `FROM {tbl}` in {os.path.relpath(f)} but extracted "
                    f"0 columns from its SELECT list — coverage for {tbl} may be understated.",
                    file=sys.stderr,
                )
                continue
            read.setdefault(tbl, [])
            for c in cols:
                if c not in read[tbl]:
                    read[tbl].append(c)
    return read


def _generated_columns(setup_text: str, source_tables: set[str]) -> dict[str, set[str]]:
    """Columns each setup block generates, keyed by the block's target source table.

    A block is delimited by a `table_name = f"…{catalog}.{schema}.SOURCE"` assignment
    (or a `saveAsTable(f"…SOURCE")`) and runs until the next such marker. Within a block
    we collect both `.withColumn("X", …)` (dbldatagen) and `AS X` / `` AS `X` `` (raw
    spark.sql SELECT, e.g. a date dimension built with `spark.sql(...)`).
    """
    raw = [
        (m.start(), m.group(1))
        for m in re.finditer(
            r'(?:table_name\s*=\s*f"|saveAsTable\(f")\{[a-z_]+\}\.\{[a-z_]+\}\.(' + _TBL + r')"',
            setup_text,
        )
    ]
    # Collapse consecutive markers for the *same* table into one block boundary. A notebook
    # may both assign `table_name = f"…X"` and later `saveAsTable(f"…X")` for the same X;
    # without this the second marker would start a spurious empty block and split X's
    # `withColumn`s. Keeping only the first marker per run means each table's columns stay
    # attributed to it up to the *next distinct* table's marker.
    markers: list[tuple[int, str | None]] = []
    for pos, tbl in raw:
        if not markers or markers[-1][1] != tbl:
            markers.append((pos, tbl))
    markers.append((len(setup_text), None))
    gen: dict[str, set[str]] = {t: set() for t in source_tables}
    for i in range(len(markers) - 1):
        start, tbl = markers[i]
        end = markers[i + 1][0]
        if tbl is None or tbl not in source_tables:
            continue
        block = setup_text[start:end]
        cols = set(re.findall(r'withColumn\("(' + _COL + r')"', block))
        cols |= set(re.findall(r'\bAS\s+`(' + _COL + r')`', block))
        # unquoted `AS COL` (raw spark.sql SELECT, e.g. a date dimension) — but NOT the
        # `AS <TYPE>` of a CAST, which would spuriously add STRING/DATE/BIGINT/… to the
        # generated set and could mask a real read column that happens to share that name.
        cols |= {
            c
            for c in re.findall(r'\bAS\s+(' + _TBL + r')\b', block)
            if c.upper() not in _SQL_TYPES
        }
        gen[tbl] |= cols
    return gen


def _read(path: str) -> str:
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("bundle", help="bundle root directory")
    ap.add_argument(
        "--setup",
        default=None,
        help="setup notebook path relative to bundle (default: auto-detect under src/setup/)",
    )
    args = ap.parse_args()

    root = args.bundle
    if args.setup:
        setup_path = os.path.join(root, args.setup)
    else:
        cands = glob.glob(os.path.join(root, "src", "setup", "*.py"))
        if not cands:
            print(f"ERROR: no setup notebook found under {root}/src/setup/", file=sys.stderr)
            return 2
        setup_path = cands[0]

    if not os.path.exists(setup_path):
        print(f"ERROR: setup notebook not found: {setup_path}", file=sys.stderr)
        return 2

    setup_text = _read(setup_path)
    source_tables = _source_tables(setup_text)
    if not source_tables:
        print("ERROR: could not identify any source tables in the setup notebook.", file=sys.stderr)
        return 2

    transform_files = sorted(
        glob.glob(os.path.join(root, "src", "**", "*.sql"), recursive=True)
        + glob.glob(os.path.join(root, "src", "**", "*.py"), recursive=True)
    )
    transform_files = [f for f in transform_files if os.path.abspath(f) != os.path.abspath(setup_path)]

    read = _read_columns(transform_files, source_tables)
    gen = _generated_columns(setup_text, source_tables)

    missing: dict[str, list[str]] = {}
    for tbl, cols in read.items():
        miss = [c for c in cols if c not in gen.get(tbl, set())]
        if miss:
            missing[tbl] = miss

    print(f"Source tables: {len(source_tables)} | read from by transforms: {len(read)}")
    for tbl in sorted(read):
        status = "OK" if tbl not in missing else f"MISSING {len(missing[tbl])}"
        print(f"  {tbl:42} reads {len(read[tbl]):3}  generates {len(gen.get(tbl, set())):3}  [{status}]")

    if missing:
        print("\nMissing columns (read by a transform, not generated by setup):")
        for tbl in sorted(missing):
            print(f"  {tbl}: {missing[tbl]}")
        print("\nAdd these to the setup notebook (correct type/values per the transform's usage).")
        return 1

    print("\nAll columns read by transforms are generated by the setup notebook. ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
