#!/usr/bin/env python3
"""Sanity-check the copyable code in this skill so non-runnable examples can't ship.

This is a documentation skill: its YAML and notebook snippets get copied verbatim
into real bundles, so a broken example is a real bug. This check does NOT need a
Databricks workspace or credentials — it only parses/compiles/lints locally:

  1. Every *.yml / *.yaml parses as valid YAML.
  2. Every notebook source under src/ compiles as Python (py_compile).
  3. Every notebook source is valid *Databricks notebook-source* format:
       - first line is `# Databricks notebook source`
       - no bare `%magic` lines (must be `# MAGIC %magic`)

Run locally with:  python3 scripts/check_examples.py
"""
from __future__ import annotations

import py_compile
import pathlib
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
SKIP = (".databricks/", ".git/")


def _skip(path: pathlib.Path) -> bool:
    return any(part in str(path) for part in SKIP)


def main() -> int:
    errors: list[str] = []

    # 1) all YAML parses
    for f in sorted([*ROOT.rglob("*.yml"), *ROOT.rglob("*.yaml")]):
        if _skip(f):
            continue
        try:
            list(yaml.safe_load_all(f.read_text()))
        except yaml.YAMLError as e:
            errors.append(f"YAML parse error in {f.relative_to(ROOT)}: {e}")

    # 2) + 3) notebook sources under src/: compile + notebook-source lint
    for f in sorted(ROOT.rglob("src/**/*.py")):
        if _skip(f):
            continue
        rel = f.relative_to(ROOT)
        try:
            py_compile.compile(str(f), doraise=True)
        except py_compile.PyCompileError as e:
            errors.append(f"py_compile error in {rel}: {e}")

        lines = f.read_text().splitlines()
        if not lines or lines[0].strip() != "# Databricks notebook source":
            errors.append(f"{rel}: missing '# Databricks notebook source' first line")
        for i, ln in enumerate(lines, 1):
            s = ln.strip()
            if s.startswith("%") and not s.startswith("%%"):
                errors.append(f"{rel}:{i}: bare magic '{s}' must be a '# MAGIC {s}' line")

    if errors:
        print("FAILED — example checks found issues:")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("OK — all example checks pass")
    return 0


if __name__ == "__main__":
    sys.exit(main())
