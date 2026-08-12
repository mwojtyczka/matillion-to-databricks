"""Unit tests for scripts/check_setup_coverage.py.

The coverage checker is a regex parser, and its correctness *is* the tool's value —
a parsing gap makes it report "all covered" while the migrated job still fails at
runtime with UNRESOLVED_COLUMN. These fixtures lock down the read-node forms the
parser must handle (backticked, alias-qualified, IDENTIFIER, bare) plus the missing-
column and date-dimension cases.

Run:  pytest        (or: python3 -m pytest)
"""
from __future__ import annotations

import importlib.util
import pathlib

import pytest

# Load scripts/check_setup_coverage.py as a module (it's a standalone script, not a package).
_SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "scripts" / "check_setup_coverage.py"
_spec = importlib.util.spec_from_file_location("check_setup_coverage", _SCRIPT)
csc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(csc)


# --- _cols_from_blob: the extraction that F1 fixed --------------------------------

@pytest.mark.parametrize(
    "blob, expected",
    [
        ("`FROM_DATE`, `STATUS`, `ID`", ["FROM_DATE", "STATUS", "ID"]),          # backticked
        ("o.`FROM_DATE`, o.`STATUS`", ["FROM_DATE", "STATUS"]),                  # alias + backtick (F1)
        ("FROM_DATE, STATUS, ID", ["FROM_DATE", "STATUS", "ID"]),                # bare
        ("r.FROM_DATE, r.STATUS", ["FROM_DATE", "STATUS"]),                      # alias + bare (F1)
        ("`445_YYYY-MM`, `445_QUARTER`", ["445_YYYY-MM", "445_QUARTER"]),        # digit/hyphen names
        ("`A`, `A`, `B`", ["A", "B"]),                                          # de-duped, order kept
    ],
)
def test_cols_from_blob(blob, expected):
    assert csc._cols_from_blob(blob) == expected


# --- _source_tables ---------------------------------------------------------------

def test_source_tables_both_markers():
    setup = (
        'table_name = f"{catalog}.{schema}.SRC_ORDERS"\n'
        'df.write.saveAsTable(f"{catalog_adl}.{schema_adl}.AD_ORG")\n'
    )
    assert csc._source_tables(setup) == {"SRC_ORDERS", "AD_ORG"}


def test_source_tables_bare_saveastable_and_create():
    # Bare unqualified writes (with a separate USE CATALOG/SCHEMA) must be detected too.
    setup = (
        'df.write.mode("overwrite").saveAsTable("prisma_t_uk_risk")\n'
        'spark.sql("CREATE TABLE IF NOT EXISTS cdm_titles (id STRING)")\n'
    )
    assert csc._source_tables(setup) == {"prisma_t_uk_risk", "cdm_titles"}


def test_generated_columns_bare_saveastable_with_schema_list():
    # createDataFrame(rows, [..]) / make_df(rows, [..]) + bare saveAsTable("X") at block END:
    # columns precede the marker and are declared as a quoted string list.
    setup = '''
risk_rows = [(1, None, "x")]
risk_df = make_df(risk_rows, ["id", "to_date", "region"], null_date_cols=["to_date"])
risk_df.write.mode("overwrite").saveAsTable("prisma_t_uk_risk")
'''
    gen = csc._generated_columns(setup, csc._source_tables(setup))
    assert gen["prisma_t_uk_risk"] == {"id", "to_date", "region"}


# --- end-to-end via a temp bundle -------------------------------------------------

def _write_bundle(tmp_path, setup_body: str, transform_sql: str):
    (tmp_path / "src" / "setup").mkdir(parents=True)
    (tmp_path / "src" / "sql").mkdir(parents=True)
    (tmp_path / "src" / "setup" / "00_generate_source_data.py").write_text(
        "# Databricks notebook source\nimport dbldatagen as dg\n" + setup_body
    )
    (tmp_path / "src" / "sql" / "t.sql").write_text(transform_sql)
    return tmp_path


_SETUP_ORDERS = """
table_name = f"{catalog}.{schema}.SRC_ORDERS"
if not spark.catalog.tableExists(table_name):
    df = (dg.DataGenerator(spark, name="orders", rows=10, seedColumnName="_seq")
        .withColumn("ID", "string")
        .withColumn("STATUS", "string")
        .build())
    df.write.saveAsTable(table_name)
"""


def test_missing_column_detected_via_alias_read(tmp_path):
    # Transform reads o.`FROM_DATE` (alias-qualified) which setup does NOT generate.
    sql = (
        "CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :schema || '.OUT') AS\n"
        "WITH x AS (\n"
        "  SELECT o.`ID`, o.`STATUS`, o.`FROM_DATE` "
        "FROM IDENTIFIER(:catalog || '.' || :schema || '.SRC_ORDERS') o\n"
        ")\nSELECT * FROM x;\n"
    )
    bundle = _write_bundle(tmp_path, _SETUP_ORDERS, sql)
    setup_text = (bundle / "src" / "setup" / "00_generate_source_data.py").read_text()
    tables = csc._source_tables(setup_text)
    files = [str(bundle / "src" / "sql" / "t.sql")]
    read = csc._read_columns(files, tables)
    gen = csc._generated_columns(setup_text, tables)
    assert read["SRC_ORDERS"] == ["ID", "STATUS", "FROM_DATE"]
    missing = [c for c in read["SRC_ORDERS"] if c not in gen["SRC_ORDERS"]]
    assert missing == ["FROM_DATE"]


def test_full_coverage_passes(tmp_path):
    setup = """
table_name = f"{catalog}.{schema}.SRC_ORDERS"
if not spark.catalog.tableExists(table_name):
    df = (dg.DataGenerator(spark, name="orders", rows=10, seedColumnName="_seq")
        .withColumn("ID", "string")
        .withColumn("STATUS", "string")
        .withColumn("FROM_DATE", "date")
        .build())
    df.write.saveAsTable(table_name)
"""
    sql = (
        "SELECT `ID`, `STATUS`, `FROM_DATE` "
        "FROM IDENTIFIER(:catalog || '.' || :schema || '.SRC_ORDERS')\n"
    )
    bundle = _write_bundle(tmp_path, setup, sql)
    setup_text = (bundle / "src" / "setup" / "00_generate_source_data.py").read_text()
    tables = csc._source_tables(setup_text)
    read = csc._read_columns([str(bundle / "src" / "sql" / "t.sql")], tables)
    gen = csc._generated_columns(setup_text, tables)
    assert all(c in gen["SRC_ORDERS"] for c in read["SRC_ORDERS"])


def test_generated_columns_from_raw_select_as(tmp_path):
    # Date-dimension style: columns created via unquoted `AS COL` in a spark.sql SELECT.
    setup = """
table_name = f"{catalog}.{schema}.SI_DIM_DATE"
if not spark.catalog.tableExists(table_name):
    df = spark.sql("SELECT d AS FULL_DATE, weekofyear(d) AS WEEK_NUM FROM base")
    df.write.saveAsTable(table_name)
"""
    tables = csc._source_tables(setup)
    gen = csc._generated_columns(setup, tables)
    assert {"FULL_DATE", "WEEK_NUM"} <= gen["SI_DIM_DATE"]


def test_cast_type_not_captured_as_generated_column():
    # `CAST(NULL AS STRING) AS BANK_HOLIDAY_NAME` must yield BANK_HOLIDAY_NAME, not STRING.
    setup = """
table_name = f"{catalog}.{schema}.SI_DIM_DATE"
if not spark.catalog.tableExists(table_name):
    df = spark.sql("SELECT d AS FULL_DATE, CAST(NULL AS STRING) AS BANK_HOLIDAY_NAME FROM base")
    df.write.saveAsTable(table_name)
"""
    gen = csc._generated_columns(setup, csc._source_tables(setup))["SI_DIM_DATE"]
    assert "BANK_HOLIDAY_NAME" in gen
    assert "STRING" not in gen  # CAST type must not masquerade as a generated column


def test_dual_marker_same_table_not_split():
    # A table that both assigns table_name AND saveAsTable(f"…SAME") must keep all its
    # columns attributed to it (the second marker must not start a spurious empty block).
    setup = """
table_name = f"{catalog}.{schema}.SRC"
if not spark.catalog.tableExists(table_name):
    df = (dg.DataGenerator(spark, name="s", rows=5, seedColumnName="_seq")
        .withColumn("A", "string")
        .withColumn("B", "string")
        .build())
    df.write.saveAsTable(f"{catalog}.{schema}.SRC")
"""
    gen = csc._generated_columns(setup, csc._source_tables(setup))
    assert gen["SRC"] == {"A", "B"}


def test_select_star_read_is_ignored(tmp_path):
    # `SELECT *` is not an enumerated read node; it must not register (empty) columns.
    sql = "SELECT * FROM IDENTIFIER(:catalog || '.' || :schema || '.SRC_ORDERS')\n"
    bundle = _write_bundle(tmp_path, _SETUP_ORDERS, sql)
    setup_text = (bundle / "src" / "setup" / "00_generate_source_data.py").read_text()
    tables = csc._source_tables(setup_text)
    read = csc._read_columns([str(bundle / "src" / "sql" / "t.sql")], tables)
    assert "SRC_ORDERS" not in read  # nothing enumerated → table absent, not falsely covered
