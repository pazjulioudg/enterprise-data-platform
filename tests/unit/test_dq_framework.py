"""The same YAML rules must give the same answers on both engines (DQX and Great Expectations)."""
from unittest.mock import MagicMock

import pytest

from common.dq import dqx_engine, gx_engine
from common.dq.rules import load_gate

RULES = [
    {"name": "id_not_null", "type": "not_null", "column": "id", "severity": "error", "max_fail_pct": 0},
    {"name": "id_unique", "type": "unique", "columns": ["id"], "severity": "error", "max_fail_pct": 0},
    {"name": "status_valid", "type": "in_set", "column": "status", "values": ["A", "T"], "severity": "warn", "max_fail_pct": 0},
    {"name": "amount_range", "type": "between", "column": "amount", "min": 0, "max": 100, "severity": "error", "max_fail_pct": 0},
    {"name": "end_after_start", "type": "expression", "expression": "end_day >= start_day", "severity": "error", "max_fail_pct": 0},
    {"name": "current_unique", "type": "unique", "columns": ["id"], "where": "is_current", "severity": "error", "max_fail_pct": 0},
    {"name": "dept_fk", "type": "foreign_key", "column": "dept", "ref_table": "dept", "ref_column": "dept", "severity": "error", "max_fail_pct": 0},
]


def data(spark):
    df = spark.createDataFrame(
        [("1", "A", 10.0, 1, 2, True, "D1"), ("1", "T", 20.0, 1, 2, True, "D1"),     # duplicate id; 2 current
         (None, "A", 30.0, 1, 2, False, "D1"), ("3", "X", 500.0, 5, 2, False, "D9")],  # null id; bad status/amount/dates/fk
        "id string, status string, amount double, start_day int, end_day int, is_current boolean, dept string")
    return df, {"dept": spark.createDataFrame([("D1",)], "dept string")}


@pytest.mark.parametrize("engine", [dqx_engine, gx_engine])
def test_both_engines_count_the_same_failures(spark, engine):
    df, refs = data(spark)
    counts, total, quarantine = engine.run(spark, MagicMock(), "t", df, RULES, refs)
    assert total == 4
    assert counts["id_not_null"] == 1
    assert counts["status_valid"] == 1 and counts["amount_range"] == 1 and counts["end_after_start"] == 1
    assert counts["current_unique"] == 2 and counts["dept_fk"] == 1
    assert quarantine.count() >= 2          # rows with ERROR failures are quarantined with reasons


def test_rule_files_are_valid():
    for gate in ("hr_gold", "orm_gold"):
        spec = load_gate(gate)
        assert spec["tables"]
