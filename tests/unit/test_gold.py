"""Unit tests for the HR Gold transforms and data-quality gate (run locally and in CI)."""
import datetime as dt
from decimal import Decimal

import pytest
from unittest.mock import MagicMock

import build_gold as g
from common.dq.runner import gate_passed, run_gate

D = dt.date


def job_history(spark, rows):
    schema = ("emplid string, effdt date, dept_id string, job_code string, job_title string, "
              "location string, empl_status string, annual_rt decimal(18,2), currency_cd string, "
              "manager_id string, __START_AT bigint, __END_AT bigint")
    return spark.createDataFrame(rows, schema)


def personal(spark, rows):
    schema = ("emplid string, first_name string, last_name string, birth_date date, "
              "country_code string, national_id_type string, national_id string, hire_date date")
    return spark.createDataFrame(rows, schema)


def sample(spark):
    jh = job_history(spark, [
        ("E1", D(2020, 1, 1), "D001", "J100", "Analyst", "London", "A", Decimal("50000"), "GBP", None, 10, 20),
        ("E1", D(2026, 1, 2), "D002", "J110", "Senior Analyst", "London", "A", Decimal("60000"), "GBP", None, 20, None),
        ("E2", D(2019, 5, 1), "D001", "J200", "Engineer", "New York", "A", Decimal("90000"), "USD", None, 11, None),
    ])
    pe = personal(spark, [
        ("E1", "Olivia", "Smith", D(1990, 4, 2), "GB", "NINO", "QQ123456C", D(2020, 1, 1)),
        # E2 erased (GDPR) -> no personal row
    ])
    return jh, pe


def test_dim_employee_validity_windows(spark):
    jh, pe = sample(spark)
    dim = {(r.emplid, r.valid_from): r for r in g.build_dim_employee(jh, pe).collect()}
    old, new = dim[("E1", D(2020, 1, 1))], dim[("E1", D(2026, 1, 2))]
    assert old.valid_to == D(2026, 1, 1) and old.is_current is False
    assert new.valid_to == D(9999, 12, 31) and new.is_current is True
    assert old.employee_sk != new.employee_sk


def test_dim_employee_erased_person_has_no_pii(spark):
    jh, pe = sample(spark)
    e2 = [r for r in g.build_dim_employee(jh, pe).collect() if r.emplid == "E2"][0]
    assert e2.national_id is None and e2.first_name is None and e2.is_current is True


def test_point_in_time_join_picks_version_valid_on_fact_date(spark):
    jh, pe = sample(spark)
    dim = g.build_dim_employee(jh, pe)
    absence = spark.createDataFrame(
        [("A1", "E1", "SICK", D(2025, 6, 1), 2), ("A2", "E1", "SICK", D(2026, 1, 3), 1),
         ("A3", "E9", "SICK", D(2026, 1, 3), 1)],
        "absence_id string, emplid string, absence_type string, start_dt date, days int")
    out = {r.absence_id: r for r in g.build_fact_absence(absence, dim).collect()}
    assert out["A1"].dept_id == "D001" and out["A2"].dept_id == "D002"
    assert out["A3"].employee_sk is None          # orphan: unknown employee
    assert len(out) == 3                          # no row multiplication


def _frames(spark, dept_rows, absence_rows):
    jh, pe = sample(spark)
    dim = g.build_dim_employee(jh, pe)
    dept = g.build_dim_department(spark.createDataFrame(dept_rows, "dept_id string, dept_name string, cost_centre string"))
    absence = spark.createDataFrame(absence_rows, "absence_id string, emplid string, absence_type string, start_dt date, days int")
    pulse = spark.createDataFrame([], "response_id string, emplid string, survey_date date, engagement_score int, sentiment_score double")
    return {"dim_department": dept, "dim_employee": dim, "fact_absence": g.build_fact_absence(absence, dim),
            "fact_engagement": g.build_fact_engagement(pulse, dim)}


@pytest.mark.parametrize("engine", ["dqx", "gx"])
def test_quality_gate_passes_on_clean_data(spark, engine):
    frames = _frames(spark, [("D001", "Ops", "CC1"), ("D002", "Fin", "CC2")], [("A1", "E1", "SICK", D(2025, 6, 1), 2)])
    res = run_gate(spark, "hr_gold", engine, frames, "x", "t", workspace_client=MagicMock(), write=False)
    assert gate_passed(res), [r for r in res if not r["passed"]]


@pytest.mark.parametrize("engine", ["dqx", "gx"])
def test_quality_gate_blocks_referential_failure_and_tolerates_warnings(spark, engine):
    frames = _frames(spark, [("D001", "Ops", "CC1")], [("A9", "E9", "SICK", D(2026, 1, 3), 1)])
    res = {r["check_name"]: r for r in run_gate(spark, "hr_gold", engine, frames, "x", "t",
                                                 workspace_client=MagicMock(), write=False)}
    assert not res["dept_exists"]["passed"] and res["dept_exists"]["failed_count"] == 1   # D002 missing
    assert not res["absence_has_employee_version"]["passed"]                             # 1 of 1 orphan > 1%
    assert not gate_passed(list(res.values()))


def test_skewed_dimension_join_uses_broadcast(spark):
    """Joins on the skewed dept key must not shuffle the fact side."""
    from pyspark.sql import functions as F
    facts = spark.range(0, 1000).withColumn("dept_id", F.when(F.col("id") % 10 < 5, "D001").otherwise("D002"))
    dept = spark.createDataFrame([("D001", "Ops"), ("D002", "Fin")], "dept_id string, dept_name string")
    plan = facts.join(F.broadcast(dept), "dept_id")._jdf.queryExecution().executedPlan().toString()
    assert "BroadcastHashJoin" in plan
