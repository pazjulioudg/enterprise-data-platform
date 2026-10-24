"""Unit and end-to-end tests for the ORM Gold layer (local Spark)."""
import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from unittest.mock import MagicMock

import build_risk_gold as g
from common.dq.runner import gate_passed, run_gate
import generate_orm_sources as gen

D = dt.date
AS_AT = D(2026, 10, 31)
MOVE_SCHEMA = ("movement_id string, event_id string, movement_type string, amount decimal(18,2), "
               "currency string, accounting_date date")


def fx(spark):
    rows = []
    for d in [D(2024, 3, 1), D(2024, 6, 1), D(2025, 2, 1), D(2026, 1, 15)]:
        rows += [("EUR", d, 1.0, 0.85), ("GBP", d, 1.2, 1.0), ("USD", d, 0.9, 0.75)]
    return spark.createDataFrame(rows, "currency string, rate_date date, rate_to_eur double, rate_to_gbp double")


def ref(spark):
    et = spark.createDataFrame([("EDPM_TCEM", "EDPM"), ("EF_TF", "EF"), ("BDSF_SYS", "BDSF")],
                               "event_type_l2_code string, event_type_l1_code string")
    bl = spark.createDataFrame([("RB_RB", "RB"), ("CB_CB", "CB")], "business_line_l2_code string, business_line_l1_code string")
    return et, bl


def events(spark, rows):
    schema = ("event_id string, source_system string, legal_entity_id string, org_unit_id string, "
              "business_line_l2_code string, event_type_l2_code string, cause_code string, occurrence_date date, "
              "discovery_date date, first_accounting_date date, event_status string, boundary_type string, "
              "is_near_miss boolean, is_operational_gain boolean, is_rapidly_recovered boolean, "
              "parent_event_id string, inclusion_status string, confidentiality string, currency string, "
              "event_title string, event_description string")
    full = [(e, "GRC", "LE01", "OU101", bl, et, "PROC_EXEC", D(2024, 2, 1), D(2024, 2, 10), D(2024, 3, 1), "CLOSED",
             boundary, nm, False, False, parent, "INCLUDED", "STANDARD", "GBP", "t", "d")
            for e, bl, et, boundary, nm, parent in rows]
    return spark.createDataFrame(full, schema)


def test_provisions_releases_and_recoveries_net_correctly(spark):
    moves = spark.createDataFrame([
        ("m1", "E1", "PROVISION", Decimal("100"), "GBP", D(2024, 3, 1)),
        ("m2", "E1", "PROVISION_RELEASE", Decimal("100"), "GBP", D(2024, 6, 1)),
        ("m3", "E1", "GROSS_LOSS", Decimal("80"), "GBP", D(2024, 6, 1)),
        ("m4", "E1", "RECOVERY_OTHER", Decimal("30"), "GBP", D(2025, 2, 1)),
    ], MOVE_SCHEMA)
    et, bl = ref(spark)
    conv = g.convert_movements(moves, fx(spark), AS_AT)
    p = g.build_positions(events(spark, [("E1", "RB_RB", "EDPM_TCEM", "NONE", False, None)]), conv, et, bl, AS_AT).first()
    assert p.gross_loss_txn == Decimal("80.00") and p.recoveries_txn == Decimal("30.00") and p.net_loss_txn == Decimal("50.00")
    assert p.net_loss_eur == Decimal("60.00")      # GBP converted at 1.2 on each accounting date
    assert p.event_type_l1_code == "EDPM" and p.business_line_l1_code == "RB"


def test_position_respects_as_at_date(spark):
    moves = spark.createDataFrame([("m1", "E1", "GROSS_LOSS", Decimal("50"), "EUR", D(2024, 3, 1)),
                                   ("m2", "E1", "RECOVERY_OTHER", Decimal("50"), "EUR", D(2026, 1, 15))], MOVE_SCHEMA)
    et, bl = ref(spark)
    conv = g.convert_movements(moves, fx(spark), D(2025, 12, 31))
    p = g.build_positions(events(spark, [("E1", "RB_RB", "EDPM_TCEM", "NONE", False, None)]), conv, et, bl, D(2025, 12, 31)).first()
    assert p.net_loss_eur == Decimal("50.00")      # recovery booked after the as-at date is ignored


def test_regulatory_dataset_groups_root_events_and_excludes_boundary_and_near_miss(spark):
    moves = spark.createDataFrame([
        ("a", "P", "GROSS_LOSS", Decimal("15000"), "EUR", D(2024, 3, 1)),   # parent
        ("b", "C", "GROSS_LOSS", Decimal("15000"), "EUR", D(2025, 2, 1)),   # child of P: root total 30k
        ("c", "X", "GROSS_LOSS", Decimal("50000"), "EUR", D(2024, 3, 1)),   # credit boundary -> excluded
        ("d", "S", "GROSS_LOSS", Decimal("15000"), "EUR", D(2024, 3, 1)),   # standalone, below threshold
    ], MOVE_SCHEMA)
    ev = events(spark, [("P", "RB_RB", "BDSF_SYS", "NONE", False, None), ("C", "RB_RB", "BDSF_SYS", "NONE", False, "P"),
                        ("X", "CB_CB", "EF_TF", "CREDIT", False, None), ("S", "RB_RB", "EDPM_TCEM", "NONE", False, None),
                        ("N", "RB_RB", "EDPM_TCEM", "NONE", True, None)])
    et, bl = ref(spark)
    conv = g.convert_movements(moves, fx(spark), AS_AT)
    reg = {(r.threshold_eur, r.accounting_year): r for r in
           g.build_regulatory_dataset(g.build_positions(ev, conv, et, bl, AS_AT), conv, AS_AT).collect()}
    assert set(reg) == {(20000, 2024), (20000, 2025)}            # only the grouped root qualifies
    assert reg[(20000, 2024)].net_loss_eur == Decimal("15000.00")  # allocated by accounting year
    assert reg[(20000, 2025)].net_loss_eur == Decimal("15000.00")


def test_kri_rag_and_trend(spark):
    defs = spark.createDataFrame([("K1", "Failed trades", "R1", 20.0, 40.0, "HIGHER_IS_WORSE"),
                                  ("K2", "Test completion", "R1", 90.0, 80.0, "LOWER_IS_WORSE")],
                                 "kri_id string, name string, risk_id string, amber_threshold double, red_threshold double, direction string")
    obs = spark.createDataFrame([("K1", D(2026, 8, 31), 10.0), ("K1", D(2026, 9, 30), 45.0),
                                 ("K2", D(2026, 9, 30), 85.0)], "kri_id string, period_end date, value double")
    risks = spark.createDataFrame([("R1", "OU101")], "risk_id string, org_unit_id string")
    org = spark.createDataFrame([("OU101", "LE01")], "org_unit_id string, legal_entity_id string")
    out = {(r.kri_id, r.period_end): r for r in g.build_kri_dashboard(defs, obs, risks, org).collect()}
    assert out[("K1", D(2026, 8, 31))].rag_status == "GREEN" and out[("K1", D(2026, 8, 31))].trend == "NEW"
    assert out[("K1", D(2026, 9, 30))].rag_status == "RED" and out[("K1", D(2026, 9, 30))].trend == "WORSENING"
    assert out[("K2", D(2026, 9, 30))].rag_status == "AMBER"


def test_gl_reconciliation_statuses(spark):
    moves = spark.createDataFrame([("1", "E1", "GROSS_LOSS", Decimal("100"), "GBP", D(2024, 3, 1)),
                                   ("2", "E2", "GROSS_LOSS", Decimal("100"), "GBP", D(2024, 3, 1)),
                                   ("3", "E3", "GROSS_LOSS", Decimal("100"), "GBP", D(2024, 3, 1)),
                                   ("4", "E1", "RECOVERY_OTHER", Decimal("40"), "GBP", D(2024, 3, 20))], MOVE_SCHEMA)
    gl = spark.createDataFrame([(D(2024, 3, 2), Decimal("100"), "GBP", "E1"), (D(2024, 3, 21), Decimal("-40"), "GBP", "E1"),
                                (D(2024, 3, 1), Decimal("90"), "GBP", "E2"), (D(2024, 3, 1), Decimal("5"), "GBP", "E9")],
                               "posting_date date, amount decimal(18,2), currency string, event_ref string")
    ev = spark.createDataFrame([("E1", "LE01"), ("E2", "LE01"), ("E3", "LE01")], "event_id string, legal_entity_id string")
    st = {r.event_id: r.recon_status for r in g.build_recon(moves, gl, ev, 1.0, AS_AT).collect()}
    assert st == {"E1": "MATCHED", "E2": "AMOUNT_BREAK", "E3": "MISSING_IN_GL", "E9": "MISSING_IN_LOSS"}


def test_issue_ageing_buckets(spark):
    issues = spark.createDataFrame([("I1", "R1", "OPEN", D(2026, 10, 1)), ("I2", "R1", "OPEN", D(2026, 6, 1)),
                                    ("I3", "R1", "CLOSED", D(2026, 1, 1)), ("I4", "R1", "OPEN", D(2026, 12, 1))],
                                   "issue_id string, risk_id string, status string, due_date date")
    risks = spark.createDataFrame([("R1", "OU101")], "risk_id string, org_unit_id string")
    org = spark.createDataFrame([("OU101", "LE01")], "org_unit_id string, legal_entity_id string")
    b = {r.issue_id: (r.ageing_bucket, r.is_overdue) for r in g.build_issue_ageing(issues, risks, org, AS_AT).collect()}
    assert b == {"I1": ("1-30", True), "I2": ("90+", True), "I3": ("CLOSED", False), "I4": ("NOT_DUE", False)}


# ----------------------------------------------------------------------------- end to end on generated data
@pytest.fixture(scope="module")
def generated(tmp_path_factory, spark):
    root = str(tmp_path_factory.mktemp("orm"))
    gen.generate(root, 0)
    xw = {(s, d, sc): cc for s, d, sc, cc, _f, _t in gen.crosswalk_rows()}
    rd = lambda e: spark.read.json(f"{root}/grc/{e}/*/")  # noqa: E731
    m = F.create_map(*[x for (s, d, sc), cc in xw.items() if s == "GRC" for x in (F.lit(f"{d}|{sc}"), F.lit(cc))])
    look = lambda d, c: m[F.concat(F.lit(f"{d}|"), F.col(c))]  # noqa: E731
    ev = (rd("loss_events")
          .withColumn("event_type_l2_code", look("event_type", "event_type_code"))
          .withColumn("business_line_l2_code", look("business_line", "business_line_code"))
          .withColumn("source_system", F.lit("GRC"))
          .withColumn("cause_code", look("cause", "cause_code"))
          .withColumn("parent_event_id", F.col("parent_event_id").cast("string"))
          .withColumn("exclusion_reason", F.col("exclusion_reason").cast("string")))
    for c in ("occurrence_date", "discovery_date", "first_accounting_date"):
        ev = ev.withColumn(c, F.to_date(c))
    mv = (rd("loss_movements").withColumn("amount", F.col("amount").cast("decimal(18,2)"))
          .withColumn("accounting_date", F.to_date("accounting_date")))
    fxdf = spark.createDataFrame(gen.fx_rows(), "currency string, rate_date string, rate_to_eur double, rate_to_gbp double") \
                .withColumn("rate_date", F.to_date("rate_date"))
    et = spark.createDataFrame([(e[0], e[1]) for e in gen.EVENT_TYPES], "event_type_l2_code string, event_type_l1_code string")
    bl = spark.createDataFrame([(b[0], b[1]) for b in gen.BUSINESS_LINES], "business_line_l2_code string, business_line_l1_code string")
    gl = (spark.read.option("header", "true").csv(f"{root}/gl/postings/*/")
          .withColumn("posting_date", F.to_date("posting_date")).withColumn("amount", F.col("amount").cast("decimal(18,2)"))
          .withColumn("event_ref", F.regexp_extract("narrative", r"OPL (\S+)", 1)))
    return ev, mv, fxdf, et, bl, gl


@pytest.mark.parametrize("engine", ["dqx", "gx"])
def test_end_to_end_quality_gate_passes_on_generated_history(spark, generated, engine):
    ev, mv, fxdf, et, bl, gl = generated
    as_at = D(2026, 9, 30)
    conv = g.convert_movements(mv, fxdf, as_at).cache()
    pos = g.build_positions(ev, conv, et, bl, as_at).cache()
    reg = g.build_regulatory_dataset(pos, conv, as_at)
    mat = g.build_loss_matrix(pos)
    recon = g.build_recon(mv, gl, ev, 1.0, as_at)
    res = run_gate(spark, "orm_gold", engine, {"movements_converted": conv, "fact_loss_event_position": pos,
                                                "recon_loss_vs_gl": recon}, "x", "t",
                   workspace_client=MagicMock(), write=False) + g.reconciliation_checks(pos, reg, mat)
    by = {r["check_name"]: r for r in res}
    assert gate_passed(res), [r for r in res if not r["passed"]]
    assert by["ten_years_present"]["passed"]
    assert by["gl_reconciles"]["failed_count"] > 0 and by["gl_reconciles"]["passed"]   # breaks found, within 5%
    assert pos.where("confidentiality = 'RESTRICTED'").count() > 0
