"""
ORM Gold build (Databricks job task: spark_python_task).

Products (all carry legal_entity_id for row-level security):
  fact_loss_event_position      per event, as at a date: gross / recoveries / net in txn currency, EUR, GBP
  mart_regulatory_loss_dataset  10-year annual net losses above EUR 20k and EUR 100k (Basel loss-component input)
  mart_loss_matrix              business line x event type counts and amounts per reporting year
  mart_kri_dashboard            KRI values with RAG status and trend
  mart_residual_risk_heatmap    latest approved RCSA per risk with control-test failures
  mart_issue_ageing             open/overdue issues by severity
  recon_loss_vs_gl              loss movements reconciled to GL postings per event and month

Business rules
  * Gross loss  = GROSS_LOSS + PROVISION - PROVISION_RELEASE (provisions count until released)
  * Recoveries  = RECOVERY_INSURANCE + RECOVERY_OTHER (never netted into gross)
  * Every movement is converted at the FX rate of its own accounting date
  * Regulatory data set: included, non-near-miss, non-gain events, excluding credit-boundary events,
    grouped to the root event (parent_event_id) before the threshold test; losses are allocated to
    years by accounting date
  * Data-quality gate: shared framework (src/common/dq, rules/orm_gold.yml, DQX or GX) plus custom
    reconciliation tie-outs. Failed ERROR rules abort before Gold is written; WARN results are recorded
"""
import argparse
import datetime as dt
import os
import sys
import uuid

# Make the shared src/ package importable when this file runs as a Databricks job task
_SRC = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__ if "__file__" in globals() else sys.argv[0])), "..", ".."))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
from common.dq.runner import gate_passed, print_results, run_gate, write_results  # noqa: E402

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

GROSS_SIGN = {"GROSS_LOSS": 1, "PROVISION": 1, "PROVISION_RELEASE": -1}
RECOVERY_TYPES = ["RECOVERY_INSURANCE", "RECOVERY_OTHER"]
GL_SIGN = {"GROSS_LOSS": 1, "PROVISION": 1, "PROVISION_RELEASE": -1, "RECOVERY_INSURANCE": -1, "RECOVERY_OTHER": -1}
THRESHOLDS_EUR = [20000, 100000]

POSITION_COLS = ["event_id", "source_system", "legal_entity_id", "org_unit_id", "business_line_l1_code",
                 "business_line_l2_code", "event_type_l1_code", "event_type_l2_code", "cause_code",
                 "occurrence_date", "discovery_date", "first_accounting_date", "event_status", "boundary_type",
                 "is_near_miss", "is_operational_gain", "is_rapidly_recovered", "parent_event_id", "root_event_id",
                 "inclusion_status", "confidentiality", "currency", "event_title", "event_description",
                 "gross_loss_txn", "recoveries_txn", "net_loss_txn", "gross_loss_eur", "recoveries_eur",
                 "net_loss_eur", "net_loss_gbp", "above_20k_eur", "above_100k_eur", "as_at_date", "_loaded_at"]
REG_COLS = ["legal_entity_id", "threshold_eur", "accounting_year", "event_count", "gross_loss_eur",
            "recoveries_eur", "net_loss_eur", "as_at_date", "_loaded_at"]
MATRIX_COLS = ["legal_entity_id", "reporting_year", "business_line_l1_code", "event_type_l1_code",
               "event_count", "gross_loss_eur", "recoveries_eur", "net_loss_eur", "_loaded_at"]
KRI_COLS = ["legal_entity_id", "org_unit_id", "kri_id", "kri_name", "risk_id", "period_end", "value",
            "amber_threshold", "red_threshold", "direction", "rag_status", "prior_value", "trend", "_loaded_at"]
HEAT_COLS = ["legal_entity_id", "org_unit_id", "risk_id", "risk_title", "event_type_l1_code", "assessment_period",
             "inherent_score", "residual_score", "control_effectiveness", "key_controls", "failed_tests",
             "residual_rating", "_loaded_at"]
ISSUE_COLS = ["legal_entity_id", "issue_id", "title", "source", "severity", "status", "owner_emplid", "raised_date",
              "due_date", "days_overdue", "ageing_bucket", "is_overdue", "_loaded_at"]
RECON_COLS = ["legal_entity_id", "event_id", "accounting_month", "loss_amount", "gl_amount", "difference",
              "recon_status", "currency", "_loaded_at"]


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Build ORM Gold layer")
    p.add_argument("--catalog", required=True)
    p.add_argument("--as-at", default="", help="Position date YYYY-MM-DD (default: today)")
    p.add_argument("--recon-tolerance", type=float, default=1.0, help="Absolute difference treated as matched")
    p.add_argument("--dq-engine", default="dqx", choices=["dqx", "gx"])
    return p.parse_args(argv)


# ----------------------------------------------------------------------------- transforms
def convert_movements(moves: DataFrame, fx: DataFrame, as_at: dt.date) -> DataFrame:
    """Signed gross/recovery components per movement, converted at the accounting-date rate."""
    m = moves.where(F.col("accounting_date") <= F.lit(as_at))
    gross_sign = F.coalesce(*[F.when(F.col("movement_type") == k, F.lit(v)) for k, v in GROSS_SIGN.items()], F.lit(0))
    m = (m.withColumn("gross_part", F.col("amount") * gross_sign)
          .withColumn("recovery_part", F.when(F.col("movement_type").isin(RECOVERY_TYPES), F.col("amount"))
                      .otherwise(F.lit(0))))
    r = fx.select(F.col("currency").alias("_ccy"), F.col("rate_date").alias("_d"), "rate_to_eur", "rate_to_gbp")
    m = m.join(F.broadcast(r), (m["currency"] == r["_ccy"]) & (m["accounting_date"] == r["_d"]), "left") \
         .drop("_ccy", "_d")
    for part in ("gross_part", "recovery_part"):
        m = (m.withColumn(f"{part}_eur", F.col(part) * F.col("rate_to_eur"))
              .withColumn(f"{part}_gbp", F.col(part) * F.col("rate_to_gbp")))
    return m


def current_events(loss_event: DataFrame) -> DataFrame:
    return loss_event.where(F.col("__END_AT").isNull())


def build_positions(events: DataFrame, conv: DataFrame, ref_et: DataFrame, ref_bl: DataFrame,
                    as_at: dt.date) -> DataFrame:
    agg = conv.groupBy("event_id").agg(
        F.sum("gross_part").alias("gross_loss_txn"), F.sum("recovery_part").alias("recoveries_txn"),
        F.sum("gross_part_eur").alias("gross_loss_eur"), F.sum("recovery_part_eur").alias("recoveries_eur"),
        F.sum("gross_part_gbp").alias("gross_gbp"), F.sum("recovery_part_gbp").alias("rec_gbp"),
        F.count(F.when(F.col("rate_to_eur").isNull(), 1)).alias("_missing_fx"))
    e = (events.join(F.broadcast(ref_et.select("event_type_l2_code", "event_type_l1_code")), "event_type_l2_code", "left")
               .join(F.broadcast(ref_bl.select(F.col("business_line_l2_code"), "business_line_l1_code")),
                     "business_line_l2_code", "left"))
    zero = lambda c: F.coalesce(F.col(c), F.lit(0)).cast("decimal(18,2)")  # noqa: E731
    out = (e.join(agg, "event_id", "left")
            .withColumn("root_event_id", F.coalesce("parent_event_id", "event_id"))
            .withColumn("gross_loss_txn", zero("gross_loss_txn")).withColumn("recoveries_txn", zero("recoveries_txn"))
            .withColumn("net_loss_txn", (F.col("gross_loss_txn") - F.col("recoveries_txn")).cast("decimal(18,2)"))
            .withColumn("gross_loss_eur", zero("gross_loss_eur")).withColumn("recoveries_eur", zero("recoveries_eur"))
            .withColumn("net_loss_eur", (F.col("gross_loss_eur") - F.col("recoveries_eur")).cast("decimal(18,2)"))
            .withColumn("net_loss_gbp", (zero("gross_gbp") - zero("rec_gbp")).cast("decimal(18,2)"))
            .withColumn("above_20k_eur", F.col("net_loss_eur") >= 20000)
            .withColumn("above_100k_eur", F.col("net_loss_eur") >= 100000)
            .withColumn("as_at_date", F.lit(as_at))
            .withColumn("_loaded_at", F.current_timestamp()))
    return out


def build_regulatory_dataset(positions: DataFrame, conv: DataFrame, as_at: dt.date) -> DataFrame:
    """Annual net losses by accounting year for root events above each threshold (last 10 years)."""
    eligible = positions.where(
        (F.col("inclusion_status") == "INCLUDED") & ~F.col("is_near_miss") & ~F.col("is_operational_gain")
        & (F.col("boundary_type") != "CREDIT"))
    roots = (eligible.groupBy("legal_entity_id", "root_event_id")
             .agg(F.sum("net_loss_eur").alias("root_net_eur")))
    ev_root = eligible.select("event_id", "legal_entity_id", "root_event_id")
    yearly = (conv.join(ev_root, "event_id")
              .withColumn("accounting_year", F.year("accounting_date"))
              .groupBy("legal_entity_id", "root_event_id", "accounting_year")
              .agg(F.sum("gross_part_eur").alias("g"), F.sum("recovery_part_eur").alias("r")))
    thresholds = conv.sparkSession.createDataFrame([(t,) for t in THRESHOLDS_EUR], "threshold_eur int")
    first_year = as_at.year - 9
    return (yearly.join(roots, ["legal_entity_id", "root_event_id"]).crossJoin(thresholds)
            .where((F.col("root_net_eur") >= F.col("threshold_eur")) & (F.col("accounting_year") >= first_year))
            .groupBy("legal_entity_id", "threshold_eur", "accounting_year")
            .agg(F.countDistinct("root_event_id").alias("event_count"),
                 F.sum("g").cast("decimal(18,2)").alias("gross_loss_eur"),
                 F.sum("r").cast("decimal(18,2)").alias("recoveries_eur"))
            .withColumn("net_loss_eur", (F.col("gross_loss_eur") - F.col("recoveries_eur")).cast("decimal(18,2)"))
            .withColumn("as_at_date", F.lit(as_at)).withColumn("_loaded_at", F.current_timestamp()))


def build_loss_matrix(positions: DataFrame) -> DataFrame:
    p = positions.where((F.col("inclusion_status") == "INCLUDED") & ~F.col("is_near_miss")
                        & F.col("first_accounting_date").isNotNull())
    return (p.withColumn("reporting_year", F.year("first_accounting_date"))
            .groupBy("legal_entity_id", "reporting_year", "business_line_l1_code", "event_type_l1_code")
            .agg(F.count("*").alias("event_count"), F.sum("gross_loss_eur").cast("decimal(18,2)").alias("gross_loss_eur"),
                 F.sum("recoveries_eur").cast("decimal(18,2)").alias("recoveries_eur"),
                 F.sum("net_loss_eur").cast("decimal(18,2)").alias("net_loss_eur"))
            .withColumn("_loaded_at", F.current_timestamp()))


def rag(value, amber, red, direction) -> F.Column:
    higher = F.when(value >= red, "RED").when(value >= amber, "AMBER").otherwise("GREEN")
    lower = F.when(value <= red, "RED").when(value <= amber, "AMBER").otherwise("GREEN")
    return F.when(direction == "HIGHER_IS_WORSE", higher).otherwise(lower)


def build_kri_dashboard(defs: DataFrame, obs: DataFrame, risks: DataFrame, org: DataFrame) -> DataFrame:
    w = Window.partitionBy("kri_id").orderBy("period_end")
    d = defs.select("kri_id", F.col("name").alias("kri_name"), "risk_id", "amber_threshold", "red_threshold", "direction")
    ro = risks.select("risk_id", "org_unit_id").join(org.select("org_unit_id", "legal_entity_id"), "org_unit_id", "left")
    o = (obs.select("kri_id", "period_end", "value").join(d, "kri_id").join(ro, "risk_id", "left")
         .withColumn("rag_status", rag(F.col("value"), F.col("amber_threshold"), F.col("red_threshold"), F.col("direction")))
         .withColumn("prior_value", F.lag("value").over(w)))
    worse = F.when(F.col("direction") == "HIGHER_IS_WORSE", F.col("value") > F.col("prior_value") * 1.05) \
             .otherwise(F.col("value") < F.col("prior_value") * 0.95)
    better = F.when(F.col("direction") == "HIGHER_IS_WORSE", F.col("value") < F.col("prior_value") * 0.95) \
              .otherwise(F.col("value") > F.col("prior_value") * 1.05)
    return (o.withColumn("trend", F.when(F.col("prior_value").isNull(), "NEW").when(worse, "WORSENING")
                         .when(better, "IMPROVING").otherwise("STABLE"))
            .withColumn("_loaded_at", F.current_timestamp()))


def build_heatmap(rcsa: DataFrame, risks: DataFrame, org: DataFrame, links: DataFrame, controls: DataFrame,
                  tests: DataFrame, ref_et: DataFrame, as_at: dt.date) -> DataFrame:
    w = Window.partitionBy("risk_id").orderBy(F.col("assessment_period").desc())
    latest = (rcsa.where("approval_status = 'APPROVED'").withColumn("_rn", F.row_number().over(w))
              .where("_rn = 1").drop("_rn"))
    key_ctl = (links.join(controls.where("is_key_control").select("control_id"), "control_id")
               .groupBy("risk_id").agg(F.count("*").alias("key_controls")))
    since = F.lit(as_at - dt.timedelta(days=365))
    failed = (links.join(tests.where((F.col("test_date") >= since) &
                                     ((F.col("design_effectiveness") == "FAIL") | (F.col("operating_effectiveness") == "FAIL")))
                         .select("control_id"), "control_id")
              .groupBy("risk_id").agg(F.count("*").alias("failed_tests")))
    r = (risks.select("risk_id", F.col("title").alias("risk_title"), "org_unit_id", "risk_event_type_l2_code")
         .join(org.select("org_unit_id", "legal_entity_id"), "org_unit_id", "left")
         .join(F.broadcast(ref_et.select(F.col("event_type_l2_code").alias("risk_event_type_l2_code"),
                                         "event_type_l1_code")), "risk_event_type_l2_code", "left"))
    out = (latest.join(r, "risk_id")
           .withColumn("inherent_score", F.col("inherent_likelihood") * F.col("inherent_impact"))
           .withColumn("residual_score", F.col("residual_likelihood") * F.col("residual_impact"))
           .join(key_ctl, "risk_id", "left").join(failed, "risk_id", "left")
           .withColumn("key_controls", F.coalesce("key_controls", F.lit(0)))
           .withColumn("failed_tests", F.coalesce("failed_tests", F.lit(0)))
           .withColumn("residual_rating", F.when(F.col("residual_score") >= 15, "HIGH")
                       .when(F.col("residual_score") >= 8, "MEDIUM").otherwise("LOW"))
           .withColumn("_loaded_at", F.current_timestamp()))
    return out


def build_issue_ageing(issues: DataFrame, risks: DataFrame, org: DataFrame, as_at: dt.date) -> DataFrame:
    ro = risks.select("risk_id", "org_unit_id").join(org.select("org_unit_id", "legal_entity_id"), "org_unit_id", "left")
    open_ = F.col("status") != "CLOSED"
    days = F.datediff(F.lit(as_at), F.col("due_date"))
    return (issues.join(ro, "risk_id", "left")
            .withColumn("days_overdue", F.when(open_ & (days > 0), days).otherwise(F.lit(0)))
            .withColumn("is_overdue", open_ & (days > 0))
            .withColumn("ageing_bucket", F.when(~open_, "CLOSED").when(days <= 0, "NOT_DUE")
                        .when(days <= 30, "1-30").when(days <= 90, "31-90").otherwise("90+"))
            .withColumn("_loaded_at", F.current_timestamp()))


def build_recon(moves: DataFrame, gl: DataFrame, events: DataFrame, tolerance: float, as_at: dt.date) -> DataFrame:
    sign = F.coalesce(*[F.when(F.col("movement_type") == k, F.lit(v)) for k, v in GL_SIGN.items()], F.lit(0))
    loss = (moves.where(F.col("accounting_date") <= F.lit(as_at))
            .withColumn("accounting_month", F.date_trunc("month", "accounting_date").cast("date"))
            .groupBy("event_id", "accounting_month", "currency")
            .agg(F.sum(F.col("amount") * sign).cast("decimal(18,2)").alias("loss_amount")))
    g = (gl.where(F.col("posting_date") <= F.lit(as_at))
         .withColumn("accounting_month", F.date_trunc("month", "posting_date").cast("date"))
         .groupBy(F.col("event_ref").alias("event_id"), "accounting_month", "currency")
         .agg(F.sum("amount").cast("decimal(18,2)").alias("gl_amount")))
    j = loss.join(g, ["event_id", "accounting_month", "currency"], "full_outer")
    diff = F.coalesce("loss_amount", F.lit(0)) - F.coalesce("gl_amount", F.lit(0))
    status = (F.when(F.col("gl_amount").isNull(), "MISSING_IN_GL")
              .when(F.col("loss_amount").isNull(), "MISSING_IN_LOSS")
              .when(F.abs(diff) <= tolerance, "MATCHED").otherwise("AMOUNT_BREAK"))
    return (j.withColumn("difference", diff.cast("decimal(18,2)")).withColumn("recon_status", status)
            .join(events.select("event_id", "legal_entity_id"), "event_id", "left")
            .withColumn("_loaded_at", F.current_timestamp()))


# ----------------------------------------------------------------------------- custom reconciliation checks
def reconciliation_checks(positions, reg, matrix):
    """Cross-table tie-outs that rule engines do not express well; same result shape as run_gate()."""
    out = []
    pos_net = positions.where("inclusion_status = 'INCLUDED' AND NOT is_near_miss AND first_accounting_date IS NOT NULL") \
                       .agg(F.sum("net_loss_eur")).first()[0] or 0
    mat_net = matrix.agg(F.sum("net_loss_eur")).first()[0] or 0
    gap = abs(float(pos_net) - float(mat_net))
    out.append(dict(gate="orm_gold", engine="custom", table_name="mart_loss_matrix", check_name="matrix_ties_to_positions",
                    severity="error", passed=gap < 1.0, failed_count=int(gap), total_count=1, threshold_pct=0.0,
                    details="Loss matrix total equals position total (EUR)"))
    years = reg.where(F.col("threshold_eur") == 20000).select("accounting_year").distinct().count()
    out.append(dict(gate="orm_gold", engine="custom", table_name="mart_regulatory_loss_dataset",
                    check_name="ten_years_present", severity="warn", passed=years >= 10, failed_count=max(0, 10 - years),
                    total_count=10, threshold_pct=0.0, details="10 accounting years present at EUR 20k"))
    return out


def overwrite_table(spark, df, table, cols):
    view = f"_stage_{table.split('.')[-1]}"
    df.select(*cols).createOrReplaceTempView(view)
    spark.sql(f"INSERT OVERWRITE TABLE {table} SELECT {', '.join(cols)} FROM {view}")


def main(argv=None):
    a = parse_args(argv)
    c = a.catalog
    as_at = dt.date.fromisoformat(a.as_at) if a.as_at else dt.date.today()
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        spark.conf.set("spark.sql.shuffle.partitions", "auto")
    run_id = str(uuid.uuid4())
    # STEP 1/2 - read Silver and build every candidate product (nothing written yet)
    s = lambda t: spark.table(f"{c}.silver.{t}")  # noqa: E731
    cur = lambda t: s(t).where(F.col("__END_AT").isNull())  # noqa: E731  (SCD2 current rows)

    events = cur("loss_event")
    conv = convert_movements(s("loss_financial_movement"), s("ref_fx_daily"), as_at).cache()
    positions = build_positions(events, conv, s("ref_event_type"), s("ref_business_line"), as_at).cache()
    reg = build_regulatory_dataset(positions, conv, as_at).cache()
    matrix = build_loss_matrix(positions).cache()
    kri = build_kri_dashboard(s("kri_definition"), s("kri_observation"), cur("risk"), cur("org_unit"))
    heat = build_heatmap(s("rcsa_assessment"), cur("risk"), cur("org_unit"), s("risk_control"), cur("control"),
                         s("control_test"), s("ref_event_type"), as_at)
    ageing = build_issue_ageing(s("issue_action"), cur("risk"), cur("org_unit"), as_at)
    recon = build_recon(s("loss_financial_movement"), s("gl_posting"), events, a.recon_tolerance, as_at).cache()

    # STEP 3 - quality gate: YAML rules (src/common/dq/rules/orm_gold.yml) + custom tie-outs
    results = run_gate(spark, "orm_gold", a.dq_engine,
                       {"movements_converted": conv, "fact_loss_event_position": positions,
                        "recon_loss_vs_gl": recon}, c, run_id)
    custom = reconciliation_checks(positions, reg, matrix)
    write_results(spark, c, run_id, custom, [])
    results += custom
    print_results(results)
    if not gate_passed(results):
        raise RuntimeError(f"Data-quality gate failed; Gold not updated. run_id={run_id}")

    # STEP 4 - promote every product (data replaced, governance metadata kept)
    for df, table, cols in [(positions, "fact_loss_event_position", POSITION_COLS),
                            (reg, "mart_regulatory_loss_dataset", REG_COLS),
                            (matrix, "mart_loss_matrix", MATRIX_COLS),
                            (kri, "mart_kri_dashboard", KRI_COLS),
                            (heat, "mart_residual_risk_heatmap", HEAT_COLS),
                            (ageing, "mart_issue_ageing", ISSUE_COLS),
                            (recon, "recon_loss_vs_gl", RECON_COLS)]:
        overwrite_table(spark, df, f"{c}.gold.{table}", cols)
        print(f"promoted {c}.gold.{table}")
    print(f"ORM Gold promoted as at {as_at}. run_id={run_id}")


if __name__ == "__main__":
    main(sys.argv[1:])
