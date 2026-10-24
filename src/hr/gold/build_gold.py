"""
HR Gold build (Databricks job task: spark_python_task).

Reads Silver (SCD2 job history, current personal data, facts) and reference data,
builds a star schema and promotes it to Gold only if the data-quality gate passes.

    gold.dim_employee     SCD2 dimension (one row per job/comp version), surrogate key
    gold.dim_department   Department dimension
    gold.fact_absence     Absence facts, point-in-time joined to the employee version
    gold.fact_engagement  Pulse survey facts, point-in-time joined

Design notes
  * Gold tables are created once by setup DDL (with governed tags for ABAC). This job only
    replaces their data with INSERT OVERWRITE, so tags, comments and policies are preserved
    and every run is idempotent (safe to re-run).
  * Skew: 45% of employees are in department D001. Joins on the skewed dept key use small
    broadcast dimensions (no shuffle on the skewed key); AQE skew-join handling stays enabled
    for any residual skew in shuffled joins.
  * Data-quality gate: the shared framework (src/common/dq) runs the YAML rules in
    rules/hr_gold.yml with DQX or Great Expectations (--dq-engine) on the candidate DataFrames
    BEFORE anything is written. Failed ERROR rules abort the job; WARN rules are only recorded.
"""
import argparse
import os
import sys
import uuid

# Make the shared src/ package importable when this file runs as a Databricks job task
_SRC = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__ if "__file__" in globals() else sys.argv[0])), "..", ".."))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
from common.dq.runner import gate_passed, print_results, run_gate  # noqa: E402

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

OPEN_END = "9999-12-31"

DIM_EMPLOYEE_COLS = ["employee_sk", "emplid", "first_name", "last_name", "birth_date",
                     "country_code", "national_id_type", "national_id", "hire_date",
                     "dept_id", "job_code", "job_title", "location", "empl_status",
                     "annual_rt", "currency_cd", "manager_id", "valid_from", "valid_to",
                     "is_current", "_loaded_at"]
DIM_DEPARTMENT_COLS = ["dept_id", "dept_name", "cost_centre", "_loaded_at"]
FACT_ABSENCE_COLS = ["absence_id", "employee_sk", "emplid", "dept_id", "absence_type",
                     "start_dt", "days", "_loaded_at"]
FACT_ENGAGEMENT_COLS = ["response_id", "employee_sk", "emplid", "dept_id", "survey_date",
                        "engagement_score", "sentiment_score", "_loaded_at"]


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Build HR Gold layer")
    p.add_argument("--catalog", required=True)
    p.add_argument("--dq-engine", default="dqx", choices=["dqx", "gx"],
                   help="Data-quality engine for the Gold gate (same YAML rules either way)")
    return p.parse_args(argv)


def configure_spark(spark: SparkSession) -> None:
    """Production tuning. Databricks-only settings are applied only on Databricks."""
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        # Databricks auto-optimized shuffle: AQE picks the partition count per stage
        spark.conf.set("spark.sql.shuffle.partitions", "auto")


# ----------------------------------------------------------------------------- transforms
def build_dim_employee(job_history: DataFrame, personal: DataFrame) -> DataFrame:
    """SCD2 employee dimension.

    job_history: AUTO CDC SCD2 output (has __START_AT / __END_AT sequenced by LSN).
    personal:    current personal data (SCD1). Erased employees have no row -> PII is NULL.
    Business validity uses effective dates (effdt), ordered by the source sequence.
    """
    w = Window.partitionBy("emplid").orderBy("__START_AT")
    versions = (job_history
                .withColumn("valid_from", F.col("effdt"))
                .withColumn("next_from", F.lead("effdt").over(w))
                .withColumn("valid_to", F.when(F.col("next_from").isNull(), F.lit(OPEN_END).cast("date"))
                            .otherwise(F.greatest(F.col("valid_from"), F.date_sub(F.col("next_from"), 1))))
                .withColumn("is_current", F.col("__END_AT").isNull())
                .withColumn("employee_sk", F.xxhash64(F.col("emplid"), F.col("__START_AT"))))
    pii = personal.select("emplid", "first_name", "last_name", "birth_date", "country_code",
                          "national_id_type", "national_id", "hire_date")
    return (versions.join(pii, "emplid", "left")
            .withColumn("_loaded_at", F.current_timestamp())
            .select(*DIM_EMPLOYEE_COLS))


def point_in_time_join(facts: DataFrame, dim: DataFrame, date_col: str) -> DataFrame:
    """Attach the employee version valid on the fact date.

    If several versions are valid on the same day (multiple changes in one day), the latest
    version wins, so each fact row maps to exactly one employee_sk.
    """
    d = dim.select("employee_sk", F.col("emplid").alias("_d_emplid"), "valid_from",
                   "valid_to", F.col("dept_id").alias("_d_dept"))
    cond = ((facts["emplid"] == d["_d_emplid"]) &
            (facts[date_col] >= d["valid_from"]) & (facts[date_col] <= d["valid_to"]))
    joined = facts.join(d, cond, "left")
    w = Window.partitionBy(*[facts[c] for c in facts.columns]).orderBy(F.col("valid_from").desc_nulls_last(),
                                                                       F.col("employee_sk").desc_nulls_last())
    return (joined.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
            .drop("_rn", "_d_emplid", "valid_from", "valid_to")
            .withColumnRenamed("_d_dept", "dept_id"))


def build_dim_department(ref_department: DataFrame) -> DataFrame:
    return (ref_department.withColumn("_loaded_at", F.current_timestamp())
            .select(*DIM_DEPARTMENT_COLS))


def build_fact_absence(absence: DataFrame, dim_employee: DataFrame) -> DataFrame:
    f = absence.select("absence_id", "emplid", "absence_type", "start_dt", "days")
    return (point_in_time_join(f, dim_employee, "start_dt")
            .withColumn("_loaded_at", F.current_timestamp()).select(*FACT_ABSENCE_COLS))


def build_fact_engagement(pulse: DataFrame, dim_employee: DataFrame) -> DataFrame:
    f = pulse.select("response_id", "emplid", "survey_date", "engagement_score", "sentiment_score")
    return (point_in_time_join(f, dim_employee, "survey_date")
            .withColumn("_loaded_at", F.current_timestamp()).select(*FACT_ENGAGEMENT_COLS))


def overwrite_table(spark: SparkSession, df: DataFrame, table: str, cols: list) -> None:
    """Replace data but keep table metadata (tags, comments, policies). Idempotent."""
    view = f"_stage_{table.split('.')[-1]}"
    df.select(*cols).createOrReplaceTempView(view)
    spark.sql(f"INSERT OVERWRITE TABLE {table} SELECT {', '.join(cols)} FROM {view}")


# ----------------------------------------------------------------------------- main
def main(argv=None):
    """Steps: read Silver -> build candidates -> quality gate -> promote to Gold (only if the gate passes)."""
    args = parse_args(argv)
    c = args.catalog
    spark = SparkSession.builder.getOrCreate()
    configure_spark(spark)
    run_id = str(uuid.uuid4())

    # STEP 1 - read the Silver inputs
    job_history = spark.table(f"{c}.silver.hr_job_history")
    personal = spark.table(f"{c}.silver.hr_personal_current")
    ref_department = spark.table(f"{c}.bronze.ref_department")

    # STEP 2 - build candidate Gold DataFrames (nothing is written yet)
    dim_employee = build_dim_employee(job_history, personal).cache()
    dim_department = F.broadcast(build_dim_department(ref_department))
    fact_absence = build_fact_absence(spark.table(f"{c}.silver.hr_absence"), dim_employee).cache()
    fact_engagement = build_fact_engagement(spark.table(f"{c}.silver.hr_pulse_survey"), dim_employee).cache()

    # STEP 3 - quality gate (rules: src/common/dq/rules/hr_gold.yml), results -> ops.dq_results
    results = run_gate(spark, "hr_gold", args.dq_engine,
                       {"dim_department": dim_department, "dim_employee": dim_employee,
                        "fact_absence": fact_absence, "fact_engagement": fact_engagement}, c, run_id)
    print_results(results)
    if not gate_passed(results):
        raise RuntimeError(f"Data-quality gate failed; Gold not updated. run_id={run_id}")

    # STEP 4 - promote: replace data only (tags, comments and policies are kept)
    overwrite_table(spark, dim_department, f"{c}.gold.dim_department", DIM_DEPARTMENT_COLS)
    overwrite_table(spark, dim_employee, f"{c}.gold.dim_employee", DIM_EMPLOYEE_COLS)
    overwrite_table(spark, fact_absence, f"{c}.gold.fact_absence", FACT_ABSENCE_COLS)
    overwrite_table(spark, fact_engagement, f"{c}.gold.fact_engagement", FACT_ENGAGEMENT_COLS)
    print(f"Gold promoted. engine={args.dq_engine} run_id={run_id}")


if __name__ == "__main__":
    main(sys.argv[1:])
