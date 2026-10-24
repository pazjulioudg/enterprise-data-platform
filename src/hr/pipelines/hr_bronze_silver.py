"""
HR Bronze -> Silver pipeline (Lakeflow Spark Declarative Pipelines, Python API).

Bronze : Auto Loader ingestion of Debezium-style CDC JSON from the landing volume.
         Permissive: schema evolution (addNewColumns), source file + ingest time kept.
Silver : Strict contract. Flattened, typed, de-duplicated, validated.
         * hr_job_history       - SCD Type 2 (AUTO CDC) on job/compensation attributes
         * hr_personal_current  - SCD Type 1 with deletes (GDPR erasure)
         * hr_absence / hr_performance / hr_pulse_survey - append-only facts
         * hr_*_quarantine      - rejected rows with the names of the rules they failed

Erasure (GDPR): the ops erasure job DELETES rows from Bronze. Silver streams read Bronze with
skipChangeCommits=true so those deletes do not break the streams (they are not re-processed).

Pipeline configuration keys (set in resources/hr_pipeline.yml):
    edp.catalog       e.g. hr_dev
    edp.landing_root  e.g. /Volumes/hr_dev/bronze/landing/hr
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("edp.catalog")  # noqa: F821  (spark is provided by the pipeline runtime)
LANDING = spark.conf.get("edp.landing_root")  # noqa: F821
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
DEDUP_WATERMARK = "2 days"


# ----------------------------------------------------------------------------- helpers
def safe(rules: dict) -> dict:
    """Make expectation expressions NULL-safe: a NULL result counts as a failure."""
    return {name: f"coalesce(({expr}), false)" for name, expr in rules.items()}


def with_failed_rules(df, rules: dict):
    """Add an array column listing every rule the row fails (empty array = clean row)."""
    checks = [F.when(~F.expr(expr), F.lit(name)) for name, expr in safe(rules).items()]
    return df.withColumn("failed_rules", F.filter(F.array(*checks), lambda x: x.isNotNull()))


def autoload(entity: str):
    return (spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(f"{LANDING}/{entity}/")
            .select("*",
                    F.col("_metadata.file_path").alias("_source_file"),
                    F.current_timestamp().alias("_ingest_ts")))


# ----------------------------------------------------------------------------- bronze
def make_bronze(entity: str):
    @dp.table(
        name=f"{BRONZE}.hr_{entity}_raw",
        comment=f"Raw Debezium-style CDC events for HCM entity '{entity}'. Append-only, schema evolves.",
        table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"},
    )
    def _bronze():
        return autoload(entity)
    return _bronze


for _entity in ["job", "personal_data", "absence", "performance", "pulse_survey"]:
    make_bronze(_entity)


# ----------------------------------------------------------------------------- job (SCD2)
JOB_RULES = {
    "valid_emplid": "emplid IS NOT NULL",
    "valid_salary": "annual_rt > 0",
    "valid_currency": "currency_cd IN ('USD', 'GBP', 'INR')",
    "valid_status": "empl_status IN ('A', 'T', 'L')",
    "valid_lsn": "lsn IS NOT NULL",
}
JOB_HISTORY_COLS = ["dept_id", "job_code", "job_title", "location", "empl_status",
                    "annual_rt", "currency_cd", "manager_id"]


@dp.temporary_view(name="v_job_changes", comment="Flattened, typed job events (contract columns only).")
def v_job_changes():
    a = F.col("after")
    return (spark.readStream.option("skipChangeCommits", "true").table(f"{BRONZE}.hr_job_raw")  # noqa: F821
            .select(
                F.col("op"),
                F.col("lsn").cast("long").alias("lsn"),
                (F.col("ts_ms") / 1000).cast("timestamp").alias("event_ts"),
                a.getField("emplid").alias("emplid"),
                a.getField("effdt").cast("date").alias("effdt"),
                a.getField("effseq").cast("int").alias("effseq"),
                a.getField("dept_id").alias("dept_id"),
                a.getField("job_code").alias("job_code"),
                a.getField("job_title").alias("job_title"),
                a.getField("location").alias("location"),
                a.getField("empl_status").alias("empl_status"),
                a.getField("annual_rt").cast("decimal(18,2)").alias("annual_rt"),
                a.getField("currency_cd").alias("currency_cd"),
                a.getField("manager_id").alias("manager_id"),
                F.col("_source_file")))


@dp.table(name=f"{SILVER}.hr_job_quarantine",
          comment="Job events rejected by data-quality rules, with the failed rule names.",
          table_properties={"quality": "quarantine"})
def hr_job_quarantine():
    df = with_failed_rules(spark.readStream.table("v_job_changes"), JOB_RULES)  # noqa: F821
    return (df.where(F.size("failed_rules") > 0)
              .withColumn("quarantined_at", F.current_timestamp()))


@dp.temporary_view(name="v_job_clean", comment="Valid, de-duplicated job events feeding AUTO CDC.")
@dp.expect_all_or_drop(safe(JOB_RULES))
def v_job_clean():
    return (spark.readStream.table("v_job_changes")  # noqa: F821
            .withWatermark("event_ts", DEDUP_WATERMARK)
            .dropDuplicatesWithinWatermark(["emplid", "lsn"]))


dp.create_streaming_table(
    name=f"{SILVER}.hr_job_history",
    comment="Employee job and compensation history (SCD Type 2, sequenced by source LSN).",
    table_properties={"quality": "silver"},
    cluster_by=["emplid"],
)
dp.create_auto_cdc_flow(
    target=f"{SILVER}.hr_job_history",
    source="v_job_clean",
    keys=["emplid"],
    sequence_by=F.col("lsn"),
    except_column_list=["op", "event_ts", "_source_file"],
    stored_as_scd_type=2,
    track_history_column_list=JOB_HISTORY_COLS,
)


# ----------------------------------------------------------------------------- personal data (SCD1 + deletes)
PERSONAL_RULES = {
    "valid_emplid": "emplid IS NOT NULL",
    "valid_country": "op = 'd' OR country_code IN ('US', 'GB', 'IN')",
    "valid_id_type": ("op = 'd' OR (country_code = 'US' AND national_id_type = 'SSN') "
                      "OR (country_code = 'GB' AND national_id_type = 'NINO') "
                      "OR (country_code = 'IN' AND national_id_type IN ('PAN', 'AADHAAR'))"),
    "valid_national_id_format": (
        "op = 'd' OR "
        "(national_id_type = 'SSN' AND national_id RLIKE '^[0-9]{3}-[0-9]{2}-[0-9]{4}$') OR "
        "(national_id_type = 'NINO' AND national_id RLIKE '^[A-Z]{2}[0-9]{6}[A-D]$') OR "
        "(national_id_type = 'PAN' AND national_id RLIKE '^[A-Z]{5}[0-9]{4}[A-Z]$') OR "
        "(national_id_type = 'AADHAAR' AND national_id RLIKE '^[0-9]{4}-[0-9]{4}-[0-9]{4}$')"),
}


@dp.temporary_view(name="v_personal_changes", comment="Flattened personal-data events (key from before-image on delete).")
def v_personal_changes():
    a = F.col("after")
    return (spark.readStream.option("skipChangeCommits", "true").table(f"{BRONZE}.hr_personal_data_raw")  # noqa: F821
            .select(
                F.col("op"),
                F.col("lsn").cast("long").alias("lsn"),
                (F.col("ts_ms") / 1000).cast("timestamp").alias("event_ts"),
                F.coalesce(a.getField("emplid"), F.col("before").getField("emplid")).alias("emplid"),
                a.getField("first_name").alias("first_name"),
                a.getField("last_name").alias("last_name"),
                a.getField("birth_date").cast("date").alias("birth_date"),
                a.getField("country_code").alias("country_code"),
                a.getField("national_id_type").alias("national_id_type"),
                a.getField("national_id").alias("national_id"),
                a.getField("hire_date").cast("date").alias("hire_date")))


@dp.table(name=f"{SILVER}.hr_personal_quarantine",
          comment="Personal-data events rejected by data-quality rules.",
          table_properties={"quality": "quarantine"})
def hr_personal_quarantine():
    df = with_failed_rules(spark.readStream.table("v_personal_changes"), PERSONAL_RULES)  # noqa: F821
    return (df.where(F.size("failed_rules") > 0)
              .withColumn("quarantined_at", F.current_timestamp()))


@dp.temporary_view(name="v_personal_clean")
@dp.expect_all_or_drop(safe(PERSONAL_RULES))
def v_personal_clean():
    return (spark.readStream.table("v_personal_changes")  # noqa: F821
            .withWatermark("event_ts", DEDUP_WATERMARK)
            .dropDuplicatesWithinWatermark(["emplid", "lsn"]))


dp.create_streaming_table(
    name=f"{SILVER}.hr_personal_current",
    comment="Current personal data (SCD Type 1). History is deliberately not kept for PII; "
            "deletes implement right-to-erasure requests.",
    table_properties={"quality": "silver"},
    cluster_by=["emplid"],
)
dp.create_auto_cdc_flow(
    target=f"{SILVER}.hr_personal_current",
    source="v_personal_clean",
    keys=["emplid"],
    sequence_by=F.col("lsn"),
    apply_as_deletes=F.expr("op = 'd'"),
    except_column_list=["op", "event_ts"],
    stored_as_scd_type=1,
)


# ----------------------------------------------------------------------------- append-only facts
def flat_fact(entity: str, cols: dict):
    a = F.col("after")
    sel = [a.getField(c).cast(t).alias(c) for c, t in cols.items()]
    return (spark.readStream.option("skipChangeCommits", "true").table(f"{BRONZE}.hr_{entity}_raw")  # noqa: F821
            .select(*sel, (F.col("ts_ms") / 1000).cast("timestamp").alias("event_ts"),
                    F.col("lsn").cast("long").alias("lsn")))


@dp.table(name=f"{SILVER}.hr_absence", comment="Absence events (append-only, de-duplicated).",
          table_properties={"quality": "silver"})
@dp.expect_all_or_drop(safe({"valid_id": "absence_id IS NOT NULL",
                             "valid_emplid": "emplid IS NOT NULL",
                             "valid_days": "days BETWEEN 1 AND 365"}))
def hr_absence():
    return (flat_fact("absence", {"absence_id": "string", "emplid": "string", "absence_type": "string",
                                  "start_dt": "date", "days": "int"})
            .withWatermark("event_ts", DEDUP_WATERMARK)
            .dropDuplicatesWithinWatermark(["absence_id"]))


@dp.table(name=f"{SILVER}.hr_performance", comment="Performance review ratings (append-only).",
          table_properties={"quality": "silver"})
@dp.expect_all_or_drop(safe({"valid_id": "review_id IS NOT NULL",
                             "valid_rating": "rating BETWEEN 1 AND 5"}))
def hr_performance():
    return (flat_fact("performance", {"review_id": "string", "emplid": "string",
                                      "review_period": "string", "rating": "int"})
            .withWatermark("event_ts", DEDUP_WATERMARK)
            .dropDuplicatesWithinWatermark(["review_id"]))


@dp.table(name=f"{SILVER}.hr_pulse_survey", comment="Pulse survey responses (append-only).",
          table_properties={"quality": "silver"})
@dp.expect_all_or_drop(safe({"valid_id": "response_id IS NOT NULL",
                             "valid_engagement": "engagement_score BETWEEN 0 AND 10",
                             "valid_sentiment": "sentiment_score BETWEEN -1 AND 1"}))
def hr_pulse_survey():
    return (flat_fact("pulse_survey", {"response_id": "string", "emplid": "string",
                                       "survey_date": "date", "engagement_score": "int",
                                       "sentiment_score": "double"})
            .withWatermark("event_ts", DEDUP_WATERMARK)
            .dropDuplicatesWithinWatermark(["response_id"]))
