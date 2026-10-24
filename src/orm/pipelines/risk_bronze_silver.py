"""
ORM Bronze -> canonical Silver pipeline (Lakeflow Spark Declarative Pipelines, Python API).

Sources (landing volume /Volumes/<catalog>/bronze/landing/orm):
  grc/<entity>/   JSON lines, changed records only, each with last_modified_ts
  legacy/loss_log CSV from the acquired entity (UK dates, local categories)
  gl/postings     CSV general-ledger postings

Canonical Silver:
  org_unit, risk, control                         SCD Type 2 (AUTO CDC)
  loss_event                                      SCD Type 2 - keeps reclassification history
  risk_control, rcsa_assessment, control_test,
  kri_definition, kri_observation, issue_action   SCD Type 1 (AUTO CDC)
  loss_financial_movement, event_attribute,
  gl_posting                                      append-only, de-duplicated
  *_quarantine                                    rejected rows + failed rule names

Every source code is translated to a canonical code through silver.ref_crosswalk.
An unmapped code yields NULL and the row is quarantined, never silently dropped.

Pipeline configuration keys: edp.catalog (e.g. risk_dev), edp.landing_root.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("edp.catalog")  # noqa: F821
LANDING = spark.conf.get("edp.landing_root")  # noqa: F821
BRONZE, SILVER = f"{CATALOG}.bronze", f"{CATALOG}.silver"
DEDUP_WATERMARK = "7 days"
CURRENCIES = "('GBP', 'EUR', 'USD')"


# ----------------------------------------------------------------------------- helpers
def safe(rules: dict) -> dict:
    return {n: f"coalesce(({e}), false)" for n, e in rules.items()}


def with_failed_rules(df, rules: dict):
    checks = [F.when(~F.expr(e), F.lit(n)) for n, e in safe(rules).items()]
    return df.withColumn("failed_rules", F.filter(F.array(*checks), lambda x: x.isNotNull()))


def xwalk(df, source_system: str, domain: str, src_col: str, out_col: str):
    """Translate a source code to the canonical code (static lookup, broadcast)."""
    m = (spark.read.table(f"{SILVER}.ref_crosswalk")  # noqa: F821
         .where((F.col("source_system") == source_system) & (F.col("source_domain") == domain))
         .select(F.col("source_code").alias(f"_src_{out_col}"), F.col("canonical_code").alias(out_col)))
    return (df.join(F.broadcast(m), df[src_col] == m[f"_src_{out_col}"], "left")
              .drop(f"_src_{out_col}"))


def autoload(path: str, fmt: str):
    reader = (spark.readStream.format("cloudFiles")  # noqa: F821
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns"))
    if fmt == "json":
        reader = reader.option("cloudFiles.inferColumnTypes", "true")
    else:  # CSV: keep everything as strings in Bronze; type in Silver
        reader = reader.option("header", "true").option("cloudFiles.inferColumnTypes", "false")
    return (reader.load(path)
            .select("*", F.col("_metadata.file_path").alias("_source_file"),
                    F.current_timestamp().alias("_ingest_ts")))


def quarantine_and_clean(view_name: str, rules: dict, keys: list):
    """Register <entity>_quarantine (table) and v_<entity>_clean (view) for a changes view."""
    entity = view_name.replace("v_", "").replace("_changes", "")

    @dp.table(name=f"{SILVER}.{entity}_quarantine", comment=f"Rejected {entity} records with failed rule names.",
              table_properties={"quality": "quarantine"})
    def _q():
        df = with_failed_rules(spark.readStream.table(view_name), rules)  # noqa: F821
        return df.where(F.size("failed_rules") > 0).withColumn("quarantined_at", F.current_timestamp())

    @dp.temporary_view(name=f"v_{entity}_clean")
    @dp.expect_all_or_drop(safe(rules))
    def _c():
        return (spark.readStream.table(view_name)  # noqa: F821
                .withWatermark("_ingest_ts", DEDUP_WATERMARK)
                .dropDuplicatesWithinWatermark(keys))
    return f"v_{entity}_clean"


def cdc_target(entity: str, source: str, keys: list, scd: int, comment: str, track=None, cluster=None):
    dp.create_streaming_table(name=f"{SILVER}.{entity}", comment=comment,
                              table_properties={"quality": "silver"}, cluster_by=cluster or keys)
    kwargs = dict(target=f"{SILVER}.{entity}", source=source, keys=keys,
                  sequence_by=F.col("last_modified_ts"),
                  except_column_list=["_ingest_ts", "_source_file"], stored_as_scd_type=scd)
    if track:
        kwargs["track_history_column_list"] = track
    dp.create_auto_cdc_flow(**kwargs)


# ----------------------------------------------------------------------------- bronze
GRC_ENTITIES = ["org_units", "risks", "controls", "risk_controls", "rcsa_assessments", "control_tests",
                "kri_definitions", "kri_observations", "issues", "loss_events", "loss_movements",
                "event_attributes"]


def make_grc_bronze(entity: str):
    @dp.table(name=f"{BRONZE}.grc_{entity}_raw", comment=f"Raw GRC extract: {entity}",
              table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"})
    def _b():
        return autoload(f"{LANDING}/grc/{entity}/", "json")
    return _b


for _e in GRC_ENTITIES:
    make_grc_bronze(_e)

LEGACY_COLS = {"Incident Ref": "incident_ref", "Date Occurred": "date_occurred", "Date Found": "date_found",
               "Date Booked": "date_booked", "Category": "category", "Dept": "dept", "Loss GBP": "loss_gbp",
               "Recovered GBP": "recovered_gbp", "Description": "description", "Status": "status"}


@dp.table(name=f"{BRONZE}.legacy_loss_log_raw", comment="Raw legacy loss log of the acquired entity (LE04).",
          table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"})
def legacy_loss_log_raw():
    df = autoload(f"{LANDING}/legacy/loss_log/", "csv")
    # Delta column names cannot contain spaces: normalise to snake_case in Bronze
    return df.select(*[F.col(f"`{src}`").alias(dst) for src, dst in LEGACY_COLS.items()],
                     "_source_file", "_ingest_ts")


@dp.table(name=f"{BRONZE}.gl_postings_raw", comment="Raw general-ledger postings to operational-loss accounts.",
          table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"})
def gl_postings_raw():
    return autoload(f"{LANDING}/gl/postings/", "csv")


def grc(entity: str):
    return spark.readStream.table(f"{BRONZE}.grc_{entity}_raw")  # noqa: F821


LM = F.to_timestamp("last_modified_ts").alias("last_modified_ts")
META = [F.col("_ingest_ts"), F.col("_source_file")]

# ----------------------------------------------------------------------------- org units (SCD2)


@dp.temporary_view(name="v_org_unit_changes")
def v_org_unit_changes():
    df = xwalk(grc("org_units"), "GRC", "business_line", "business_line_code", "business_line_l2_code")
    return df.select("org_unit_id", "legal_entity_id", "division", "business_unit", "cost_centre",
                     "business_line_l2_code", LM, *META)


cdc_target("org_unit", quarantine_and_clean("v_org_unit_changes", {
    "valid_key": "org_unit_id IS NOT NULL", "mapped_business_line": "business_line_l2_code IS NOT NULL",
    "valid_entity": "legal_entity_id IN ('LE01', 'LE02', 'LE03', 'LE04')"}, ["org_unit_id", "last_modified_ts"]),
    ["org_unit_id"], 2, "Organisation units with history (reorganisations).",
    track=["legal_entity_id", "division", "business_unit", "cost_centre", "business_line_l2_code"])

# ----------------------------------------------------------------------------- risks and controls (SCD2)


@dp.temporary_view(name="v_risk_changes")
def v_risk_changes():
    df = xwalk(grc("risks"), "GRC", "event_type", "risk_category_code", "risk_event_type_l2_code")
    return df.select("risk_id", "title", "risk_event_type_l2_code", "org_unit_id", "owner_emplid", "status",
                     LM, *META)


cdc_target("risk", quarantine_and_clean("v_risk_changes", {
    "valid_key": "risk_id IS NOT NULL", "mapped_category": "risk_event_type_l2_code IS NOT NULL",
    "valid_status": "status IN ('ACTIVE', 'RETIRED')"}, ["risk_id", "last_modified_ts"]),
    ["risk_id"], 2, "Risk register with history.",
    track=["title", "risk_event_type_l2_code", "org_unit_id", "owner_emplid", "status"])


@dp.temporary_view(name="v_control_changes")
def v_control_changes():
    return grc("controls").select("control_id", "description", "control_type", "nature", "frequency",
                                  F.col("is_key_control").cast("boolean").alias("is_key_control"),
                                  "owner_emplid", "status", LM, *META)


cdc_target("control", quarantine_and_clean("v_control_changes", {
    "valid_key": "control_id IS NOT NULL",
    "valid_type": "control_type IN ('PREVENTIVE', 'DETECTIVE', 'CORRECTIVE')",
    "valid_nature": "nature IN ('MANUAL', 'AUTOMATED')"}, ["control_id", "last_modified_ts"]),
    ["control_id"], 2, "Control library with history.",
    track=["description", "control_type", "nature", "frequency", "is_key_control", "owner_emplid", "status"])

# ----------------------------------------------------------------------------- SCD1 entities


@dp.temporary_view(name="v_risk_control_changes")
def v_risk_control_changes():
    return grc("risk_controls").select("risk_id", "control_id", LM, *META)


cdc_target("risk_control", quarantine_and_clean("v_risk_control_changes", {
    "valid_keys": "risk_id IS NOT NULL AND control_id IS NOT NULL"},
    ["risk_id", "control_id", "last_modified_ts"]), ["risk_id", "control_id"], 1,
    "Many-to-many link between risks and the controls that mitigate them.")


@dp.temporary_view(name="v_rcsa_assessment_changes")
def v_rcsa_assessment_changes():
    ints = ["inherent_likelihood", "inherent_impact", "residual_likelihood", "residual_impact"]
    return grc("rcsa_assessments").select("assessment_id", "risk_id", "assessment_period",
                                          *[F.col(c).cast("int").alias(c) for c in ints],
                                          "control_effectiveness", "assessor_emplid", "approval_status", LM, *META)


cdc_target("rcsa_assessment", quarantine_and_clean("v_rcsa_assessment_changes", {
    "valid_key": "assessment_id IS NOT NULL",
    "valid_scale": "inherent_likelihood BETWEEN 1 AND 5 AND inherent_impact BETWEEN 1 AND 5 "
                   "AND residual_likelihood BETWEEN 1 AND 5 AND residual_impact BETWEEN 1 AND 5",
    "residual_not_above_inherent": "residual_likelihood * residual_impact <= inherent_likelihood * inherent_impact",
    "valid_effectiveness": "control_effectiveness IN ('EFFECTIVE', 'PARTIALLY_EFFECTIVE', 'INEFFECTIVE')"},
    ["assessment_id", "last_modified_ts"]), ["assessment_id"], 1,
    "Risk and control self-assessments (latest state per assessment).")


@dp.temporary_view(name="v_control_test_changes")
def v_control_test_changes():
    return grc("control_tests").select("test_id", "control_id", F.to_date("test_date").alias("test_date"),
                                       "tester_emplid", "design_effectiveness", "operating_effectiveness",
                                       F.col("sample_size").cast("int").alias("sample_size"),
                                       F.col("exceptions").cast("int").alias("exceptions"), LM, *META)


cdc_target("control_test", quarantine_and_clean("v_control_test_changes", {
    "valid_key": "test_id IS NOT NULL",
    "valid_results": "design_effectiveness IN ('PASS', 'FAIL') AND operating_effectiveness IN ('PASS', 'FAIL')",
    "operating_needs_design": "NOT (design_effectiveness = 'FAIL' AND operating_effectiveness = 'PASS')"},
    ["test_id", "last_modified_ts"]), ["test_id"], 1, "Control test results.")


@dp.temporary_view(name="v_kri_definition_changes")
def v_kri_definition_changes():
    return grc("kri_definitions").select("kri_id", "name", "risk_id", "unit", "frequency",
                                         F.col("amber_threshold").cast("double").alias("amber_threshold"),
                                         F.col("red_threshold").cast("double").alias("red_threshold"),
                                         "direction", LM, *META)


cdc_target("kri_definition", quarantine_and_clean("v_kri_definition_changes", {
    "valid_key": "kri_id IS NOT NULL",
    "valid_direction": "direction IN ('HIGHER_IS_WORSE', 'LOWER_IS_WORSE')",
    "consistent_thresholds": "(direction = 'HIGHER_IS_WORSE' AND red_threshold > amber_threshold) OR "
                             "(direction = 'LOWER_IS_WORSE' AND red_threshold < amber_threshold)"},
    ["kri_id", "last_modified_ts"]), ["kri_id"], 1, "Key Risk Indicator definitions and thresholds.")


@dp.temporary_view(name="v_kri_observation_changes")
def v_kri_observation_changes():
    return grc("kri_observations").select("kri_id", F.to_date("period_end").alias("period_end"),
                                          F.col("value").cast("double").alias("value"), LM, *META)


cdc_target("kri_observation", quarantine_and_clean("v_kri_observation_changes", {
    "valid_keys": "kri_id IS NOT NULL AND period_end IS NOT NULL", "valid_value": "value >= 0"},
    ["kri_id", "period_end", "last_modified_ts"]), ["kri_id", "period_end"], 1, "KRI values per period.")


@dp.temporary_view(name="v_issue_action_changes")
def v_issue_action_changes():
    return grc("issues").select("issue_id", "title", "source", "severity", "event_id", "risk_id", "control_id",
                                "owner_emplid", F.to_date("raised_date").alias("raised_date"),
                                F.to_date("due_date").alias("due_date"), "status",
                                F.to_date("closed_date").alias("closed_date"), LM, *META)


cdc_target("issue_action", quarantine_and_clean("v_issue_action_changes", {
    "valid_key": "issue_id IS NOT NULL",
    "valid_severity": "severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')",
    "closed_has_date": "status <> 'CLOSED' OR closed_date IS NOT NULL"},
    ["issue_id", "last_modified_ts"]), ["issue_id"], 1, "Issues and remediation actions.")

# ----------------------------------------------------------------------------- loss events (SCD2, two sources)
EVENT_COLS = ["event_id", "source_system", "source_event_id", "event_title", "event_description",
              "legal_entity_id", "org_unit_id", "business_line_l2_code", "event_type_l2_code", "cause_code",
              "occurrence_date", "discovery_date", "first_accounting_date", "event_status", "boundary_type",
              "is_near_miss", "is_operational_gain", "is_rapidly_recovered", "parent_event_id",
              "inclusion_status", "exclusion_reason", "confidentiality", "reported_by_emplid",
              "owner_emplid", "currency", "last_modified_ts", "_ingest_ts", "_source_file"]


def grc_events():
    df = grc("loss_events")
    df = xwalk(df, "GRC", "event_type", "event_type_code", "event_type_l2_code")
    df = xwalk(df, "GRC", "business_line", "business_line_code", "business_line_l2_code")
    df = xwalk(df, "GRC", "cause", "cause_code", "cause_canonical")
    return df.select(
        "event_id", F.lit("GRC").alias("source_system"), F.col("event_id").alias("source_event_id"),
        "event_title", "event_description", "legal_entity_id", "org_unit_id", "business_line_l2_code",
        "event_type_l2_code", F.col("cause_canonical").alias("cause_code"),
        F.to_date("occurrence_date").alias("occurrence_date"), F.to_date("discovery_date").alias("discovery_date"),
        F.to_date("first_accounting_date").alias("first_accounting_date"), "event_status", "boundary_type",
        *[F.col(c).cast("boolean").alias(c) for c in ("is_near_miss", "is_operational_gain", "is_rapidly_recovered")],
        "parent_event_id", "inclusion_status", "exclusion_reason", "confidentiality", "reported_by_emplid",
        "owner_emplid", "currency", LM, *META)


def legacy_events():
    df = spark.readStream.table(f"{BRONZE}.legacy_loss_log_raw")  # noqa: F821
    df = xwalk(df, "LEGACY_LOSSLOG", "event_type", "category", "event_type_l2_code")
    df = xwalk(df, "LEGACY_LOSSLOG", "business_line", "dept", "business_line_l2_code")
    df = xwalk(df, "LEGACY_LOSSLOG", "org_unit", "dept", "org_unit_id")
    d = lambda c: F.to_date(F.col(c), "dd/MM/yyyy")  # noqa: E731  (UK date format in the legacy log)
    return df.select(
        F.concat(F.lit("LEG-"), "incident_ref").alias("event_id"), F.lit("LEGACY_LOSSLOG").alias("source_system"),
        F.col("incident_ref").alias("source_event_id"), F.col("category").alias("event_title"),
        F.col("description").alias("event_description"), F.lit("LE04").alias("legal_entity_id"), "org_unit_id",
        "business_line_l2_code", "event_type_l2_code", F.lit(None).cast("string").alias("cause_code"),
        d("date_occurred").alias("occurrence_date"), d("date_found").alias("discovery_date"),
        d("date_booked").alias("first_accounting_date"), F.upper("status").alias("event_status"),
        F.lit("NONE").alias("boundary_type"), F.lit(False).alias("is_near_miss"),
        F.lit(False).alias("is_operational_gain"), F.lit(False).alias("is_rapidly_recovered"),
        F.lit(None).cast("string").alias("parent_event_id"), F.lit("INCLUDED").alias("inclusion_status"),
        F.lit(None).cast("string").alias("exclusion_reason"),
        F.when(F.col("event_type_l2_code").startswith("IF"), "RESTRICTED").otherwise("STANDARD").alias("confidentiality"),
        F.lit(None).cast("string").alias("reported_by_emplid"), F.lit(None).cast("string").alias("owner_emplid"),
        F.lit("GBP").alias("currency"), F.to_timestamp(d("date_booked")).alias("last_modified_ts"), *META)


@dp.temporary_view(name="v_loss_event_changes", comment="GRC and legacy loss events mapped to the canonical model.")
def v_loss_event_changes():
    return grc_events().select(*EVENT_COLS).unionByName(legacy_events().select(*EVENT_COLS))


LOSS_EVENT_RULES = {
    "valid_key": "event_id IS NOT NULL",
    "mapped_event_type": "event_type_l2_code IS NOT NULL",
    "mapped_business_line": "business_line_l2_code IS NOT NULL",
    "mapped_org_unit": "org_unit_id IS NOT NULL",
    "mapped_cause_for_grc": "source_system <> 'GRC' OR cause_code IS NOT NULL",
    "discovery_not_before_occurrence": "discovery_date >= occurrence_date",
    "accounting_not_before_occurrence": "first_accounting_date IS NULL OR first_accounting_date >= occurrence_date",
    "valid_confidentiality": "confidentiality IN ('STANDARD', 'RESTRICTED')",
    "valid_boundary": "boundary_type IN ('NONE', 'CREDIT', 'MARKET')",
    "valid_currency": f"currency IN {CURRENCIES}",
}
cdc_target("loss_event", quarantine_and_clean("v_loss_event_changes", LOSS_EVENT_RULES,
                                              ["event_id", "last_modified_ts"]),
           ["event_id"], 2, "Canonical loss events (SCD2: reclassifications and status changes are history).",
           track=["event_type_l2_code", "business_line_l2_code", "org_unit_id", "cause_code", "event_status",
                  "boundary_type", "inclusion_status", "confidentiality", "parent_event_id"],
           cluster=["event_id"])

# ----------------------------------------------------------------------------- movements, attributes, GL (append)
MOVE_TYPES = "('GROSS_LOSS', 'PROVISION', 'PROVISION_RELEASE', 'RECOVERY_INSURANCE', 'RECOVERY_OTHER')"


@dp.temporary_view(name="v_loss_financial_movement_changes")
def v_loss_financial_movement_changes():
    g = grc("loss_movements").select(
        "movement_id", "event_id", "movement_type", "effect_type_code",
        F.col("amount").cast("decimal(18,2)").alias("amount"), "currency",
        F.to_date("accounting_date").alias("accounting_date"), "gl_account", "cost_centre", *META)
    leg = spark.readStream.table(f"{BRONZE}.legacy_loss_log_raw")  # noqa: F821
    booked = F.to_date("date_booked", "dd/MM/yyyy")
    base = leg.select(F.concat(F.lit("LEG-"), "incident_ref").alias("event_id"), booked.alias("accounting_date"),
                      F.col("loss_gbp").cast("decimal(18,2)").alias("loss"),
                      F.col("recovered_gbp").cast("decimal(18,2)").alias("rec"), *META)
    gross = base.select(F.concat("event_id", F.lit("-G")).alias("movement_id"), "event_id",
                        F.lit("GROSS_LOSS").alias("movement_type"), F.lit(None).cast("string").alias("effect_type_code"),
                        F.col("loss").alias("amount"), F.lit("GBP").alias("currency"), "accounting_date",
                        F.lit("640100").alias("gl_account"), F.lit(None).cast("string").alias("cost_centre"), *META)
    rec = (base.where("rec > 0")
           .select(F.concat("event_id", F.lit("-R")).alias("movement_id"), "event_id",
                   F.lit("RECOVERY_OTHER").alias("movement_type"), F.lit(None).cast("string").alias("effect_type_code"),
                   F.col("rec").alias("amount"), F.lit("GBP").alias("currency"), "accounting_date",
                   F.lit("640910").alias("gl_account"), F.lit(None).cast("string").alias("cost_centre"), *META))
    return g.unionByName(gross).unionByName(rec)


MOVEMENT_RULES = {
    "valid_keys": "movement_id IS NOT NULL AND event_id IS NOT NULL",
    "valid_type": f"movement_type IN {MOVE_TYPES}",
    "positive_amount": "amount > 0",
    "has_accounting_date": "accounting_date IS NOT NULL",
    "valid_currency": f"currency IN {CURRENCIES}",
}
_clean_moves = quarantine_and_clean("v_loss_financial_movement_changes", MOVEMENT_RULES, ["movement_id"])


@dp.table(name=f"{SILVER}.loss_financial_movement",
          comment="Every money movement on every loss event (gross, provisions, releases, recoveries). Append-only.",
          table_properties={"quality": "silver"}, cluster_by=["event_id"])
def loss_financial_movement():
    return spark.readStream.table(_clean_moves).drop("_source_file")  # noqa: F821


@dp.table(name=f"{SILVER}.event_attribute", comment="Event to attribute links (cyber, third-party, conduct, ESG, large loss).",
          table_properties={"quality": "silver"})
@dp.expect_all_or_drop(safe({"valid_keys": "event_id IS NOT NULL AND attribute_code IS NOT NULL"}))
def event_attribute():
    return (grc("event_attributes").select("event_id", "attribute_code", LM, F.col("_ingest_ts"))
            .withWatermark("_ingest_ts", DEDUP_WATERMARK)
            .dropDuplicatesWithinWatermark(["event_id", "attribute_code"]))


@dp.table(name=f"{SILVER}.gl_posting", comment="General-ledger postings to operational-loss accounts.",
          table_properties={"quality": "silver"}, cluster_by=["event_ref"])
@dp.expect_all_or_drop(safe({"valid_key": "journal_id IS NOT NULL", "has_amount": "amount IS NOT NULL",
                             "has_event_ref": "event_ref <> ''"}))
def gl_posting():
    return (spark.readStream.table(f"{BRONZE}.gl_postings_raw")  # noqa: F821
            .select("journal_id", F.to_date("posting_date").alias("posting_date"), "gl_account", "cost_centre",
                    F.col("amount").cast("decimal(18,2)").alias("amount"), "currency", "narrative",
                    F.regexp_extract("narrative", r"OPL (\S+)", 1).alias("event_ref"), F.col("_ingest_ts"))
            .withWatermark("_ingest_ts", DEDUP_WATERMARK)
            .dropDuplicatesWithinWatermark(["journal_id"]))
