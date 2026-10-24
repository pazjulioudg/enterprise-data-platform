# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Platform setup (run ONCE in EACH workspace: DEV, then TEST, then PROD)
# MAGIC
# MAGIC **What it creates for this environment** (everything is idempotent - safe to re-run):
# MAGIC 1. Governed tags used by security policies (account level, created once)
# MAGIC 2. Three catalogs - `hr_<env>`, `finance_<env>`, `risk_<env>` - on the workspace's default storage
# MAGIC 3. Medallion schemas (bronze, silver, gold) + ops, governance, features/ml/sandbox where relevant
# MAGIC 4. Landing volumes (where source files arrive)
# MAGIC 5. Gold and ops tables as code (columns, comments, primary keys, clustering, governed tags)
# MAGIC 6. Security: HR regional PII masks, ORM row-level security and narrative masking (ABAC policies)
# MAGIC 7. Least-privilege grants to groups and to the environment's service principal
# MAGIC
# MAGIC **Before running:** groups and the service principal exist in the account console (guide steps 3-4).
# MAGIC **Compute:** serverless (the default in this workspace).

# COMMAND ----------

# STEP 0 - parameters: which environment this workspace is, and its service principal
dbutils.widgets.dropdown("env", "dev", ["dev", "test", "prod"], "Environment of THIS workspace")
dbutils.widgets.text("sp_app_id", "", "Service principal application ID for this environment")
ENV = dbutils.widgets.get("env")
SP = dbutils.widgets.get("sp_app_id").strip()
assert SP, "Fill in the sp_app_id widget first"
HR, FIN, RISK = f"hr_{ENV}", f"finance_{ENV}", f"risk_{ENV}"
EXISTS = ("ALREADY_EXISTS", "already exists")
# Identities never masked or filtered: admins, engineers (build the data), and the run-as service principal.
# Without the service principal here, derived tables would permanently store masked data.
EXEMPT = f"`edp-admins`, `edp-engineers`, `{SP}`"


def run(sql: str, ignore=()):
    """Run one SQL statement; ignore only the listed 'already exists' style errors."""
    try:
        spark.sql(sql)
    except Exception as e:  # noqa: BLE001
        if any(t in str(e) for t in ignore):
            print("skipped (already exists):", sql.strip().splitlines()[0][:90])
        else:
            raise

# COMMAND ----------

# MAGIC %md ## 1. Governed tags (account level; skipped if they already exist)

# COMMAND ----------

for name, desc, values in [
    ("pii", "Type of personal data in the column", "'national_id', 'dob', 'first_name', 'last_name'"),
    ("pii_context", "Column giving context for regional PII masking", "'country', 'id_type'"),
    ("rls_scope", "Column used for row-level security", "'entity', 'entity_conf'"),
    ("rls_conf", "Confidentiality column used for row-level security", "'confidentiality'"),
    ("sensitive_text", "Free text that may contain personal data", "'free_text'"),
]:
    run(f"CREATE GOVERNED TAG {name} DESCRIPTION '{desc}' VALUES ({values})", EXISTS)

# COMMAND ----------

# MAGIC %md ## 2-4. Catalogs, schemas and landing volumes

# COMMAND ----------

SCHEMAS = {
    "bronze": "Raw data exactly as received. Engineers only.",
    "silver": "Cleansed, conformed, de-duplicated data with history. Engineers only.",
    "gold": "Certified data products for BI, analytics and data science.",
    "ops": "Data-quality results, quarantine, maintenance and compliance logs.",
    "governance": "Masking and row-filter functions used by ABAC policies.",
}
EXTRA = {HR: ["features", "ml"] + (["sandbox"] if ENV == "dev" else []), FIN: [], RISK: []}
for cat, label in [(HR, "Human Resources"), (FIN, "Finance / market data"), (RISK, "Operational Risk Management")]:
    # Serverless workspaces back catalogs with default storage: no storage location is needed
    run(f"CREATE CATALOG IF NOT EXISTS {cat} COMMENT '{label} domain, {ENV.upper()} environment'")
    for schema, comment in SCHEMAS.items():
        run(f"CREATE SCHEMA IF NOT EXISTS {cat}.{schema} COMMENT '{comment}'")
    for schema in EXTRA[cat]:
        run(f"CREATE SCHEMA IF NOT EXISTS {cat}.{schema}")
    run(f"CREATE VOLUME IF NOT EXISTS {cat}.bronze.landing COMMENT 'Landing zone for source files'")
    run(f"ALTER CATALOG {cat} OWNER TO `edp-admins`")
print("catalogs, schemas and volumes ready")

# COMMAND ----------

# MAGIC %md ## 5. Gold and ops tables as code (HR, ORM)

# COMMAND ----------

GOLD_DDL = {
    "dim_employee": """
        employee_sk BIGINT NOT NULL COMMENT 'Surrogate key of the employee version',
        emplid STRING NOT NULL COMMENT 'HCM employee ID (business key)',
        first_name STRING COMMENT 'Given name',
        last_name STRING COMMENT 'Family name',
        birth_date DATE COMMENT 'Date of birth',
        country_code STRING COMMENT 'ISO country of employment (US, GB, IN)',
        national_id_type STRING COMMENT 'SSN, NINO, PAN or AADHAAR',
        national_id STRING COMMENT 'National identifier (masked by policy)',
        hire_date DATE COMMENT 'Original hire date',
        dept_id STRING COMMENT 'Department ID',
        job_code STRING COMMENT 'Job code',
        job_title STRING COMMENT 'Job title',
        location STRING COMMENT 'Work location',
        empl_status STRING COMMENT 'A = active, T = terminated, L = leave',
        annual_rt DECIMAL(18,2) COMMENT 'Annual salary in local currency',
        currency_cd STRING COMMENT 'Salary currency',
        manager_id STRING COMMENT 'Manager emplid',
        valid_from DATE COMMENT 'Version effective from (inclusive)',
        valid_to DATE COMMENT 'Version effective to (inclusive); 9999-12-31 = open',
        is_current BOOLEAN COMMENT 'True for the current version',
        _loaded_at TIMESTAMP COMMENT 'Load timestamp',
        CONSTRAINT dim_employee_pk PRIMARY KEY (employee_sk)""",
    "dim_department": """
        dept_id STRING NOT NULL COMMENT 'Department ID',
        dept_name STRING COMMENT 'Department name',
        cost_centre STRING COMMENT 'Finance cost centre',
        _loaded_at TIMESTAMP COMMENT 'Load timestamp',
        CONSTRAINT dim_department_pk PRIMARY KEY (dept_id)""",
    "fact_absence": """
        absence_id STRING NOT NULL COMMENT 'Absence event ID',
        employee_sk BIGINT COMMENT 'Employee version valid on start_dt',
        emplid STRING COMMENT 'HCM employee ID',
        dept_id STRING COMMENT 'Department at the time of absence',
        absence_type STRING COMMENT 'SICK, ANNUAL, PARENTAL, OTHER',
        start_dt DATE COMMENT 'First day of absence',
        days INT COMMENT 'Number of days absent',
        _loaded_at TIMESTAMP COMMENT 'Load timestamp'""",
    "fact_engagement": """
        response_id STRING NOT NULL COMMENT 'Pulse survey response ID',
        employee_sk BIGINT COMMENT 'Employee version valid on survey_date',
        emplid STRING COMMENT 'HCM employee ID',
        dept_id STRING COMMENT 'Department at the time of the survey',
        survey_date DATE COMMENT 'Survey date',
        engagement_score INT COMMENT 'Engagement 0-10',
        sentiment_score DOUBLE COMMENT 'Free-text sentiment -1..1',
        _loaded_at TIMESTAMP COMMENT 'Load timestamp'""",
}
GOLD_COMMENTS = {
    "dim_employee": "Certified SCD2 employee dimension. PII columns are masked by ABAC policy.",
    "dim_department": "Certified department dimension.",
    "fact_absence": "Certified absence facts (grain: one absence event).",
    "fact_engagement": "Certified pulse-survey facts (grain: one survey response).",
}
CLUSTER_KEYS = {"dim_employee": "emplid", "dim_department": "dept_id",
                "fact_absence": "start_dt", "fact_engagement": "survey_date"}
COLUMN_TAGS = {  # table -> {column: (tag, value)}
    "dim_employee": {
        "national_id": ("pii", "national_id"), "birth_date": ("pii", "dob"),
        "first_name": ("pii", "first_name"), "last_name": ("pii", "last_name"),
        "country_code": ("pii_context", "country"), "national_id_type": ("pii_context", "id_type"),
    },
}

CLUSTER_KEYS = {"dim_employee": "emplid", "dim_department": "dept_id",
                "fact_absence": "start_dt", "fact_engagement": "survey_date"}
for table, cols in GOLD_DDL.items():
    run(f"CREATE TABLE IF NOT EXISTS {HR}.gold.{table} ({cols}) CLUSTER BY ({CLUSTER_KEYS[table]}) "
        f"COMMENT '{GOLD_COMMENTS[table]}'")
for col, (tag, value) in {"national_id": ("pii", "national_id"), "birth_date": ("pii", "dob"),
                          "first_name": ("pii", "first_name"), "last_name": ("pii", "last_name"),
                          "country_code": ("pii_context", "country"),
                          "national_id_type": ("pii_context", "id_type")}.items():
    run(f"ALTER TABLE {HR}.gold.dim_employee ALTER COLUMN {col} SET TAGS ('{tag}' = '{value}')")

AMT = "DECIMAL(18,2)"
GOLD = {
    "fact_loss_event_position": ("Certified loss positions per event as at a date (EUR/GBP converted).", "event_id", [
        ("event_id", "STRING", "Canonical event ID"), ("source_system", "STRING", "GRC or LEGACY_LOSSLOG"),
        ("legal_entity_id", "STRING", "Legal entity"), ("org_unit_id", "STRING", "Organisation unit"),
        ("business_line_l1_code", "STRING", "Basel business line L1"), ("business_line_l2_code", "STRING", "Basel business line L2"),
        ("event_type_l1_code", "STRING", "Basel event type L1"), ("event_type_l2_code", "STRING", "Basel event type L2"),
        ("cause_code", "STRING", "Canonical cause"), ("occurrence_date", "DATE", "Date the event occurred"),
        ("discovery_date", "DATE", "Date the event was discovered"), ("first_accounting_date", "DATE", "First accounting date"),
        ("event_status", "STRING", "OPEN, UNDER_REVIEW, CLOSED"), ("boundary_type", "STRING", "NONE, CREDIT, MARKET"),
        ("is_near_miss", "BOOLEAN", "No financial impact"), ("is_operational_gain", "BOOLEAN", "Accidental gain"),
        ("is_rapidly_recovered", "BOOLEAN", "Fully recovered within days"), ("parent_event_id", "STRING", "Root event, if linked"),
        ("root_event_id", "STRING", "Event used for grouping (parent or self)"),
        ("inclusion_status", "STRING", "INCLUDED or EXCLUDED from the regulatory data set"),
        ("confidentiality", "STRING", "STANDARD or RESTRICTED"), ("currency", "STRING", "Transaction currency"),
        ("event_title", "STRING", "Title (free text, masked)"), ("event_description", "STRING", "Description (free text, masked)"),
        ("gross_loss_txn", AMT, "Gross loss, transaction currency"), ("recoveries_txn", AMT, "Recoveries, transaction currency"),
        ("net_loss_txn", AMT, "Net loss, transaction currency"), ("gross_loss_eur", AMT, "Gross loss, EUR"),
        ("recoveries_eur", AMT, "Recoveries, EUR"), ("net_loss_eur", AMT, "Net loss, EUR"), ("net_loss_gbp", AMT, "Net loss, GBP"),
        ("above_20k_eur", "BOOLEAN", "Net loss >= EUR 20,000"), ("above_100k_eur", "BOOLEAN", "Net loss >= EUR 100,000"),
        ("as_at_date", "DATE", "Position date"), ("_loaded_at", "TIMESTAMP", "Load timestamp")]),
    "mart_regulatory_loss_dataset": ("10-year annual net losses above EUR 20k / 100k (Basel loss-component input).", "accounting_year", [
        ("legal_entity_id", "STRING", "Legal entity"), ("threshold_eur", "INT", "Loss event threshold in EUR"),
        ("accounting_year", "INT", "Year of accounting date"), ("event_count", "BIGINT", "Root events above threshold"),
        ("gross_loss_eur", AMT, "Gross loss EUR"), ("recoveries_eur", AMT, "Recoveries EUR"), ("net_loss_eur", AMT, "Net loss EUR"),
        ("as_at_date", "DATE", "Position date"), ("_loaded_at", "TIMESTAMP", "Load timestamp")]),
    "mart_loss_matrix": ("Business line x event type loss matrix per reporting year.", "reporting_year", [
        ("legal_entity_id", "STRING", "Legal entity"), ("reporting_year", "INT", "Year of first accounting date"),
        ("business_line_l1_code", "STRING", "Basel business line L1"), ("event_type_l1_code", "STRING", "Basel event type L1"),
        ("event_count", "BIGINT", "Number of events"), ("gross_loss_eur", AMT, "Gross loss EUR"),
        ("recoveries_eur", AMT, "Recoveries EUR"), ("net_loss_eur", AMT, "Net loss EUR"), ("_loaded_at", "TIMESTAMP", "Load timestamp")]),
    "mart_kri_dashboard": ("KRI values with RAG status and trend.", "kri_id", [
        ("legal_entity_id", "STRING", "Legal entity"), ("org_unit_id", "STRING", "Organisation unit"), ("kri_id", "STRING", "KRI"),
        ("kri_name", "STRING", "KRI name"), ("risk_id", "STRING", "Linked risk"), ("period_end", "DATE", "Period end"),
        ("value", "DOUBLE", "Observed value"), ("amber_threshold", "DOUBLE", "Amber threshold"), ("red_threshold", "DOUBLE", "Red threshold"),
        ("direction", "STRING", "HIGHER_IS_WORSE or LOWER_IS_WORSE"), ("rag_status", "STRING", "RED, AMBER, GREEN"),
        ("prior_value", "DOUBLE", "Previous period value"), ("trend", "STRING", "WORSENING, IMPROVING, STABLE, NEW"),
        ("_loaded_at", "TIMESTAMP", "Load timestamp")]),
    "mart_residual_risk_heatmap": ("Latest approved RCSA per risk with control-test failures.", "risk_id", [
        ("legal_entity_id", "STRING", "Legal entity"), ("org_unit_id", "STRING", "Organisation unit"), ("risk_id", "STRING", "Risk"),
        ("risk_title", "STRING", "Risk title"), ("event_type_l1_code", "STRING", "Risk category (event type L1)"),
        ("assessment_period", "STRING", "RCSA period"), ("inherent_score", "INT", "Inherent likelihood x impact"),
        ("residual_score", "INT", "Residual likelihood x impact"), ("control_effectiveness", "STRING", "Assessed control effectiveness"),
        ("key_controls", "BIGINT", "Number of key controls"), ("failed_tests", "BIGINT", "Failed control tests (12 months)"),
        ("residual_rating", "STRING", "HIGH, MEDIUM, LOW"), ("_loaded_at", "TIMESTAMP", "Load timestamp")]),
    "mart_issue_ageing": ("Issues and actions with overdue ageing.", "issue_id", [
        ("legal_entity_id", "STRING", "Legal entity"), ("issue_id", "STRING", "Issue"), ("title", "STRING", "Issue title"),
        ("source", "STRING", "AUDIT, RCSA, LOSS_EVENT, REGULATOR, SELF_IDENTIFIED"), ("severity", "STRING", "Severity"),
        ("status", "STRING", "Status"), ("owner_emplid", "STRING", "Owner employee ID"), ("raised_date", "DATE", "Raised"),
        ("due_date", "DATE", "Due"), ("days_overdue", "INT", "Days past due (open issues)"),
        ("ageing_bucket", "STRING", "NOT_DUE, 1-30, 31-90, 90+, CLOSED"), ("is_overdue", "BOOLEAN", "Open and past due"),
        ("_loaded_at", "TIMESTAMP", "Load timestamp")]),
    "recon_loss_vs_gl": ("Loss movements reconciled to general-ledger postings per event and month.", "event_id", [
        ("legal_entity_id", "STRING", "Legal entity"), ("event_id", "STRING", "Event"), ("accounting_month", "DATE", "Month"),
        ("loss_amount", AMT, "Signed loss-data amount"), ("gl_amount", AMT, "Signed GL amount"), ("difference", AMT, "Loss minus GL"),
        ("recon_status", "STRING", "MATCHED, AMOUNT_BREAK, MISSING_IN_GL, MISSING_IN_LOSS"), ("currency", "STRING", "Currency"),
        ("_loaded_at", "TIMESTAMP", "Load timestamp")]),
}
for table, (comment, cluster, cols) in GOLD.items():
    body = ",\n".join(f"{n} {t} COMMENT '{c}'" for n, t, c in cols)
    run(f"CREATE TABLE IF NOT EXISTS {RISK}.gold.{table} ({body}) CLUSTER BY ({cluster}) COMMENT '{comment}'")
    scope = "entity_conf" if table == "fact_loss_event_position" else "entity"
    run(f"ALTER TABLE {RISK}.gold.{table} ALTER COLUMN legal_entity_id SET TAGS ('rls_scope' = '{scope}')")
run(f"ALTER TABLE {RISK}.gold.fact_loss_event_position ALTER COLUMN confidentiality SET TAGS ('rls_conf' = 'confidentiality')")
for col in ("event_title", "event_description"):
    run(f"ALTER TABLE {RISK}.gold.fact_loss_event_position ALTER COLUMN {col} SET TAGS ('sensitive_text' = 'free_text')")

# Ops tables: data-quality results (written by both DQ engines) and the GDPR request register
for cat in (HR, RISK):
    run(f"""CREATE TABLE IF NOT EXISTS {cat}.ops.dq_results (
              gate STRING, engine STRING, table_name STRING, check_name STRING, severity STRING, passed BOOLEAN,
              failed_count BIGINT, total_count BIGINT, threshold_pct DOUBLE, details STRING,
              run_id STRING, run_ts TIMESTAMP)
            COMMENT 'One row per data-quality rule per gate run'""")
run(f"""CREATE TABLE IF NOT EXISTS {HR}.ops.erasure_requests (
          request_id STRING, emplid STRING, received_date DATE, legal_basis STRING,
          status STRING, completed_ts TIMESTAMP, evidence STRING)
        COMMENT 'GDPR right-to-erasure request register (audit evidence)'""")
print("tables ready")

# COMMAND ----------

# MAGIC %md ## 6. Security policies (ABAC): HR regional PII masks, ORM row-level security and narrative masks

# COMMAND ----------

g = f"{HR}.governance"
cat = HR
run(f"""CREATE OR REPLACE FUNCTION {g}.mask_national_id(nid STRING, ctry STRING, idt STRING)
        RETURNS STRING
        COMMENT 'Regional national-ID mask: clear text only for the matching regional PII group'
        RETURN CASE
          WHEN nid IS NULL THEN NULL
          WHEN is_account_group_member('hr-pii-global') THEN nid
          WHEN ctry = 'US' AND idt = 'SSN' AND is_account_group_member('hr-pii-us') THEN nid
          WHEN ctry = 'GB' AND idt = 'NINO' AND is_account_group_member('hr-pii-uk') THEN nid
          WHEN ctry = 'IN' AND idt IN ('PAN', 'AADHAAR') AND is_account_group_member('hr-pii-in') THEN nid
          WHEN idt = 'SSN' THEN concat('***-**-', right(nid, 4))
          WHEN idt = 'NINO' THEN concat('******', right(nid, 3))
          WHEN idt = 'PAN' THEN concat('*****', substr(nid, 6, 4), '*')
          WHEN idt = 'AADHAAR' THEN concat('****-****-', right(nid, 4))
          ELSE '********'
        END""")
run(f"""CREATE OR REPLACE FUNCTION {g}.mask_dob(d DATE, ctry STRING)
        RETURNS DATE
        COMMENT 'Date of birth generalised to 1 January of the birth year unless authorised'
        RETURN CASE
          WHEN d IS NULL THEN NULL
          WHEN is_account_group_member('hr-pii-global') THEN d
          WHEN ctry = 'US' AND is_account_group_member('hr-pii-us') THEN d
          WHEN ctry = 'GB' AND is_account_group_member('hr-pii-uk') THEN d
          WHEN ctry = 'IN' AND is_account_group_member('hr-pii-in') THEN d
          ELSE make_date(year(d), 1, 1)
        END""")
run(f"""CREATE OR REPLACE FUNCTION {g}.mask_name(n STRING, ctry STRING)
        RETURNS STRING
        COMMENT 'Name redaction unless authorised'
        RETURN CASE
          WHEN n IS NULL THEN NULL
          WHEN is_account_group_member('hr-pii-global') THEN n
          WHEN ctry = 'US' AND is_account_group_member('hr-pii-us') THEN n
          WHEN ctry = 'GB' AND is_account_group_member('hr-pii-uk') THEN n
          WHEN ctry = 'IN' AND is_account_group_member('hr-pii-in') THEN n
          ELSE concat(left(n, 1), '***')
        END""")

policies = {
    "mask_national_id": ("mask_national_id", "has_tag_value('pii', 'national_id') AS nid, "
                         "has_tag_value('pii_context', 'country') AS ctry, has_tag_value('pii_context', 'id_type') AS idt",
                         "nid", "ctry, idt"),
    "mask_dob": ("mask_dob", "has_tag_value('pii', 'dob') AS dob, has_tag_value('pii_context', 'country') AS ctry", "dob", "ctry"),
    "mask_first_name": ("mask_name", "has_tag_value('pii', 'first_name') AS fn, has_tag_value('pii_context', 'country') AS ctry", "fn", "ctry"),
    "mask_last_name": ("mask_name", "has_tag_value('pii', 'last_name') AS ln, has_tag_value('pii_context', 'country') AS ctry", "ln", "ctry"),
}
for pname, (fn, match, on_col, using) in policies.items():
    run(f"""CREATE OR REPLACE POLICY {pname} ON CATALOG {HR}
            COMMENT 'Regional PII protection ({pname})'
            COLUMN MASK {g}.{fn}
            TO `account users` EXCEPT {EXEMPT}
            FOR TABLES MATCH COLUMNS {match} ON COLUMN {on_col} USING COLUMNS ({using})""")

G = f"{RISK}.governance"
run(f"""CREATE OR REPLACE FUNCTION {G}.rls_entity(le STRING)
        RETURNS BOOLEAN
        COMMENT 'True if the user may see rows of this legal entity'
        RETURN is_account_group_member('orm-global') OR is_account_group_member(concat('orm-le-', lower(le)))""")
run(f"""CREATE OR REPLACE FUNCTION {G}.rls_entity_conf(le STRING, conf STRING)
        RETURNS BOOLEAN
        COMMENT 'Legal-entity filter plus restricted events for Financial Crime only'
        RETURN (is_account_group_member('orm-global') OR is_account_group_member(concat('orm-le-', lower(le))))
               AND (conf <> 'RESTRICTED' OR is_account_group_member('orm-fincrime'))""")
run(f"""CREATE OR REPLACE FUNCTION {G}.mask_free_text(t STRING)
        RETURNS STRING
        COMMENT 'Redacts free-text narratives unless authorised'
        RETURN CASE WHEN t IS NULL THEN NULL
                    WHEN is_account_group_member('orm-narrative-readers') OR is_account_group_member('orm-fincrime') THEN t
                    ELSE '[REDACTED - narrative may contain personal data]' END""")
run(f"""CREATE OR REPLACE POLICY rls_legal_entity ON CATALOG {RISK}
        COMMENT 'Row-level security by legal entity'
        ROW FILTER {G}.rls_entity
        TO `account users` EXCEPT {EXEMPT}
        FOR TABLES
        MATCH COLUMNS has_tag_value('rls_scope', 'entity') AS le
        USING COLUMNS (le)""")
run(f"""CREATE OR REPLACE POLICY rls_legal_entity_confidential ON CATALOG {RISK}
        COMMENT 'Row-level security by legal entity and confidentiality'
        ROW FILTER {G}.rls_entity_conf
        TO `account users` EXCEPT {EXEMPT}
        FOR TABLES
        MATCH COLUMNS has_tag_value('rls_scope', 'entity_conf') AS le, has_tag_value('rls_conf', 'confidentiality') AS c
        USING COLUMNS (le, c)""")
run(f"""CREATE OR REPLACE POLICY mask_free_text ON CATALOG {RISK}
        COMMENT 'Redact free-text narratives'
        COLUMN MASK {G}.mask_free_text
        TO `account users` EXCEPT {EXEMPT}
        FOR TABLES
        MATCH COLUMNS has_tag_value('sensitive_text', 'free_text') AS t
        ON COLUMN t""")
print("ABAC policies ready")

# COMMAND ----------

# MAGIC %md ## 7. Grants (least privilege, always to groups or the service principal - never to individuals)

# COMMAND ----------

BUILD = ("USE SCHEMA, SELECT, MODIFY, CREATE TABLE, CREATE MATERIALIZED VIEW, CREATE FUNCTION, "
         "CREATE VOLUME, READ VOLUME, WRITE VOLUME, EXECUTE")
for cat in (HR, FIN, RISK):
    run(f"GRANT USE CATALOG, {BUILD} ON CATALOG {cat} TO `{SP}`")                  # pipelines and jobs
    if ENV == "dev":
        run(f"GRANT USE CATALOG, {BUILD} ON CATALOG {cat} TO `edp-engineers`")       # build in DEV
    elif ENV == "test":
        run(f"GRANT USE CATALOG, USE SCHEMA, SELECT, READ VOLUME ON CATALOG {cat} TO `edp-engineers`")
    run(f"GRANT USE CATALOG ON CATALOG {cat} TO `edp-analysts`")                     # analysts: Gold only
    run(f"GRANT USE SCHEMA, SELECT ON SCHEMA {cat}.gold TO `edp-analysts`")
run(f"GRANT USE CATALOG ON CATALOG {HR} TO `edp-data-scientists`")                    # data science
for s in ["gold", "features"]:
    run(f"GRANT USE SCHEMA, SELECT ON SCHEMA {HR}.{s} TO `edp-data-scientists`")
if ENV == "dev":
    run(f"GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA {HR}.sandbox TO `edp-data-scientists`")
for grp in ("orm-analysts", "orm-fincrime"):                                              # ORM consumers
    run(f"GRANT USE CATALOG ON CATALOG {RISK} TO `{grp}`")
    run(f"GRANT USE SCHEMA, SELECT ON SCHEMA {RISK}.gold TO `{grp}`")
print("grants applied")

# COMMAND ----------

# MAGIC %md ## 8. Check: policies attached to the protected tables

# COMMAND ----------

display(spark.sql(f"SHOW EFFECTIVE POLICIES ON TABLE {HR}.gold.dim_employee"))
display(spark.sql(f"SHOW EFFECTIVE POLICIES ON TABLE {RISK}.gold.fact_loss_event_position"))
