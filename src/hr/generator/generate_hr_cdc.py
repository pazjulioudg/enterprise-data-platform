"""
Synthetic HCM (PeopleSoft/Oracle HCM-style) change-data-capture generator.

Writes Debezium-style JSON change events into the HR landing volume:
    <output_root>/<entity>/batch_<NNNN>/part-*.json

Envelope per event:
    {"op": "r|c|u|d", "ts_ms": <long>, "lsn": <long>,
     "source": {"db": "hcm", "table": "<PS_TABLE>"},
     "before": {...} | null, "after": {...} | null}

Batch 0  = initial snapshot (op 'r') + reference data tables.
Batch 1+ = incremental changes: promotions/pay changes, terminations,
           new hires, GDPR erasures (deletes), plus deliberately injected
           data-quality problems for testing:
             * duplicate events (same key + same lsn)
             * malformed events (null key, negative salary, bad currency)
             * schema drift (new field 'work_mode' on job events from batch 2)

Run as a Databricks job task (spark_python_task) or locally for testing.
All randomness is deterministic (hash-based) so runs are reproducible.
"""
import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

FIRST_NAMES = ["James", "Olivia", "Arjun", "Priya", "Liam", "Amelia", "Rahul", "Ananya",
               "Noah", "Isla", "Vikram", "Sophie", "Ethan", "Grace", "Rohan", "Emily"]
LAST_NAMES = ["Smith", "Patel", "Jones", "Sharma", "Brown", "Iyer", "Taylor", "Reddy",
              "Wilson", "Gupta", "Evans", "Nair", "Walker", "Rao", "Wright", "Khan"]
JOB_CODES = ["J100", "J110", "J120", "J200", "J210", "J300", "J310", "J400", "J410",
             "J500", "J510", "J600"]
JOB_TITLES = ["Analyst", "Senior Analyst", "Lead Analyst", "Engineer", "Senior Engineer",
              "Manager", "Senior Manager", "Specialist", "Senior Specialist",
              "Consultant", "Principal Consultant", "Director"]
DEPARTMENTS = [(f"D{i:03d}", n) for i, n in enumerate(
    ["Operations", "Finance", "HR", "Technology", "Risk", "Compliance", "Sales",
     "Marketing", "Legal", "Procurement", "Treasury", "Audit", "Data", "Facilities",
     "Customer Service", "Product", "Strategy", "Security", "Payroll", "Learning"], start=1)]


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Synthetic HCM CDC generator")
    p.add_argument("--catalog", required=True, help="Target catalog, e.g. hr_dev")
    p.add_argument("--batch-id", type=int, default=0, help="0 = initial snapshot")
    p.add_argument("--employees", type=int, default=20000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--output-root", default=None,
                   help="Defaults to /Volumes/<catalog>/bronze/landing/hr")
    p.add_argument("--write-reference-tables", default="true",
                   help="'true' to (re)write bronze reference tables on batch 0")
    return p.parse_args(argv)


def rnd(seed: int, salt: str, *cols) -> F.Column:
    """Deterministic pseudo-random number in [0, 1) derived from the given columns."""
    h = F.xxhash64(*cols, F.lit(salt), F.lit(seed))
    return (F.pmod(h, F.lit(1_000_000)) / F.lit(1_000_000.0)).cast("double")


def pick(arr, r: F.Column) -> F.Column:
    """Pick an element of a Python list using a [0,1) random column."""
    arr_col = F.array(*[F.lit(x) for x in arr])
    idx = (F.floor(r * F.lit(len(arr))) + F.lit(1)).cast("int")
    return F.element_at(arr_col, idx)


def employees_base(spark: SparkSession, start_id: int, end_id: int, seed: int) -> DataFrame:
    """Deterministic employee master attributes for ids in [start_id, end_id]."""
    df = spark.range(start_id, end_id + 1).withColumnRenamed("id", "n")
    df = df.withColumn("emplid", F.format_string("E%07d", F.col("n")))
    r_c = rnd(seed, "country", F.col("n"))
    df = df.withColumn("country_code",
                       F.when(r_c < 0.40, "US").when(r_c < 0.75, "GB").otherwise("IN"))
    r_t = rnd(seed, "idtype", F.col("n"))
    df = df.withColumn(
        "national_id_type",
        F.when(F.col("country_code") == "US", "SSN")
         .when(F.col("country_code") == "GB", "NINO")
         .when(r_t < 0.5, "PAN").otherwise("AADHAAR"))
    d = lambda salt, mod: F.pmod(F.xxhash64(F.col("n"), F.lit(salt), F.lit(seed)), F.lit(mod))
    letters = F.array(*[F.lit(c) for c in "ABCDEFGHJKLMNPRSTWXYZ"])
    letter = lambda salt: F.element_at(letters, (d(salt, 21) + 1).cast("int"))
    df = df.withColumn(
        "national_id",
        F.when(F.col("national_id_type") == "SSN",  # 9xx area numbers are never issued
               F.format_string("9%02d-%02d-%04d", d("s1", 100), d("s2", 100), d("s3", 10000)))
         .when(F.col("national_id_type") == "NINO",  # QQ prefix is reserved for examples
               F.concat(F.lit("QQ"), F.format_string("%06d", d("n1", 1000000)),
                        F.element_at(F.array(*[F.lit(c) for c in "ABCD"]),
                                     (d("n2", 4) + 1).cast("int"))))
         .when(F.col("national_id_type") == "PAN",
               F.concat(letter("p1"), letter("p2"), letter("p3"), F.lit("P"), letter("p5"),
                        F.format_string("%04d", d("p6", 10000)), letter("p7")))
         .otherwise(F.format_string("9%03d-%04d-%04d", d("a1", 1000), d("a2", 10000),
                                    d("a3", 10000))))
    df = (df.withColumn("first_name", pick(FIRST_NAMES, rnd(seed, "fn", F.col("n"))))
            .withColumn("last_name", pick(LAST_NAMES, rnd(seed, "ln", F.col("n"))))
            .withColumn("birth_date", F.expr(
                f"date_add(DATE'1962-01-01', CAST(pmod(xxhash64(n, 'dob', {seed}), 14600) AS INT))"))
            .withColumn("hire_date", F.expr(
                f"date_add(DATE'2008-01-01', CAST(pmod(xxhash64(n, 'hire', {seed}), 6200) AS INT))")))
    # Deliberate skew: 45% of employees sit in department D001 (Operations)
    r_d = rnd(seed, "dept", F.col("n"))
    df = df.withColumn("dept_id", F.when(r_d < 0.45, F.lit("D001")).otherwise(
        F.format_string("D%03d", (F.floor((r_d - 0.45) / 0.55 * 19) + 2).cast("int"))))
    r_j = rnd(seed, "job", F.col("n"))
    df = df.withColumn("job_code", pick(JOB_CODES, r_j)).withColumn("job_title", pick(JOB_TITLES, r_j))
    r_s = rnd(seed, "sal", F.col("n"))
    df = (df.withColumn("currency_cd", F.when(F.col("country_code") == "US", "USD")
                        .when(F.col("country_code") == "GB", "GBP").otherwise("INR"))
            .withColumn("annual_rt", F.round(
                F.when(F.col("country_code") == "US", F.lit(50000) + r_s * 130000)
                 .when(F.col("country_code") == "GB", F.lit(30000) + r_s * 90000)
                 .otherwise(F.lit(600000) + r_s * 3900000), 2).cast("decimal(18,2)"))
            .withColumn("location", F.when(F.col("country_code") == "US", "New York")
                        .when(F.col("country_code") == "GB", "London").otherwise("Bengaluru"))
            .withColumn("manager_id", F.when(F.col("n") > 500,
                        F.format_string("E%07d", F.pmod(F.col("n"), F.lit(500)) + 1)))
            .withColumn("empl_status", F.lit("A")))
    return df


PERSONAL_COLS = ["emplid", "first_name", "last_name", "birth_date", "country_code",
                 "national_id_type", "national_id", "hire_date"]
JOB_COLS = ["emplid", "effdt", "effseq", "dept_id", "job_code", "job_title", "location",
            "empl_status", "annual_rt", "currency_cd", "manager_id"]


def envelope(df: DataFrame, op_col: F.Column, cols, table: str, batch_id: int,
             lsn_offset: int = 0, with_before_key_only: bool = False) -> DataFrame:
    """Wrap rows in a Debezium-style envelope.

    lsn = batch_id * 1e9 + n * 10 + lsn_offset  -> unique per (employee, event kind) and
    monotonically increasing across batches, mimicking a database log sequence number.
    """
    after = F.struct(*[F.col(c) for c in cols])
    after_type = df.select(after.alias("a")).schema["a"].dataType
    null_struct = F.lit(None).cast(after_type)
    out = df.withColumn("op", op_col)
    out = out.withColumn("after", F.when(F.col("op") == "d", null_struct).otherwise(after))
    if with_before_key_only:
        before = F.struct(*[(F.col(c) if c == "emplid" else
                             F.lit(None).cast(df.schema[c].dataType)).alias(c) for c in cols])
        out = out.withColumn("before", F.when(F.col("op") == "d", before).otherwise(null_struct))
    else:
        out = out.withColumn("before", null_struct)
    base_ts = 1767225600000 + batch_id * 86_400_000  # 2026-01-01T00:00:00Z + one day per batch
    lsn = (F.lit(batch_id).cast("long") * F.lit(1_000_000_000)
           + F.col("n").cast("long") * F.lit(10) + F.lit(lsn_offset))
    return out.select(
        "op",
        (F.lit(base_ts) + F.pmod(F.col("n") * F.lit(7919), F.lit(3_600_000))).cast("long").alias("ts_ms"),
        lsn.alias("lsn"),
        F.struct(F.lit("hcm").alias("db"), F.lit(table).alias("table")).alias("source"),
        "before", "after")


def write_events(df: DataFrame, root: str, entity: str, batch_id: int) -> None:
    path = f"{root}/{entity}/batch_{batch_id:04d}"
    df.coalesce(4).write.mode("overwrite").json(path)
    print(f"wrote {entity:>14} -> {path}")


def generate(spark: SparkSession, args) -> None:
    root = args.output_root or f"/Volumes/{args.catalog}/bronze/landing/hr"
    seed, n, b = args.seed, args.employees, args.batch_id

    if b == 0:
        emp = employees_base(spark, 1, n, seed).withColumn("effdt", F.col("hire_date")) \
                                               .withColumn("effseq", F.lit(0))
        write_events(envelope(emp, F.lit("r"), PERSONAL_COLS, "PS_PERSONAL_DATA", b),
                     root, "personal_data", b)
        write_events(envelope(emp, F.lit("r"), JOB_COLS, "PS_JOB", b), root, "job", b)
        perf = (emp.select("emplid", "n")
                   .withColumn("review_period", F.lit("FY2025"))
                   .withColumn("rating", (F.floor(rnd(seed, "perf", F.col("n")) * 5) + 1).cast("int"))
                   .withColumn("review_id", F.concat(F.lit("R25-"), F.col("emplid"))))
        write_events(envelope(perf, F.lit("c"), ["review_id", "emplid", "review_period", "rating"],
                              "PS_EP_REVIEW", b), root, "performance", b)
        if args.write_reference_tables.lower() == "true":
            write_reference_tables(spark, args.catalog)
    else:
        # Existing population changes (deterministic per batch)
        emp = employees_base(spark, 1, n, seed)
        r = rnd(seed, f"chg{b}", F.col("n"))
        batch_date = F.expr(f"date_add(DATE'2026-01-01', {b})")
        changed = (emp.where(r < 0.04)
                   .withColumn("annual_rt", (F.col("annual_rt") * (F.lit(1.03) + r * 2)).cast("decimal(18,2)"))
                   .withColumn("job_title", F.when(r < 0.01, F.concat(F.lit("Senior "), F.col("job_title")))
                               .otherwise(F.col("job_title")))
                   .withColumn("dept_id", F.when(r < 0.005, F.lit("D013")).otherwise(F.col("dept_id"))))
        terminated = emp.where((r >= 0.04) & (r < 0.045)).withColumn("empl_status", F.lit("T"))
        upd = changed.unionByName(terminated).withColumn("effdt", batch_date) \
                     .withColumn("effseq", F.lit(0))
        # New hires
        start = n + (b - 1) * max(1, n // 100) + 1
        hires = employees_base(spark, start, start + max(1, n // 100) - 1, seed) \
            .withColumn("hire_date", batch_date).withColumn("effdt", batch_date) \
            .withColumn("effseq", F.lit(0))

        job_cols = list(JOB_COLS)
        if b >= 2:  # schema drift: upstream adds a new attribute
            upd = upd.withColumn("work_mode", F.when(r < 0.02, "Hybrid").otherwise("Office"))
            hires = hires.withColumn("work_mode", F.lit("Hybrid"))
            job_cols = job_cols + ["work_mode"]

        job_events = envelope(upd, F.lit("u"), job_cols, "PS_JOB", b).unionByName(
            envelope(hires, F.lit("c"), job_cols, "PS_JOB", b, lsn_offset=1))
        # Injected duplicates: re-emit ~1% of events with an identical lsn (at-least-once delivery)
        dups = job_events.where(F.pmod(F.col("lsn"), F.lit(1000)) == 0)
        # Injected malformed events: null key, negative salary, invalid currency
        bad_src = upd.where(rnd(seed, f"bad{b}", F.col("n")) < 0.05).limit(30)
        bad = (bad_src.withColumn("emplid", F.when(F.col("n") % 3 == 0, F.lit(None).cast("string"))
                                  .otherwise(F.col("emplid")))
                      .withColumn("annual_rt", F.when(F.col("n") % 3 == 1, F.lit(-1).cast("decimal(18,2)"))
                                  .otherwise(F.col("annual_rt")))
                      .withColumn("currency_cd", F.when(F.col("n") % 3 == 2, F.lit("XXX"))
                                  .otherwise(F.col("currency_cd"))))
        job_events = job_events.unionByName(dups).unionByName(
            envelope(bad, F.lit("u"), job_cols, "PS_JOB", b, lsn_offset=2))
        write_events(job_events, root, "job", b)

        # Personal data: new hires (c) and GDPR erasure requests (d)
        erase = emp.where(rnd(seed, f"gdpr{b}", F.col("n")) < 0.001)
        personal = envelope(hires, F.lit("c"), PERSONAL_COLS, "PS_PERSONAL_DATA", b,
                            lsn_offset=1).unionByName(
            envelope(erase, F.lit("d"), PERSONAL_COLS, "PS_PERSONAL_DATA", b, lsn_offset=3,
                     with_before_key_only=True))
        write_events(personal, root, "personal_data", b)

    # Facts every batch: absences (~2%) and pulse survey responses (~10%)
    pop = employees_base(spark, 1, n, seed)
    ra = rnd(seed, f"abs{b}", F.col("n"))
    absences = (pop.where(ra < 0.02)
                .withColumn("absence_id", F.format_string("A%04d-%s", F.lit(b), F.col("emplid")))
                .withColumn("absence_type", pick(["SICK", "ANNUAL", "PARENTAL", "OTHER"], (ra * 50) % 1))
                .withColumn("start_dt", F.expr(f"date_add(DATE'2026-01-01', {b})"))
                .withColumn("days", (F.floor(ra * 500) % 10 + 1).cast("int")))
    write_events(envelope(absences, F.lit("c"),
                          ["absence_id", "emplid", "absence_type", "start_dt", "days"],
                          "PS_ABSENCE_HIST", b), root, "absence", b)
    rp = rnd(seed, f"pulse{b}", F.col("n"))
    pulse = (pop.where(rp < 0.10)
             .withColumn("response_id", F.format_string("P%04d-%s", F.lit(b), F.col("emplid")))
             .withColumn("survey_date", F.expr(f"date_add(DATE'2026-01-01', {b})"))
             .withColumn("engagement_score", (F.floor(rp * 1000) % 11).cast("int"))
             .withColumn("sentiment_score", F.round(F.lit(-1.0) + (rp * 100 % 1) * 2, 3)))
    write_events(envelope(pulse, F.lit("c"),
                          ["response_id", "emplid", "survey_date", "engagement_score",
                           "sentiment_score"], "PULSE_SURVEY", b), root, "pulse_survey", b)


def write_reference_tables(spark: SparkSession, catalog: str) -> None:
    dept = spark.createDataFrame(
        [(d, name, f"CC{d[1:]}") for d, name in DEPARTMENTS],
        "dept_id string, dept_name string, cost_centre string")
    job = spark.createDataFrame(
        [(c, t, "Professional" if i < 9 else "Leadership", f"G{i // 2 + 1}")
         for i, (c, t) in enumerate(zip(JOB_CODES, JOB_TITLES))],
        "job_code string, job_title string, job_family string, grade string")
    for name, df in [("ref_department", dept), ("ref_job", job)]:
        df.write.mode("overwrite").option("overwriteSchema", "true") \
          .saveAsTable(f"{catalog}.bronze.{name}")
        print(f"wrote reference table {catalog}.bronze.{name}")


def main(argv=None):
    args = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    generate(spark, args)


if __name__ == "__main__":
    main(sys.argv[1:])
