"""
Table maintenance job: keeps Delta tables fast and storage tidy.

FOR EVERY MANAGED TABLE in the gold and ops schemas of each catalog:
  1. OPTIMIZE    compacts small files; on liquid-clustered tables it also re-clusters data
                 (liquid clustering replaces partitioning + Z-ORDER for new tables).
  2. VACUUM      deletes files no longer referenced after the retention period (default 7 days).
                 Never go below 7 days in production: time travel and running readers need them.
  3. ANALYZE     refreshes statistics used by the query optimiser.
Pipeline-owned tables (streaming tables, materialized views) are skipped: the pipeline maintains them.
If predictive optimization is enabled, Databricks also runs these automatically; this job makes the
practice explicit and auditable. Results go to <catalog>.ops.maintenance_log.
"""
import argparse
import sys

from pyspark.sql import SparkSession


def main(argv=None):
    a = argparse.ArgumentParser()
    a.add_argument("--catalogs", required=True, help="comma-separated, e.g. hr_dev,risk_dev,finance_dev")
    a.add_argument("--retain-hours", type=int, default=168)
    args = a.parse_args(argv)
    assert args.retain_hours >= 168, "Retention below 7 days is blocked by policy"
    spark = SparkSession.builder.getOrCreate()

    for cat in [c.strip() for c in args.catalogs.split(",") if c.strip()]:
        tables = spark.sql(f"""SELECT table_schema, table_name FROM {cat}.information_schema.tables
                               WHERE table_schema IN ('gold', 'ops') AND table_type = 'MANAGED'""").collect()
        log = []
        for t in tables:
            fq = f"{cat}.{t.table_schema}.{t.table_name}"
            for op, sql in [("OPTIMIZE", f"OPTIMIZE {fq}"),
                            ("VACUUM", f"VACUUM {fq} RETAIN {args.retain_hours} HOURS"),
                            ("ANALYZE", f"ANALYZE TABLE {fq} COMPUTE STATISTICS FOR ALL COLUMNS")]:
                try:
                    spark.sql(sql)
                    log.append((fq, op, "OK", ""))
                except Exception as e:  # noqa: BLE001 - record and continue with the next table
                    log.append((fq, op, "FAILED", str(e)[:300]))
        if log:
            (spark.createDataFrame(log, "table_name string, operation string, status string, message string")
             .selectExpr("current_timestamp() AS run_ts", "*")
             .write.mode("append").saveAsTable(f"{cat}.ops.maintenance_log"))
        print(f"{cat}: {len(tables)} tables maintained")


if __name__ == "__main__":
    main(sys.argv[1:])
