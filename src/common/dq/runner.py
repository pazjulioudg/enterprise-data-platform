"""
One entry point for every quality gate: run_gate().

    1. Load the gate's YAML rules.
    2. Run them on each candidate DataFrame with the chosen engine (dqx or gx).
    3. Decide pass/fail per rule using severity and max_fail_pct.
    4. Write results to <catalog>.ops.dq_results and failing rows to <catalog>.ops.dq_quarantine.
    5. Return the results so the job can stop BEFORE promoting data to Gold.
"""
from pyspark.sql import functions as F

from . import dqx_engine, gx_engine
from .rules import load_gate

ENGINES = {"dqx": dqx_engine, "gx": gx_engine}


def run_gate(spark, gate: str, engine: str, frames: dict, catalog: str, run_id: str,
             workspace_client=None, write: bool = True) -> list:
    """frames: {table_name: DataFrame}. Returns a list of result dicts."""
    spec = load_gate(gate)
    if workspace_client is None and engine == "dqx":
        from databricks.sdk import WorkspaceClient
        workspace_client = WorkspaceClient()        # on Databricks this authenticates automatically
    results, quarantines = [], []
    for table, t in spec["tables"].items():
        counts, total, quarantine = ENGINES[engine].run(spark, workspace_client, table, frames[table],
                                                        t["rules"], frames)
        for r in t["rules"]:
            failed = counts.get(r["name"], 0)
            pct = failed * 100.0 / total if total else 0.0
            results.append(dict(gate=gate, engine=engine, table_name=table, check_name=r["name"],
                                severity=r["severity"], passed=pct <= r["max_fail_pct"], failed_count=failed,
                                total_count=total, threshold_pct=float(r["max_fail_pct"]),
                                details=r.get("description", r["type"])))
        quarantines.append(quarantine.select(F.lit(table).alias("table_name"),
                                             F.to_json(F.struct(*[c for c in quarantine.columns])).alias("record")))
    if write:
        write_results(spark, catalog, run_id, results, quarantines)
    return results


def gate_passed(results: list) -> bool:
    """Promotion is blocked only by failed ERROR rules; WARN rules are recorded."""
    return all(r["passed"] for r in results if r["severity"] == "error")


def print_results(results: list) -> None:
    for r in results:
        flag = "PASS" if r["passed"] else "FAIL"
        print(f"[{flag}][{r['severity']:5}] {r['table_name']}.{r['check_name']}: "
              f"{r['failed_count']}/{r['total_count']} rows (tolerance {r['threshold_pct']}%)")


def write_results(spark, catalog, run_id, results, quarantines):
    schema = ("gate string, engine string, table_name string, check_name string, severity string, passed boolean, "
              "failed_count bigint, total_count bigint, threshold_pct double, details string")
    rows = [tuple(r[k] for k in ["gate", "engine", "table_name", "check_name", "severity", "passed",
                                 "failed_count", "total_count", "threshold_pct", "details"]) for r in results]
    (spark.createDataFrame(rows, schema)
     .withColumn("run_id", F.lit(run_id)).withColumn("run_ts", F.current_timestamp())
     .write.mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.ops.dq_results"))
    q = None
    for df in quarantines:
        q = df if q is None else q.unionByName(df)
    if q is not None:
        (q.withColumn("run_id", F.lit(run_id)).withColumn("quarantined_at", F.current_timestamp())
         .write.mode("append").option("mergeSchema", "true").saveAsTable(f"{catalog}.ops.dq_quarantine"))
