"""
DQX adapter: translate our YAML rules into DQX checks and run them.

Why DQX: Databricks-native; row-level AND dataset-level checks in one pass; native
error/warn criticality; splits a DataFrame into valid rows and quarantined rows with the
reasons attached (columns _errors and _warnings).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _to_dqx(rule: dict, table_name: str) -> dict:
    """One YAML rule -> one DQX check definition (metadata form)."""
    t, a = rule["type"], {}
    if t == "not_null":
        fn, a = "is_not_null", {"column": rule["column"]}
    elif t == "unique":
        fn, a = "is_unique", {"columns": rule["columns"]}
    elif t == "in_set":
        # Expressed as SQL (NULL allowed, as in the other engine) - avoids literal/column ambiguity
        vals = ", ".join(f"'{v}'" for v in rule["values"])
        fn, a = "sql_expression", {"expression": f"{rule['column']} IS NULL OR {rule['column']} IN ({vals})",
                                   "msg": rule["name"]}
    elif t == "between":
        fn, a = "is_in_range", {"column": rule["column"], "min_limit": rule.get("min"), "max_limit": rule.get("max")}
    elif t == "expression":
        fn, a = "sql_expression", {"expression": f"coalesce(({rule['expression']}), false)", "msg": rule["name"]}
    elif t == "foreign_key":
        fn, a = "foreign_key", {"columns": [rule["column"]], "ref_columns": [rule["ref_column"]],
                                "ref_df_name": rule["ref_table"]}
    check = {"name": rule["name"], "criticality": rule["severity"], "check": {"function": fn, "arguments": a}}
    if rule.get("where"):
        if t in ("unique", "foreign_key"):
            check["check"]["arguments"]["row_filter"] = rule["where"]
        else:
            check["filter"] = rule["where"]
    return check


def run(spark, workspace_client, table_name: str, df: DataFrame, rules: list, ref_dfs: dict):
    """Return (per-rule failed counts, total rows, quarantined DataFrame)."""
    from databricks.labs.dqx.engine import DQEngine
    checks = [_to_dqx(r, table_name) for r in rules]
    status = DQEngine.validate_checks(checks)
    if status.has_errors:
        raise ValueError(f"Invalid DQX checks for {table_name}: {status}")
    engine = DQEngine(workspace_client, spark=spark)
    out = engine.apply_checks_by_metadata(df, checks, ref_dfs=ref_dfs)
    annotated = out[0] if isinstance(out, tuple) else out
    annotated = annotated.cache()
    total = annotated.count()
    # Count failures per rule from the _errors / _warnings arrays DQX attaches to each row
    issues = annotated.select(F.explode(F.concat(F.coalesce("_errors", F.array()),
                                                 F.coalesce("_warnings", F.array()))).alias("i"))
    counts = {r["name"]: 0 for r in rules}
    for row in issues.groupBy(F.col("i.name").alias("rule")).count().collect():
        counts[row["rule"]] = row["count"]
    quarantine = annotated.where(F.size(F.coalesce("_errors", F.array())) > 0)
    return counts, total, quarantine
