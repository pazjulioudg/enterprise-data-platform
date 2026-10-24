"""
Great Expectations (GX Core 1.x) adapter: translate our YAML rules into Expectations.

Why GX: platform-agnostic (the same suite can run on Spark, Snowflake, Postgres, pandas),
very large expectation library, human-readable Data Docs. GX VALIDATES and reports counts;
it does not split rows into valid/quarantine, so quarantine is built here from the rule
conditions (see rules.failing_rows_condition).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import rules as R


def _flag(rule):
    """Column name holding a pre-computed boolean for rules GX has no direct expectation for."""
    return f"_dq_{rule['name']}"


def run(spark, workspace_client, table_name: str, df: DataFrame, rules: list, ref_dfs: dict):
    import great_expectations as gx
    ctx = gx.get_context(mode="ephemeral")
    asset = ctx.data_sources.add_spark(name="spark").add_dataframe_asset(name=table_name)
    batch_def = asset.add_batch_definition_whole_dataframe("whole")

    # 1. Pre-compute helper flags: GX expectations work on columns, so filters, SQL expressions
    #    and foreign keys become boolean columns that must equal TRUE.
    work = df
    for r in rules:
        if r["type"] == "expression" or (r.get("where") and r["type"] in R.ROW_LEVEL):
            work = work.withColumn(_flag(r), ~F.expr(R.failing_rows_condition(r)))
        elif r["type"] == "foreign_key":
            ref = ref_dfs[r["ref_table"]].select(F.col(r["ref_column"]).alias("_fk")).distinct()
            work = (work.join(ref, work[r["column"]] == ref["_fk"], "left")
                        .withColumn(_flag(r), F.col(r["column"]).isNull() | F.col("_fk").isNotNull())
                        .drop("_fk"))
    work = work.cache()
    total = work.count()

    # 2. Build the expectation suite, one expectation per rule.
    suite = gx.ExpectationSuite(name=f"{table_name}_suite")
    filtered = {}
    for r in rules:
        meta = {"rule": r["name"]}
        if r["type"] == "unique" and r.get("where"):
            filtered[r["name"]] = r          # validated separately on the filtered rows (below)
            continue
        if r["type"] == "unique":
            e = (gx.expectations.ExpectColumnValuesToBeUnique(column=r["columns"][0], meta=meta)
                 if len(r["columns"]) == 1 else
                 gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=r["columns"], meta=meta))
        elif _flag(r) in work.columns:
            e = gx.expectations.ExpectColumnValuesToBeInSet(column=_flag(r), value_set=[True], meta=meta)
        elif r["type"] == "not_null":
            e = gx.expectations.ExpectColumnValuesToNotBeNull(column=r["column"], meta=meta)
        elif r["type"] == "in_set":
            e = gx.expectations.ExpectColumnValuesToBeInSet(column=r["column"], value_set=r["values"], meta=meta)
        elif r["type"] == "between":
            e = gx.expectations.ExpectColumnValuesToBeBetween(column=r["column"], min_value=r.get("min"),
                                                              max_value=r.get("max"), meta=meta)
        suite.add_expectation(e)
    suite = ctx.suites.add(suite)
    vd = ctx.validation_definitions.add(gx.ValidationDefinition(name=f"{table_name}_v", data=batch_def, suite=suite))

    # 3. Validate and read the unexpected counts per rule.
    counts = {r["name"]: 0 for r in rules}
    result = vd.run(batch_parameters={"dataframe": work}, result_format="SUMMARY")
    for res in result.results:
        counts[res.expectation_config.meta["rule"]] = int(res.result.get("unexpected_count") or 0)
    for name, r in filtered.items():   # uniqueness on a subset of rows (e.g. current versions only)
        sub = work.where(r["where"])
        dup = sub.groupBy(*r["columns"]).count().where("count > 1")
        counts[name] = int(dup.agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0])

    # 4. Quarantine: rows breaking any row-level ERROR rule, with the rule names attached.
    errs = [r for r in rules if r["severity"] == "error" and (r["type"] in R.ROW_LEVEL or r["type"] == "foreign_key")]
    reasons = [F.when(~F.col(_flag(r)) if _flag(r) in work.columns else F.expr(R.failing_rows_condition(r)),
                      F.lit(r["name"])) for r in errs]
    quarantine = (work.withColumn("_errors", F.filter(F.array(*reasons), lambda x: x.isNotNull()))
                  .where(F.size("_errors") > 0)) if reasons else work.limit(0).withColumn("_errors", F.array())
    return counts, total, quarantine.drop(*[c for c in work.columns if c.startswith("_dq_")])
