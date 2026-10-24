"""
FINANCE pipeline: market ticks -> Bronze -> Silver (exactly-once, de-duplicated) -> Gold daily views.

WHAT EACH STEP DOES
  1. Bronze  bronze.ticks_raw        Auto Loader reads new JSON files only (file-level exactly-once via checkpoint)
  2. Silver  silver.ticks_quarantine malformed ticks + the rule they broke
             silver.ticks            valid ticks; duplicates removed within a 1-hour event-time watermark;
                                     ticks later than the watermark are dropped (bounded state, standard practice)
  3. Gold    gold.fx_daily_close     last price per FX pair per day (used for currency conversion)
             gold.equity_daily_ohlc  open/high/low/close/volume per equity per day

WHY IT IS SAFE TO RE-RUN
  Checkpoints record which files and offsets were processed. A re-run (or a restart after a crash)
  continues from the last committed point; it never re-ingests processed files, and duplicates that
  arrive in new files are removed by the de-duplication step.

Configuration keys: edp.catalog (e.g. finance_dev), edp.landing_root.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("edp.catalog")  # noqa: F821
LANDING = spark.conf.get("edp.landing_root")  # noqa: F821
RULES = {"valid_symbol": "symbol IS NOT NULL", "positive_price": "price > 0",
         "bid_below_ask": "bid <= ask", "has_event_time": "event_ts IS NOT NULL"}
SAFE = {k: f"coalesce(({v}), false)" for k, v in RULES.items()}


# STEP 1 - Bronze: incremental file ingestion with Auto Loader
@dp.table(name=f"{CATALOG}.bronze.ticks_raw", comment="Raw market ticks as received.",
          table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"})
def ticks_raw():
    return (spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "json").option("cloudFiles.inferColumnTypes", "true")
            .load(f"{LANDING}/")
            .select("*", F.col("_metadata.file_path").alias("_source_file"), F.current_timestamp().alias("_ingest_ts")))


@dp.temporary_view(name="v_ticks_typed")
def v_ticks_typed():
    return (spark.readStream.table(f"{CATALOG}.bronze.ticks_raw")  # noqa: F821
            .select("tick_id", "symbol", "asset_class", F.col("price").cast("double").alias("price"),
                    F.col("bid").cast("double").alias("bid"), F.col("ask").cast("double").alias("ask"),
                    F.col("size").cast("long").alias("size"), F.to_timestamp("event_ts").alias("event_ts"),
                    F.col("source_seq").cast("long").alias("source_seq"), "_ingest_ts"))


# STEP 2a - Silver quarantine: keep every rejected tick with the names of the failed rules
@dp.table(name=f"{CATALOG}.silver.ticks_quarantine", comment="Rejected ticks with failed rule names.")
def ticks_quarantine():
    checks = [F.when(~F.expr(e), F.lit(n)) for n, e in SAFE.items()]
    return (spark.readStream.table("v_ticks_typed")  # noqa: F821
            .withColumn("failed_rules", F.filter(F.array(*checks), lambda x: x.isNotNull()))
            .where(F.size("failed_rules") > 0))


# STEP 2b - Silver: valid ticks, de-duplicated within the event-time watermark
@dp.table(name=f"{CATALOG}.silver.ticks", comment="Valid, de-duplicated ticks (exactly-once).",
          cluster_by=["symbol"])
@dp.expect_all_or_drop(SAFE)
def ticks():
    return (spark.readStream.table("v_ticks_typed")  # noqa: F821
            .withWatermark("event_ts", "1 hour")
            .dropDuplicatesWithinWatermark(["tick_id"]))


# STEP 3 - Gold: daily views recomputed from Silver (materialized views)
@dp.materialized_view(name=f"{CATALOG}.gold.fx_daily_close", comment="Daily closing rate per FX pair.")
def fx_daily_close():
    t = spark.read.table(f"{CATALOG}.silver.ticks").where("asset_class = 'FX'")  # noqa: F821
    return (t.groupBy("symbol", F.to_date("event_ts").alias("rate_date"))
             .agg(F.max_by("price", "event_ts").alias("close_rate"), F.count("*").alias("tick_count")))


@dp.materialized_view(name=f"{CATALOG}.gold.equity_daily_ohlc", comment="Daily OHLC and volume per equity.")
def equity_daily_ohlc():
    t = spark.read.table(f"{CATALOG}.silver.ticks").where("asset_class = 'EQUITY'")  # noqa: F821
    return (t.groupBy("symbol", F.to_date("event_ts").alias("trade_date"))
             .agg(F.min_by("price", "event_ts").alias("open"), F.max("price").alias("high"),
                  F.min("price").alias("low"), F.max_by("price", "event_ts").alias("close"),
                  F.sum("size").alias("volume")))
