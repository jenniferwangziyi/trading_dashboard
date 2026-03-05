"""
ETF Trading Demo — DLT Pipeline (Continuous Mode)
Deploy as a Databricks Delta Live Tables pipeline targeting jennifer_wang.etf_trading.
Set pipeline mode to "continuous" for near-real-time processing.

Pipeline configuration:
  - Target catalog: jennifer_wang
  - Target schema: etf_trading
  - Mode: Continuous
  - Source tables: jennifer_wang.etf_trading.raw_market_data, raw_orders, raw_executions
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "jennifer_wang"
SCHEMA = "etf_trading"
SRC = f"{CATALOG}.{SCHEMA}"


# ══════════════════════════════════════════════════════════════════════════════
# SILVER LAYER — Streaming Tables
# ══════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="market_data_silver",
    comment="Deduplicated + enriched market ticks with spread and relative volume",
    table_properties={"quality": "silver", "pipelines.reset.allowed": "true"},
)
@dlt.expect_or_drop("valid_ticker", "ticker IS NOT NULL")
@dlt.expect_or_drop("valid_price", "last_price > 0")
@dlt.expect_or_drop("valid_spread", "ask >= bid")
def market_data_silver():
    raw = spark.readStream.format("delta").table(f"{SRC}.raw_market_data")

    # Deduplicate on (ticker, event_time) with watermark
    deduped = (
        raw
        .withWatermark("event_time", "2 minutes")
        .dropDuplicates(["ticker", "event_time"])
    )

    # 20-tick rolling average volume per ticker (approximate via session window)
    # For streaming, compute relative volume as ratio to static reference volumes
    avg_ref_vol = {
        "SPY": 120_000, "QQQ": 85_000, "IVV": 95_000,
        "VTI": 40_000, "XLK": 25_000, "XLF": 30_000,
    }

    enriched = (
        deduped
        .withColumn("mid_price", F.round((F.col("bid") + F.col("ask")) / 2, 4))
        .withColumn("spread_bps", F.round(
            (F.col("ask") - F.col("bid")) / F.col("last_price") * 10000, 2
        ))
        .withColumn("relative_volume", F.round(
            F.col("volume") / F.create_map(
                *[x for t, v in avg_ref_vol.items() for x in [F.lit(t), F.lit(float(v))]]
            ).getItem(F.col("ticker")), 3
        ))
        .withColumn("price_move_pct", F.round(
            (F.col("last_price") - F.col("vwap")) / F.col("vwap") * 100, 4
        ))
        .select(
            "event_id", "event_time", "ticker", "bid", "ask", "last_price",
            "mid_price", "spread_bps", "volume", "cum_volume", "vwap",
            "relative_volume", "price_move_pct", "source"
        )
    )
    return enriched


@dlt.table(
    name="orders_silver",
    comment="Enriched orders joined with ETF reference data",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_qty", "qty > 0")
def orders_silver():
    raw_orders = spark.readStream.format("delta").table(f"{SRC}.raw_orders")
    etf_ref = spark.read.format("delta").table(f"{SRC}.etf_reference")

    enriched = (
        raw_orders
        .join(F.broadcast(etf_ref.select("ticker", "name", "sector", "expense_ratio", "aum_bn")),
              raw_orders["etf_ticker"] == etf_ref["ticker"],
              how="left")
        .withColumn("notional_value", F.round(F.col("qty") * F.col("price_limit"), 2))
        .withColumn("time_in_market_sec", F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("created_at"))
        .withColumn("etf_name", F.col("name"))
        .drop("ticker", "name")
        .select(
            "order_id", "trader_id", "etf_ticker", "etf_name", "sector",
            "direction", "qty", "order_type", "price_limit", "arrival_price",
            "status", "created_at", "strategy", "notional_value",
            "time_in_market_sec", "expense_ratio", "aum_bn"
        )
    )
    return enriched


@dlt.table(
    name="executions_silver",
    comment="Enriched executions with slippage and fill metrics",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_exec_id", "exec_id IS NOT NULL")
@dlt.expect_or_drop("positive_fill", "fill_qty > 0")
def executions_silver():
    raw_execs = spark.readStream.format("delta").table(f"{SRC}.raw_executions")
    raw_orders = spark.read.format("delta").table(f"{SRC}.raw_orders")

    enriched = (
        raw_execs
        .join(
            raw_orders.select("order_id", "qty", "arrival_price", "etf_ticker", "trader_id", "direction"),
            on="order_id",
            how="left",
        )
        .withColumn("slippage_bps", F.round(
            F.when(F.col("arrival_price").isNotNull() & (F.col("arrival_price") > 0),
                   (F.col("fill_price") - F.col("arrival_price")) / F.col("arrival_price") * 10000
            ).otherwise(F.lit(0.0)), 2
        ))
        .withColumn("fill_notional", F.round(F.col("fill_qty") * F.col("fill_price"), 2))
        .select(
            "exec_id", "order_id", "etf_ticker", "trader_id", "direction",
            "fill_qty", "fill_price", "fill_notional", "venue", "exec_time",
            "arrival_price", "slippage_bps", "qty"
        )
    )
    return enriched


# ══════════════════════════════════════════════════════════════════════════════
# GOLD LAYER — Materialized Views (Aggregated)
# ══════════════════════════════════════════════════════════════════════════════

@dlt.table(
    name="order_analytics_gold",
    comment="Per-order execution quality metrics: fill rate, slippage, VWAP comparison",
    table_properties={"quality": "gold"},
)
def order_analytics_gold():
    orders = dlt.read("orders_silver")
    execs = dlt.read("executions_silver")

    # Aggregate executions per order
    exec_agg = (
        execs
        .groupBy("order_id")
        .agg(
            F.sum("fill_qty").alias("filled_qty"),
            F.count("exec_id").alias("fill_count"),
            F.avg("slippage_bps").alias("avg_slippage_bps"),
            F.sum("fill_notional").alias("total_fill_notional"),
            F.avg("fill_price").alias("avg_fill_price"),
            F.min("exec_time").alias("first_fill_time"),
            F.max("exec_time").alias("last_fill_time"),
        )
    )

    result = (
        orders
        .join(exec_agg, on="order_id", how="left")
        .withColumn("filled_qty", F.coalesce(F.col("filled_qty"), F.lit(0)))
        .withColumn("fill_rate", F.round(
            F.when(F.col("qty") > 0, F.col("filled_qty") / F.col("qty")).otherwise(0.0), 4
        ))
        .withColumn("remaining_qty", F.col("qty") - F.col("filled_qty"))
        .withColumn("estimated_completion_pct", F.round(F.col("fill_rate") * 100, 2))
        .withColumn("vwap_vs_limit", F.round(
            F.when(F.col("price_limit").isNotNull() & (F.col("price_limit") > 0),
                   (F.col("avg_fill_price") - F.col("price_limit")) / F.col("price_limit") * 10000
            ).otherwise(F.lit(None).cast("double")), 2
        ))
        .withColumn("avg_slippage_bps", F.round(F.coalesce(F.col("avg_slippage_bps"), F.lit(0.0)), 2))
        .select(
            "order_id", "trader_id", "etf_ticker", "etf_name", "direction",
            "qty", "filled_qty", "remaining_qty", "fill_rate", "fill_count",
            "avg_slippage_bps", "avg_fill_price", "total_fill_notional",
            "notional_value", "estimated_completion_pct", "vwap_vs_limit",
            "order_type", "strategy", "status", "created_at",
            "first_fill_time", "last_fill_time",
        )
    )
    return result


@dlt.table(
    name="market_snapshot_gold",
    comment="Latest market tick per ETF with intraday stats",
    table_properties={"quality": "gold"},
)
def market_snapshot_gold():
    mkt = dlt.read("market_data_silver")

    # Window to get latest tick per ticker
    w_latest = Window.partitionBy("ticker").orderBy(F.col("event_time").desc())

    latest = (
        mkt
        .withColumn("rn", F.row_number().over(w_latest))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # Intraday stats (today's data only)
    today_start = F.date_trunc("day", F.current_timestamp())
    intraday = (
        mkt
        .filter(F.col("event_time") >= today_start)
        .groupBy("ticker")
        .agg(
            F.min("last_price").alias("intraday_low"),
            F.max("last_price").alias("intraday_high"),
            F.first("last_price", ignorenulls=True).alias("open_price"),
            F.max("cum_volume").alias("total_volume"),
        )
    )

    result = (
        latest
        .join(intraday, on="ticker", how="left")
        .withColumn("price_change_pct", F.round(
            F.when(F.col("open_price").isNotNull() & (F.col("open_price") > 0),
                   (F.col("last_price") - F.col("open_price")) / F.col("open_price") * 100
            ).otherwise(0.0), 4
        ))
        .select(
            "ticker", "event_time", "last_price", "bid", "ask", "mid_price",
            "spread_bps", "volume", "total_volume", "cum_volume", "vwap",
            "intraday_high", "intraday_low", "open_price", "price_change_pct",
            "relative_volume", "source"
        )
    )
    return result


@dlt.table(
    name="trading_performance_gold",
    comment="Per-trader per-day performance: fill rates, slippage, notional",
    table_properties={"quality": "gold"},
)
def trading_performance_gold():
    orders = dlt.read("order_analytics_gold")

    result = (
        orders
        .withColumn("trade_date", F.to_date("created_at"))
        .groupBy("trader_id", "trade_date")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum(F.when(F.col("status") == "FILLED", 1).otherwise(0)).alias("filled_orders"),
            F.sum(F.when(F.col("status").isin("PENDING", "PARTIAL"), 1).otherwise(0)).alias("active_orders"),
            F.avg("fill_rate").alias("avg_fill_rate"),
            F.avg("avg_slippage_bps").alias("avg_slippage_bps"),
            F.sum("notional_value").alias("total_notional_usd"),
            F.sum("total_fill_notional").alias("total_filled_notional_usd"),
            F.countDistinct("etf_ticker").alias("unique_etfs_traded"),
        )
        .withColumn("fill_rate_pct", F.round(
            F.when(F.col("total_orders") > 0, F.col("filled_orders") / F.col("total_orders") * 100).otherwise(0), 2
        ))
        .withColumn("participation_rate", F.round(F.col("avg_fill_rate") * 100, 2))
        .withColumn("avg_slippage_bps", F.round(F.coalesce(F.col("avg_slippage_bps"), F.lit(0.0)), 2))
        .select(
            "trader_id", "trade_date",
            "total_orders", "filled_orders", "active_orders",
            "fill_rate_pct", "participation_rate", "avg_slippage_bps",
            "total_notional_usd", "total_filled_notional_usd", "unique_etfs_traded"
        )
    )
    return result
