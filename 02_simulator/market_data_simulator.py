"""
ETF Trading Demo — Market Data Simulator
Runs as an always-on Databricks Job (single-node cluster).
Emits realistic tick data every 5 seconds to raw_market_data Delta table.
Also generates ~5% chance of new orders and partial fills.

Deploy as a Databricks Job pointing to this notebook.
"""

import random
import math
import time
import uuid
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()

# ── Config ────────────────────────────────────────────────────────────────────
CATALOG = "jennifer_wang"
SCHEMA = "etf_trading"
MARKET_TABLE = f"{CATALOG}.{SCHEMA}.raw_market_data"
ORDERS_TABLE = f"{CATALOG}.{SCHEMA}.raw_orders"
EXECUTIONS_TABLE = f"{CATALOG}.{SCHEMA}.raw_executions"
TICK_INTERVAL = 5  # seconds

ETF_UNIVERSE = ["SPY", "QQQ", "IVV", "VTI", "XLK", "XLF"]
BASE_PRICES = {
    "SPY": 521.50, "QQQ": 448.30, "IVV": 524.75,
    "VTI": 263.40, "XLK": 218.60, "XLF": 49.80,
}
VOLATILITIES = {
    "SPY": 0.0006, "QQQ": 0.0009, "IVV": 0.0006,
    "VTI": 0.0007, "XLK": 0.0011, "XLF": 0.0008,
}
TYPICAL_SPREADS = {
    "SPY": 0.01, "QQQ": 0.02, "IVV": 0.01,
    "VTI": 0.02, "XLK": 0.03, "XLF": 0.01,
}
STRATEGIES = ["TWAP", "VWAP", "IS", "POV", "MKT_ON_CLOSE"]
ORDER_TYPES = ["LIMIT", "VWAP", "TWAP", "MKT"]
TRADER_IDS = ["T001", "T002", "T003", "T004", "T005"]
VENUES = ["NYSE_ARCA", "NASDAQ", "CBOE_EDGX", "IEX", "MEMX"]

# ── State ─────────────────────────────────────────────────────────────────────
prices = dict(BASE_PRICES)
cum_volumes = {t: 0 for t in ETF_UNIVERSE}
order_counter = 10000  # Start above seed orders
exec_counter = 100000

# ── Schemas ───────────────────────────────────────────────────────────────────
market_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("ticker", StringType(), False),
    StructField("bid", DoubleType(), False),
    StructField("ask", DoubleType(), False),
    StructField("last_price", DoubleType(), False),
    StructField("volume", LongType(), False),
    StructField("cum_volume", LongType(), False),
    StructField("vwap", DoubleType(), False),
    StructField("source", StringType(), False),
])

order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("trader_id", StringType(), False),
    StructField("etf_ticker", StringType(), False),
    StructField("direction", StringType(), False),
    StructField("qty", LongType(), False),
    StructField("order_type", StringType(), False),
    StructField("price_limit", DoubleType(), False),
    StructField("status", StringType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("strategy", StringType(), False),
    StructField("arrival_price", DoubleType(), False),
])

exec_schema = StructType([
    StructField("exec_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("fill_qty", LongType(), False),
    StructField("fill_price", DoubleType(), False),
    StructField("venue", StringType(), False),
    StructField("exec_time", TimestampType(), False),
])


def get_pending_orders():
    """Fetch PENDING and PARTIAL orders for fill simulation."""
    try:
        rows = spark.sql(f"""
            SELECT order_id, etf_ticker, direction, qty, arrival_price
            FROM {ORDERS_TABLE}
            WHERE status IN ('PENDING', 'PARTIAL')
            LIMIT 20
        """).collect()
        return rows
    except Exception:
        return []


def generate_tick(ticker: str, now: datetime) -> Row:
    """Generate next market tick via random walk."""
    global prices, cum_volumes
    vol = VOLATILITIES[ticker]
    drift = random.gauss(0, prices[ticker] * vol)
    # Slight mean reversion toward base
    mean_rev = (BASE_PRICES[ticker] - prices[ticker]) * 0.001
    prices[ticker] = max(prices[ticker] + drift + mean_rev, BASE_PRICES[ticker] * 0.85)
    price = round(prices[ticker], 2)

    spread = TYPICAL_SPREADS[ticker] * random.uniform(0.8, 1.5)
    bid = round(price - spread / 2, 2)
    ask = round(price + spread / 2, 2)

    # Volume: higher near open/close (simulate intraday U-shape via noise)
    hour = now.hour
    vol_mult = 1.8 if hour in (9, 15) else (1.3 if hour in (10, 14) else 1.0)
    tick_vol = int(random.lognormvariate(8.5, 0.4) * vol_mult)
    cum_volumes[ticker] += tick_vol
    vwap = round((prices[ticker] * 0.7 + BASE_PRICES[ticker] * 0.3), 2)

    return Row(
        event_id=f"MKT_{ticker}_{int(now.timestamp())}_{random.randint(1000, 9999)}",
        event_time=now,
        ticker=ticker,
        bid=bid,
        ask=ask,
        last_price=price,
        volume=tick_vol,
        cum_volume=cum_volumes[ticker],
        vwap=vwap,
        source="bloomberg_sim",
    )


def maybe_generate_order(now: datetime):
    """~5% chance per tick cycle: create a new incoming order."""
    global order_counter
    if random.random() > 0.05:
        return None
    order_counter += 1
    ticker = random.choice(ETF_UNIVERSE)
    direction = random.choice(["BUY", "SELL"])
    qty = random.randint(5, 500) * 100
    order_type = random.choice(ORDER_TYPES)
    strategy = random.choice(STRATEGIES)
    arrival = round(prices[ticker] * random.uniform(0.998, 1.002), 2)
    price_limit = round(arrival * (1.002 if direction == "BUY" else 0.998), 2)
    return Row(
        order_id=f"ORD-{order_counter:05d}",
        trader_id=random.choice(TRADER_IDS),
        etf_ticker=ticker,
        direction=direction,
        qty=qty,
        order_type=order_type,
        price_limit=price_limit,
        status="PENDING",
        created_at=now,
        strategy=strategy,
        arrival_price=arrival,
    )


def maybe_generate_fill(pending_order, now: datetime):
    """~30% chance per PENDING order: generate a partial fill."""
    global exec_counter
    if random.random() > 0.30:
        return None
    exec_counter += 1
    ticker = pending_order["etf_ticker"]
    arrival = pending_order["arrival_price"]
    fill_qty = random.randint(100, min(5000, int(pending_order["qty"] * 0.3)))
    slippage = random.gauss(0, arrival * 0.0002)
    fill_price = round(arrival + slippage, 2)
    return Row(
        exec_id=f"EXEC-{exec_counter:07d}",
        order_id=pending_order["order_id"],
        fill_qty=fill_qty,
        fill_price=fill_price,
        venue=random.choice(VENUES),
        exec_time=now,
    )


# ── Main Loop ─────────────────────────────────────────────────────────────────

print("=== ETF Market Data Simulator Starting ===")
print(f"Writing to: {MARKET_TABLE}")
print(f"Tick interval: {TICK_INTERVAL}s\n")

iteration = 0
while True:
    iteration += 1
    now = datetime.now(timezone.utc)
    print(f"[{now.strftime('%H:%M:%S')}] Tick #{iteration}")

    # 1. Generate market ticks for all ETFs
    tick_rows = [generate_tick(t, now) for t in ETF_UNIVERSE]
    spark.createDataFrame(tick_rows, market_schema).write.format("delta").mode("append").saveAsTable(MARKET_TABLE)

    # 2. Possibly generate a new order
    new_order = maybe_generate_order(now)
    if new_order:
        spark.createDataFrame([new_order], order_schema).write.format("delta").mode("append").saveAsTable(ORDERS_TABLE)
        print(f"  + New order: {new_order['order_id']} {new_order['direction']} {new_order['qty']} {new_order['etf_ticker']}")

    # 3. Possibly generate fills for pending orders
    pending = get_pending_orders()
    fill_rows = []
    for po in pending:
        fill = maybe_generate_fill(po, now)
        if fill:
            fill_rows.append(fill)
    if fill_rows:
        spark.createDataFrame(fill_rows, exec_schema).write.format("delta").mode("append").saveAsTable(EXECUTIONS_TABLE)
        print(f"  + {len(fill_rows)} fill(s) generated")

    print(f"  Prices: " + " | ".join(f"{t}=${prices[t]:.2f}" for t in ETF_UNIVERSE))
    time.sleep(TICK_INTERVAL)
