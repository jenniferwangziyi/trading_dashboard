"""
ETF Trading Demo — Seed Data Script
Creates schema, tables, and backfills historical data via Databricks SQL warehouse.
Run this once before starting the simulator and DLT pipeline.
"""

import io
import json
import random
import math
import time
from datetime import datetime, timedelta, timezone
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# ── Config ─────────────────────────────────────────────────────────────────
CATALOG = "jennifer_wang"
SCHEMA = "etf_trading"
WAREHOUSE_ID = "65bc200a57dac15e"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_feed"

w = WorkspaceClient()

ETF_UNIVERSE = ["SPY", "QQQ", "IVV", "VTI", "XLK", "XLF"]
HEDGE_INSTRUMENTS = [
    {"id": "ES_MAR26", "type": "FUTURES", "underlying": "SPY", "expiry": "2026-03-21", "strike": None, "contract_size": 50, "delta": 1.0},
    {"id": "NQ_MAR26", "type": "FUTURES", "underlying": "QQQ", "expiry": "2026-03-21", "strike": None, "contract_size": 20, "delta": 1.0},
    {"id": "ES_JUN26", "type": "FUTURES", "underlying": "SPY", "expiry": "2026-06-20", "strike": None, "contract_size": 50, "delta": 1.0},
    {"id": "NQ_JUN26", "type": "FUTURES", "underlying": "QQQ", "expiry": "2026-06-20", "strike": None, "contract_size": 20, "delta": 1.0},
    {"id": "SPY_P510_MAR26", "type": "OPTIONS", "underlying": "SPY", "expiry": "2026-03-21", "strike": 510.0, "contract_size": 100, "delta": -0.45},
    {"id": "SPY_P500_MAR26", "type": "OPTIONS", "underlying": "SPY", "expiry": "2026-03-21", "strike": 500.0, "contract_size": 100, "delta": -0.30},
    {"id": "QQQ_P440_MAR26", "type": "OPTIONS", "underlying": "QQQ", "expiry": "2026-03-21", "strike": 440.0, "contract_size": 100, "delta": -0.35},
    {"id": "SPY_P490_JUN26", "type": "OPTIONS", "underlying": "SPY", "expiry": "2026-06-20", "strike": 490.0, "contract_size": 100, "delta": -0.28},
]

TRADERS = [
    {"id": "T001", "name": "Alex Chen", "desk": "ETF Execution", "risk_limit_usd": 50_000_000},
    {"id": "T002", "name": "Maria Santos", "desk": "Index Arb", "risk_limit_usd": 75_000_000},
    {"id": "T003", "name": "James Park", "desk": "ETF Execution", "risk_limit_usd": 40_000_000},
    {"id": "T004", "name": "Sarah Kim", "desk": "Macro/Overlay", "risk_limit_usd": 100_000_000},
    {"id": "T005", "name": "David Liu", "desk": "ETF Execution", "risk_limit_usd": 30_000_000},
]

ETF_REF = {
    "SPY": {"name": "SPDR S&P 500 ETF Trust", "aum_bn": 530.2, "benchmark": "S&P 500", "sector": "Broad Market", "expense_ratio": 0.0945, "base_price": 521.50},
    "QQQ": {"name": "Invesco QQQ Trust", "aum_bn": 290.8, "benchmark": "Nasdaq-100", "sector": "Technology", "expense_ratio": 0.20, "base_price": 448.30},
    "IVV": {"name": "iShares Core S&P 500 ETF", "aum_bn": 490.1, "benchmark": "S&P 500", "sector": "Broad Market", "expense_ratio": 0.03, "base_price": 524.75},
    "VTI": {"name": "Vanguard Total Stock Market ETF", "aum_bn": 410.5, "benchmark": "CRSP US Total Market", "sector": "Broad Market", "expense_ratio": 0.03, "base_price": 263.40},
    "XLK": {"name": "Technology Select Sector SPDR", "aum_bn": 72.3, "benchmark": "S&P Tech Sector", "sector": "Technology", "expense_ratio": 0.09, "base_price": 218.60},
    "XLF": {"name": "Financial Select Sector SPDR", "aum_bn": 45.1, "benchmark": "S&P Financial Sector", "sector": "Financials", "expense_ratio": 0.09, "base_price": 49.80},
}

# ── Helpers ──────────────────────────────────────────────────────────────────

def run_sql(statement: str, wait: bool = True) -> dict:
    """Execute SQL via warehouse, poll until done."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        wait_timeout="30s" if wait else "0s",
    )
    if not wait:
        return resp
    if resp.status.state not in (StatementState.SUCCEEDED,):
        # Poll
        for _ in range(60):
            time.sleep(2)
            resp = w.statement_execution.get_statement(resp.statement_id)
            if resp.status.state == StatementState.SUCCEEDED:
                break
            if resp.status.state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
                raise RuntimeError(f"SQL failed: {resp.status.error}")
    return resp


def rows_to_values(rows: list[dict], cols: list[str]) -> str:
    """Convert list of dicts to SQL VALUES string."""
    parts = []
    for row in rows:
        vals = []
        for c in cols:
            v = row.get(c)
            if v is None:
                vals.append("NULL")
            elif isinstance(v, bool):
                vals.append("TRUE" if v else "FALSE")
            elif isinstance(v, (int, float)):
                vals.append(str(v))
            else:
                # Escape single quotes
                vals.append(f"'{str(v).replace(chr(39), chr(39)+chr(39))}'")
        parts.append(f"({', '.join(vals)})")
    return ",\n  ".join(parts)


def insert_batch(table: str, cols: list[str], rows: list[dict], batch_size: int = 200):
    """Insert rows in batches."""
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        vals = rows_to_values(chunk, cols)
        run_sql(f"INSERT INTO {CATALOG}.{SCHEMA}.{table} ({', '.join(cols)}) VALUES\n  {vals}")
    print(f"  ✓ Inserted {len(rows)} rows into {table}")


# ── Step 1: Schema + Volume ──────────────────────────────────────────────────

print("=== ETF Trading Demo — Seed Script ===\n")

print("1. Creating schema and volume...")
run_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} WITH DBPROPERTIES ('deleteAfter'='2027-12-31', 'industry'='etf_trading')")
run_sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_feed")
print("  ✓ Schema and volume ready\n")


# ── Step 2: Reference Tables ─────────────────────────────────────────────────

print("2. Creating reference tables...")

run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.etf_reference (
  ticker         STRING,
  name           STRING,
  aum_bn         DOUBLE,
  benchmark      STRING,
  sector         STRING,
  expense_ratio  DOUBLE,
  intraday_iv    DOUBLE,
  base_price     DOUBLE
)
""")

run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.etf_constituents (
  etf_ticker      STRING NOT NULL,
  constituent     STRING NOT NULL,
  weight_pct      DOUBLE,
  sector          STRING
)
""")

run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.hedge_instruments (
  instrument_id   STRING,
  type            STRING,
  underlying      STRING,
  expiry          STRING,
  strike          DOUBLE,
  contract_size   INT,
  delta           DOUBLE
)
""")

run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.traders (
  trader_id       STRING,
  name            STRING,
  desk            STRING,
  risk_limit_usd  BIGINT
)
""")

print("  ✓ Reference table DDL done\n")

# ── Step 3: Bronze Tables ────────────────────────────────────────────────────

print("3. Creating bronze tables...")

run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.raw_market_data (
  event_id    STRING,
  event_time  TIMESTAMP,
  ticker      STRING,
  bid         DOUBLE,
  ask         DOUBLE,
  last_price  DOUBLE,
  volume      BIGINT,
  cum_volume  BIGINT,
  vwap        DOUBLE,
  source      STRING
)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.raw_orders (
  order_id      STRING,
  trader_id     STRING,
  etf_ticker    STRING,
  direction     STRING,
  qty           INT,
  order_type    STRING,
  price_limit   DOUBLE,
  status        STRING,
  created_at    TIMESTAMP,
  strategy      STRING,
  arrival_price DOUBLE
)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

run_sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.raw_executions (
  exec_id     STRING,
  order_id    STRING,
  fill_qty    INT,
  fill_price  DOUBLE,
  venue       STRING,
  exec_time   TIMESTAMP
)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

print("  ✓ Bronze table DDL done\n")

# ── Step 4: Seed Reference Data ──────────────────────────────────────────────

print("4. Seeding reference data...")

etf_rows = []
for ticker, info in ETF_REF.items():
    etf_rows.append({
        "ticker": ticker,
        "name": info["name"],
        "aum_bn": info["aum_bn"],
        "benchmark": info["benchmark"],
        "sector": info["sector"],
        "expense_ratio": info["expense_ratio"],
        "intraday_iv": round(random.uniform(12.0, 22.0), 2),
        "base_price": info["base_price"],
    })
insert_batch("etf_reference", ["ticker", "name", "aum_bn", "benchmark", "sector", "expense_ratio", "intraday_iv", "base_price"], etf_rows)

# Constituents — top holdings per ETF
constituents_data = {
    "SPY": [("AAPL", 7.2, "Technology"), ("MSFT", 6.8, "Technology"), ("NVDA", 5.1, "Technology"), ("AMZN", 3.8, "Consumer Disc"), ("META", 2.5, "Technology"), ("GOOGL", 2.4, "Communication"), ("BRK.B", 1.7, "Financials"), ("JPM", 1.6, "Financials"), ("TSLA", 1.5, "Consumer Disc"), ("AVGO", 1.4, "Technology")],
    "QQQ": [("MSFT", 8.9, "Technology"), ("AAPL", 8.5, "Technology"), ("NVDA", 7.8, "Technology"), ("AMZN", 5.2, "Consumer Disc"), ("META", 4.1, "Technology"), ("GOOGL", 3.9, "Communication"), ("TSLA", 2.8, "Consumer Disc"), ("AVGO", 2.6, "Technology"), ("COST", 2.1, "Consumer Staples"), ("NFLX", 1.9, "Communication")],
    "IVV": [("AAPL", 7.1, "Technology"), ("MSFT", 6.7, "Technology"), ("NVDA", 5.0, "Technology"), ("AMZN", 3.7, "Consumer Disc"), ("META", 2.4, "Technology"), ("GOOGL", 2.3, "Communication"), ("BRK.B", 1.8, "Financials"), ("JPM", 1.7, "Financials"), ("TSLA", 1.6, "Consumer Disc"), ("LLY", 1.3, "Healthcare")],
    "VTI": [("MSFT", 5.8, "Technology"), ("AAPL", 5.7, "Technology"), ("NVDA", 4.1, "Technology"), ("AMZN", 3.0, "Consumer Disc"), ("META", 1.9, "Technology"), ("GOOGL", 1.8, "Communication"), ("BRK.B", 1.5, "Financials"), ("JPM", 1.4, "Financials"), ("TSLA", 1.2, "Consumer Disc"), ("LLY", 1.0, "Healthcare")],
    "XLK": [("MSFT", 22.3, "Technology"), ("AAPL", 21.8, "Technology"), ("NVDA", 18.4, "Technology"), ("AVGO", 4.8, "Technology"), ("ORCL", 3.2, "Technology"), ("CRM", 2.9, "Technology"), ("CSCO", 2.4, "Technology"), ("ADBE", 2.1, "Technology"), ("AMD", 1.8, "Technology"), ("ACN", 1.5, "Technology")],
    "XLF": [("BRK.B", 13.2, "Financials"), ("JPM", 12.1, "Financials"), ("V", 8.4, "Financials"), ("MA", 6.9, "Financials"), ("BAC", 5.1, "Financials"), ("WFC", 4.2, "Financials"), ("GS", 3.1, "Financials"), ("MS", 2.8, "Financials"), ("AXP", 2.3, "Financials"), ("BLK", 2.0, "Financials")],
}
constituent_rows = []
for etf, holdings in constituents_data.items():
    for constituent, weight, sector in holdings:
        constituent_rows.append({"etf_ticker": etf, "constituent": constituent, "weight_pct": weight, "sector": sector})
insert_batch("etf_constituents", ["etf_ticker", "constituent", "weight_pct", "sector"], constituent_rows)

hedge_rows = [{
    "instrument_id": h["id"],
    "type": h["type"],
    "underlying": h["underlying"],
    "expiry": h["expiry"],
    "strike": h["strike"],
    "contract_size": h["contract_size"],
    "delta": h["delta"],
} for h in HEDGE_INSTRUMENTS]
insert_batch("hedge_instruments", ["instrument_id", "type", "underlying", "expiry", "strike", "contract_size", "delta"], hedge_rows)

trader_rows = [{"trader_id": t["id"], "name": t["name"], "desk": t["desk"], "risk_limit_usd": t["risk_limit_usd"]} for t in TRADERS]
insert_batch("traders", ["trader_id", "name", "desk", "risk_limit_usd"], trader_rows)

print()

# ── Step 5: Market Data Backfill (2 days, 5-min intervals) ──────────────────

print("5. Backfilling market data (2 days × 6 tickers × 78 intervals)...")

NOW = datetime(2026, 3, 5, 16, 0, 0, tzinfo=timezone.utc)
MARKET_OPEN_OFFSET = 9 * 3600 + 30 * 60  # 9:30 AM
TRADING_SECONDS = 6.5 * 3600  # 6.5 trading hours
INTERVAL = 300  # 5 minutes

random.seed(42)

market_rows = []
prices = {ticker: ETF_REF[ticker]["base_price"] for ticker in ETF_UNIVERSE}
cum_vols = {ticker: 0 for ticker in ETF_UNIVERSE}

for day_offset in range(2, 0, -1):
    day_base = NOW - timedelta(days=day_offset)
    day_start = day_base.replace(hour=0, minute=0, second=0, microsecond=0)
    open_ts = day_start + timedelta(seconds=MARKET_OPEN_OFFSET)
    # Reset daily volumes
    for ticker in ETF_UNIVERSE:
        cum_vols[ticker] = 0

    n_intervals = int(TRADING_SECONDS / INTERVAL)
    for i in range(n_intervals):
        ts = open_ts + timedelta(seconds=i * INTERVAL)
        for ticker in ETF_UNIVERSE:
            base = ETF_REF[ticker]["base_price"]
            # Random walk with slight mean reversion
            drift = random.gauss(0, base * 0.0008)
            prices[ticker] = max(prices[ticker] + drift, base * 0.90)
            price = round(prices[ticker], 2)
            spread = round(random.uniform(0.01, 0.05), 2)
            bid = round(price - spread / 2, 2)
            ask = round(price + spread / 2, 2)

            # Volume: higher near open/close (U-shape)
            time_frac = i / n_intervals
            vol_mult = 1.5 if time_frac < 0.1 or time_frac > 0.9 else 1.0
            vol = int(random.lognormvariate(9, 0.5) * vol_mult)
            cum_vols[ticker] += vol
            vwap = round(price * random.uniform(0.9995, 1.0005), 2)

            event_id = f"MKT_{ticker}_{int(ts.timestamp())}_{random.randint(1000,9999)}"
            market_rows.append({
                "event_id": event_id,
                "event_time": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "ticker": ticker,
                "bid": bid,
                "ask": ask,
                "last_price": price,
                "volume": vol,
                "cum_volume": cum_vols[ticker],
                "vwap": vwap,
                "source": "bloomberg_sim",
            })

insert_batch("raw_market_data", ["event_id", "event_time", "ticker", "bid", "ask", "last_price", "volume", "cum_volume", "vwap", "source"], market_rows)
print(f"  ✓ {len(market_rows)} market ticks inserted\n")


# ── Step 6: Orders Backfill ──────────────────────────────────────────────────

print("6. Generating orders and executions...")

STRATEGIES = ["TWAP", "VWAP", "IS", "POV", "MKT_ON_CLOSE"]
ORDER_TYPES = ["LIMIT", "VWAP", "TWAP", "MKT"]
STATUSES = ["FILLED", "FILLED", "FILLED", "PARTIAL", "CANCELLED", "PENDING"]
VENUES = ["NYSE_ARCA", "NASDAQ", "CBOE_EDGX", "IEX", "MEMX"]

random.seed(99)

orders = []
executions = []
exec_counter = 1

for oid in range(1, 151):
    trader = random.choice(TRADERS)
    ticker = random.choice(ETF_UNIVERSE)
    direction = random.choice(["BUY", "SELL"])
    qty = random.randint(500, 50000) // 100 * 100
    order_type = random.choice(ORDER_TYPES)
    strategy = random.choice(STRATEGIES)
    status = random.choice(STATUSES)

    base_price = ETF_REF[ticker]["base_price"]
    arrival_price = round(base_price * random.uniform(0.997, 1.003), 2)
    price_limit = round(arrival_price * (1.002 if direction == "BUY" else 0.998), 2)

    # Created within last 2 days during market hours
    day_offset = random.choice([0, 1])
    hour = random.randint(9, 15)
    minute = random.randint(0, 59)
    if hour == 9:
        minute = random.randint(30, 59)
    created_at = (NOW - timedelta(days=day_offset)).replace(hour=hour, minute=minute, second=random.randint(0, 59), microsecond=0)

    order_id = f"ORD-{oid:05d}"
    orders.append({
        "order_id": order_id,
        "trader_id": trader["id"],
        "etf_ticker": ticker,
        "direction": direction,
        "qty": qty,
        "order_type": order_type,
        "price_limit": price_limit,
        "status": status,
        "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "strategy": strategy,
        "arrival_price": arrival_price,
    })

    # Generate fills for non-PENDING orders
    if status in ("FILLED", "PARTIAL"):
        fill_pct = 1.0 if status == "FILLED" else random.uniform(0.2, 0.8)
        remaining = int(qty * fill_pct)
        n_fills = random.randint(2, 8) if remaining > 1000 else 1
        filled_so_far = 0
        for f in range(n_fills):
            if filled_so_far >= remaining:
                break
            if f == n_fills - 1:
                fill_qty = remaining - filled_so_far
            else:
                fill_qty = random.randint(max(1, remaining // n_fills // 2), remaining // n_fills * 2)
                fill_qty = min(fill_qty, remaining - filled_so_far)
            if fill_qty <= 0:
                continue

            slippage = random.gauss(0, arrival_price * 0.0003)
            fill_price = round(arrival_price + slippage, 2)
            exec_time = created_at + timedelta(minutes=random.randint(1, 60))
            venue = random.choice(VENUES)
            exec_id = f"EXEC-{exec_counter:07d}"
            exec_counter += 1
            executions.append({
                "exec_id": exec_id,
                "order_id": order_id,
                "fill_qty": fill_qty,
                "fill_price": fill_price,
                "venue": venue,
                "exec_time": exec_time.strftime("%Y-%m-%d %H:%M:%S"),
            })
            filled_so_far += fill_qty

insert_batch("raw_orders", ["order_id", "trader_id", "etf_ticker", "direction", "qty", "order_type", "price_limit", "status", "created_at", "strategy", "arrival_price"], orders)
insert_batch("raw_executions", ["exec_id", "order_id", "fill_qty", "fill_price", "venue", "exec_time"], executions)
print(f"  ✓ {len(orders)} orders, {len(executions)} executions\n")


# ── Step 7: Volume JSON Snapshots ────────────────────────────────────────────

print("7. Uploading JSON snapshots to UC volume...")

# Market snapshots
for ticker in ETF_UNIVERSE:
    snapshot = {
        "ticker": ticker,
        "generated_at": NOW.isoformat(),
        "last_price": ETF_REF[ticker]["base_price"],
        "bid": round(ETF_REF[ticker]["base_price"] - 0.02, 2),
        "ask": round(ETF_REF[ticker]["base_price"] + 0.02, 2),
        "volume": random.randint(5_000_000, 50_000_000),
        "vwap": round(ETF_REF[ticker]["base_price"] * random.uniform(0.999, 1.001), 2),
        "intraday_high": round(ETF_REF[ticker]["base_price"] * 1.008, 2),
        "intraday_low": round(ETF_REF[ticker]["base_price"] * 0.993, 2),
    }
    path = f"{VOLUME_PATH}/snapshots/latest_{ticker}.json"
    w.files.upload(path, io.BytesIO(json.dumps(snapshot, indent=2).encode()), overwrite=True)
    print(f"  ✓ Uploaded {path}")

# Order summary JSON
order_summary = {
    "generated_at": NOW.isoformat(),
    "total_orders": len(orders),
    "total_executions": len(executions),
    "status_counts": {
        "FILLED": sum(1 for o in orders if o["status"] == "FILLED"),
        "PARTIAL": sum(1 for o in orders if o["status"] == "PARTIAL"),
        "CANCELLED": sum(1 for o in orders if o["status"] == "CANCELLED"),
        "PENDING": sum(1 for o in orders if o["status"] == "PENDING"),
    },
}
w.files.upload(f"{VOLUME_PATH}/order_summary.json", io.BytesIO(json.dumps(order_summary, indent=2).encode()), overwrite=True)
print(f"  ✓ Uploaded order summary JSON\n")


# ── Verification ─────────────────────────────────────────────────────────────

print("8. Row count verification...")
for table in ["etf_reference", "etf_constituents", "hedge_instruments", "traders", "raw_market_data", "raw_orders", "raw_executions"]:
    result = run_sql(f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.{table}")
    cnt = result.result.data_array[0][0]
    print(f"  {table}: {cnt} rows")

print("\n=== Seed complete! ===")
print(f"Next: run 02_simulator/market_data_simulator.py as a job, then deploy DLT pipeline.")
