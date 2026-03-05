"""
ETF Trading Demo — Lakebase (PostgreSQL) Setup
Provisions a Lakebase instance, creates tables for mutable order state,
and syncs initial data from Delta orders_silver via SQL warehouse.

Prerequisites:
  - Databricks CLI authenticated
  - `pip install databricks-sdk psycopg2-binary`
"""

import os
import json
import time
import subprocess
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# ── Config ────────────────────────────────────────────────────────────────────
CATALOG = "jennifer_wang"
SCHEMA = "etf_trading"
WAREHOUSE_ID = "65bc200a57dac15e"
LAKEBASE_INSTANCE_NAME = "etf-oms-lakebase"

w = WorkspaceClient()


# ── Helper: run SQL on warehouse ─────────────────────────────────────────────

def run_sql(statement: str) -> list:
    """Run SQL on SQL warehouse, return rows as list of lists."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        wait_timeout="30s",
    )
    for _ in range(60):
        if resp.status.state == StatementState.SUCCEEDED:
            break
        if resp.status.state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
            raise RuntimeError(f"SQL failed [{resp.status.state}]: {resp.status.error}")
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)

    rows = []
    if resp.result and resp.result.data_array:
        rows = resp.result.data_array
    return rows


# ── Step 1: Provision Lakebase instance ──────────────────────────────────────

print("=== Lakebase Setup ===\n")
print("1. Checking for existing Lakebase instance...")

result = subprocess.run(
    ["databricks", "lakebase", "list", "--output", "json"],
    capture_output=True, text=True,
)

instances = json.loads(result.stdout) if result.stdout.strip() else []
existing = next((i for i in instances if i.get("name") == LAKEBASE_INSTANCE_NAME), None)

if existing:
    instance_id = existing["instance_id"]
    print(f"  ✓ Found existing instance: {instance_id}")
else:
    print("  Creating new Lakebase instance...")
    create_result = subprocess.run(
        [
            "databricks", "lakebase", "create",
            "--name", LAKEBASE_INSTANCE_NAME,
            "--output", "json",
        ],
        capture_output=True, text=True,
    )
    instance_info = json.loads(create_result.stdout)
    instance_id = instance_info["instance_id"]
    print(f"  ✓ Created instance: {instance_id}")

    # Wait for RUNNING state
    print("  Waiting for instance to be RUNNING...", end="", flush=True)
    for _ in range(60):
        time.sleep(5)
        status_result = subprocess.run(
            ["databricks", "lakebase", "get", instance_id, "--output", "json"],
            capture_output=True, text=True,
        )
        info = json.loads(status_result.stdout)
        state = info.get("state", "UNKNOWN")
        print(f" {state}", end="", flush=True)
        if state == "RUNNING":
            break
    print()


# ── Step 2: Get connection credentials ───────────────────────────────────────

print("2. Getting Lakebase connection credentials...")
creds_result = subprocess.run(
    ["databricks", "lakebase", "get-connection-info", instance_id, "--output", "json"],
    capture_output=True, text=True,
)
creds = json.loads(creds_result.stdout)

DB_HOST = creds["host"]
DB_PORT = creds.get("port", 5432)
DB_NAME = creds.get("database", "postgres")
DB_USER = creds["username"]
DB_PASS = creds["password"]

print(f"  Host: {DB_HOST}:{DB_PORT}")
print(f"  Database: {DB_NAME}\n")


# ── Step 3: Create tables in Lakebase ────────────────────────────────────────

print("3. Creating Lakebase tables...")

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS live_orders (
    order_id          TEXT PRIMARY KEY,
    etf_ticker        TEXT NOT NULL,
    etf_name          TEXT,
    trader_id         TEXT NOT NULL,
    direction         TEXT NOT NULL CHECK (direction IN ('BUY', 'SELL')),
    qty               INTEGER NOT NULL,
    filled_qty        INTEGER NOT NULL DEFAULT 0,
    price_limit       DOUBLE PRECISION,
    order_type        TEXT,
    status            TEXT NOT NULL DEFAULT 'PENDING',
    strategy          TEXT,
    created_at        TIMESTAMP WITH TIME ZONE,
    last_updated      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    arrival_price     DOUBLE PRECISION,
    notional_value    DOUBLE PRECISION,
    avg_fill_price    DOUBLE PRECISION,
    avg_slippage_bps  DOUBLE PRECISION,
    fill_rate         DOUBLE PRECISION DEFAULT 0,
    assigned_hedge_id TEXT
)
""")
print("  ✓ live_orders")

cur.execute("""
CREATE TABLE IF NOT EXISTS order_actions (
    action_id   SERIAL PRIMARY KEY,
    order_id    TEXT NOT NULL,
    action_type TEXT NOT NULL,
    old_value   JSONB,
    new_value   JSONB,
    trader_id   TEXT,
    ts          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
)
""")
cur.execute("CREATE INDEX IF NOT EXISTS idx_order_actions_order_id ON order_actions(order_id)")
print("  ✓ order_actions")

cur.execute("""
CREATE TABLE IF NOT EXISTS hedge_requests (
    hedge_id         SERIAL PRIMARY KEY,
    parent_order_id  TEXT NOT NULL,
    instrument_id    TEXT NOT NULL,
    direction        TEXT NOT NULL,
    qty              INTEGER NOT NULL,
    hedge_type       TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'PENDING',
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
)
""")
print("  ✓ hedge_requests\n")


# ── Step 4: Sync orders from Delta into Lakebase ─────────────────────────────

print("4. Syncing orders from Delta (orders_silver) → Lakebase...")

# Try to read from orders_silver (DLT); fall back to raw_orders if pipeline not started yet
try:
    rows = run_sql(f"""
        SELECT
            o.order_id, o.etf_ticker, o.etf_name, o.trader_id, o.direction,
            o.qty,
            COALESCE(a.filled_qty, 0) AS filled_qty,
            o.price_limit, o.order_type, o.status, o.strategy,
            o.created_at, o.arrival_price, o.notional_value,
            a.avg_fill_price, a.avg_slippage_bps,
            COALESCE(a.fill_rate, 0) AS fill_rate
        FROM {CATALOG}.{SCHEMA}.orders_silver o
        LEFT JOIN {CATALOG}.{SCHEMA}.order_analytics_gold a ON o.order_id = a.order_id
        LIMIT 200
    """)
    source = "orders_silver + order_analytics_gold"
except Exception:
    rows = run_sql(f"""
        SELECT
            order_id, etf_ticker, NULL AS etf_name, trader_id, direction,
            qty, 0 AS filled_qty,
            price_limit, order_type, status, strategy,
            created_at, arrival_price,
            CAST(qty AS DOUBLE) * price_limit AS notional_value,
            NULL AS avg_fill_price, 0.0 AS avg_slippage_bps, 0.0 AS fill_rate
        FROM {CATALOG}.{SCHEMA}.raw_orders
        LIMIT 200
    """)
    source = "raw_orders (fallback)"

print(f"  Source: {source}")
print(f"  Fetched {len(rows)} orders from Delta\n")

if rows:
    insert_data = []
    for r in rows:
        insert_data.append((
            r[0],   # order_id
            r[1],   # etf_ticker
            r[2],   # etf_name
            r[3],   # trader_id
            r[4],   # direction
            int(r[5]) if r[5] else 0,   # qty
            int(r[6]) if r[6] else 0,   # filled_qty
            float(r[7]) if r[7] else None,  # price_limit
            r[8],   # order_type
            r[9],   # status
            r[10],  # strategy
            r[11],  # created_at
            float(r[12]) if r[12] else None,  # arrival_price
            float(r[13]) if r[13] else None,  # notional_value
            float(r[14]) if r[14] else None,  # avg_fill_price
            float(r[15]) if r[15] else 0.0,   # avg_slippage_bps
            float(r[16]) if r[16] else 0.0,   # fill_rate
        ))

    execute_values(cur, """
        INSERT INTO live_orders (
            order_id, etf_ticker, etf_name, trader_id, direction,
            qty, filled_qty, price_limit, order_type, status, strategy,
            created_at, arrival_price, notional_value,
            avg_fill_price, avg_slippage_bps, fill_rate
        ) VALUES %s
        ON CONFLICT (order_id) DO UPDATE SET
            status       = EXCLUDED.status,
            filled_qty   = EXCLUDED.filled_qty,
            avg_fill_price   = EXCLUDED.avg_fill_price,
            avg_slippage_bps = EXCLUDED.avg_slippage_bps,
            fill_rate    = EXCLUDED.fill_rate,
            last_updated = NOW()
    """, insert_data)
    print(f"  ✓ Upserted {len(insert_data)} orders into live_orders")


# ── Step 5: Verify ────────────────────────────────────────────────────────────

print("\n5. Verification...")
cur.execute("SELECT status, COUNT(*) FROM live_orders GROUP BY status ORDER BY COUNT(*) DESC")
rows_v = cur.fetchall()
for status, cnt in rows_v:
    print(f"  {status}: {cnt} orders")

cur.execute("SELECT COUNT(*) FROM order_actions")
print(f"  order_actions: {cur.fetchone()[0]} rows")
cur.execute("SELECT COUNT(*) FROM hedge_requests")
print(f"  hedge_requests: {cur.fetchone()[0]} rows")

cur.close()
conn.close()

print("\n=== Lakebase setup complete! ===")
print(f"Instance ID: {instance_id}")
print(f"Connection: {DB_HOST}:{DB_PORT}/{DB_NAME}")
print("\nNext: deploy the Databricks App (05_app/)")

# Write connection info to environment file for app
env_content = f"""# Lakebase connection — auto-generated by lakebase_setup.py
LAKEBASE_HOST={DB_HOST}
LAKEBASE_PORT={DB_PORT}
LAKEBASE_DB={DB_NAME}
LAKEBASE_USER={DB_USER}
LAKEBASE_PASSWORD={DB_PASS}
DATABRICKS_WAREHOUSE_ID={WAREHOUSE_ID}
CATALOG={CATALOG}
SCHEMA={SCHEMA}
"""
with open("../05_app/.env.lakebase", "w") as f:
    f.write(env_content)
print("\n✓ Wrote connection info to 05_app/.env.lakebase")
