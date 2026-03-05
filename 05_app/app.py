"""
ETF Trading OMS — FastAPI Backend
Databricks App: reads from Lakebase (mutable order state) and Delta gold tables.
Environment variables are injected by Databricks Apps runtime or loaded from .env.lakebase.
"""

import os
import json
import time
from datetime import datetime, timezone
from typing import Optional
from contextlib import asynccontextmanager

import psycopg2
import psycopg2.extras
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── Config ────────────────────────────────────────────────────────────────────

# Load .env.lakebase if it exists (local dev)
env_file = os.path.join(os.path.dirname(__file__), ".env.lakebase")
if os.path.exists(env_file):
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

CATALOG = os.environ.get("CATALOG", "jennifer_wang")
SCHEMA = os.environ.get("SCHEMA", "etf_trading")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "65bc200a57dac15e")

DB_HOST = os.environ.get("LAKEBASE_HOST", "")
DB_PORT = int(os.environ.get("LAKEBASE_PORT", "5432"))
DB_NAME = os.environ.get("LAKEBASE_DB", "postgres")
DB_USER = os.environ.get("LAKEBASE_USER", "")
DB_PASS = os.environ.get("LAKEBASE_PASSWORD", "")

w = WorkspaceClient()

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_db_conn():
    """Get a new Lakebase PostgreSQL connection."""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, sslmode="require",
    )


def run_warehouse_sql(statement: str) -> list[dict]:
    """Execute SQL on SQL warehouse, return list of row dicts."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        wait_timeout="30s",
    )
    for _ in range(30):
        if resp.status.state == StatementState.SUCCEEDED:
            break
        if resp.status.state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
            raise RuntimeError(f"Warehouse SQL failed: {resp.status.error}")
        time.sleep(1)
        resp = w.statement_execution.get_statement(resp.statement_id)

    if not resp.result or not resp.result.data_array:
        return []

    cols = [c.name for c in resp.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in resp.result.data_array]


# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("ETF OMS API starting up...")
    yield
    print("ETF OMS API shutting down.")


app = FastAPI(title="ETF OMS Dashboard API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Request models ────────────────────────────────────────────────────────────

class AdjustSizeRequest(BaseModel):
    qty: int
    trader_id: Optional[str] = "T001"

class AdjustPriceRequest(BaseModel):
    price_limit: float
    trader_id: Optional[str] = "T001"

class HedgeRequest(BaseModel):
    instrument_id: str
    direction: str
    qty: int
    hedge_type: str
    trader_id: Optional[str] = "T001"

class ExecuteRequest(BaseModel):
    trader_id: Optional[str] = "T001"


# ── GET /api/orders ───────────────────────────────────────────────────────────

@app.get("/api/orders")
async def get_orders(status: Optional[str] = None, ticker: Optional[str] = None, limit: int = 100):
    """Fetch active orders from Lakebase."""
    try:
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        where_clauses = []
        params = []
        if status:
            where_clauses.append("status = ANY(%s)")
            params.append(status.split(","))
        if ticker:
            where_clauses.append("etf_ticker = %s")
            params.append(ticker)
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        cur.execute(
            f"SELECT * FROM live_orders {where_sql} ORDER BY created_at DESC LIMIT %s",
            params + [limit],
        )
        rows = [dict(r) for r in cur.fetchall()]
        # Convert datetimes to ISO strings
        for row in rows:
            for k, v in row.items():
                if isinstance(v, datetime):
                    row[k] = v.isoformat()
        cur.close()
        conn.close()
        return {"orders": rows, "count": len(rows)}
    except Exception as e:
        # Fallback to warehouse if Lakebase unavailable
        try:
            rows = run_warehouse_sql(f"""
                SELECT order_id, etf_ticker, trader_id, direction, qty,
                       COALESCE(filled_qty, 0) AS filled_qty,
                       remaining_qty, order_type, status, strategy,
                       created_at, notional_value,
                       ROUND(fill_rate * 100, 1) AS fill_pct,
                       ROUND(avg_slippage_bps, 2) AS avg_slippage_bps
                FROM {CATALOG}.{SCHEMA}.order_analytics_gold
                ORDER BY created_at DESC
                LIMIT 100
            """)
            return {"orders": rows, "count": len(rows), "source": "delta_fallback"}
        except Exception as e2:
            raise HTTPException(status_code=500, detail=str(e2))


# ── GET /api/market ───────────────────────────────────────────────────────────

@app.get("/api/market")
async def get_market():
    """Latest market snapshot from market_snapshot_gold."""
    try:
        rows = run_warehouse_sql(f"""
            SELECT ticker, last_price, bid, ask, mid_price, spread_bps,
                   volume, total_volume, vwap, intraday_high, intraday_low,
                   open_price, price_change_pct, relative_volume, event_time
            FROM {CATALOG}.{SCHEMA}.market_snapshot_gold
            ORDER BY ticker
        """)
        return {"market": rows, "timestamp": datetime.now(timezone.utc).isoformat()}
    except Exception:
        # Fallback: latest tick per ticker from raw table
        rows = run_warehouse_sql(f"""
            WITH ranked AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY event_time DESC) AS rn
                FROM {CATALOG}.{SCHEMA}.raw_market_data
            )
            SELECT ticker, last_price, bid, ask, volume, cum_volume AS total_volume, vwap, event_time
            FROM ranked WHERE rn = 1
            ORDER BY ticker
        """)
        return {"market": rows, "timestamp": datetime.now(timezone.utc).isoformat(), "source": "raw_fallback"}


# ── GET /api/analytics ────────────────────────────────────────────────────────

@app.get("/api/analytics")
async def get_analytics():
    """Order analytics from order_analytics_gold."""
    try:
        rows = run_warehouse_sql(f"""
            SELECT order_id, trader_id, etf_ticker, direction,
                   qty, filled_qty, remaining_qty, fill_rate,
                   avg_slippage_bps, avg_fill_price, notional_value,
                   estimated_completion_pct, vwap_vs_limit,
                   order_type, strategy, status, created_at,
                   first_fill_time, last_fill_time
            FROM {CATALOG}.{SCHEMA}.order_analytics_gold
            ORDER BY created_at DESC
            LIMIT 200
        """)
        return {"analytics": rows, "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Analytics unavailable (DLT may be starting): {e}")


# ── GET /api/performance ──────────────────────────────────────────────────────

@app.get("/api/performance")
async def get_performance():
    """Trader performance from trading_performance_gold."""
    try:
        rows = run_warehouse_sql(f"""
            SELECT trader_id, trade_date, total_orders, filled_orders, active_orders,
                   fill_rate_pct, participation_rate, avg_slippage_bps,
                   total_notional_usd, total_filled_notional_usd, unique_etfs_traded
            FROM {CATALOG}.{SCHEMA}.trading_performance_gold
            ORDER BY trade_date DESC, trader_id
        """)
        return {"performance": rows, "count": len(rows)}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


# ── GET /api/hedge-instruments ────────────────────────────────────────────────

@app.get("/api/hedge-instruments")
async def get_hedge_instruments():
    """Available hedge instruments from etf_reference table."""
    rows = run_warehouse_sql(f"""
        SELECT instrument_id, type, underlying, expiry, strike, contract_size, delta
        FROM {CATALOG}.{SCHEMA}.hedge_instruments
        ORDER BY type, underlying
    """)
    return {"instruments": rows}


# ── POST /api/orders/{id}/cancel ──────────────────────────────────────────────

@app.post("/api/orders/{order_id}/cancel")
async def cancel_order(order_id: str, req: Request):
    body = await req.json() if req.headers.get("content-type", "").startswith("application/json") else {}
    trader_id = body.get("trader_id", "T001")
    try:
        conn = get_db_conn()
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("SELECT * FROM live_orders WHERE order_id = %s", (order_id,))
        order = cur.fetchone()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        if order["status"] in ("FILLED", "CANCELLED"):
            raise HTTPException(status_code=400, detail=f"Cannot cancel order in state {order['status']}")

        cur.execute(
            "UPDATE live_orders SET status = 'CANCELLED', last_updated = NOW() WHERE order_id = %s",
            (order_id,)
        )
        cur.execute(
            """INSERT INTO order_actions (order_id, action_type, old_value, new_value, trader_id)
               VALUES (%s, 'CANCEL', %s, %s, %s)""",
            (order_id, json.dumps({"status": order["status"]}), json.dumps({"status": "CANCELLED"}), trader_id)
        )
        cur.close()
        conn.close()
        return {"success": True, "order_id": order_id, "new_status": "CANCELLED"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── POST /api/orders/{id}/execute ────────────────────────────────────────────

@app.post("/api/orders/{order_id}/execute")
async def execute_order(order_id: str, body: ExecuteRequest):
    """Force execute an order (mark as FILLED with current market price)."""
    try:
        # Get current market price
        market_rows = run_warehouse_sql(f"""
            SELECT ticker, last_price FROM {CATALOG}.{SCHEMA}.market_snapshot_gold LIMIT 10
        """)
        market_prices = {r["ticker"]: float(r["last_price"]) for r in market_rows}

        conn = get_db_conn()
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("SELECT * FROM live_orders WHERE order_id = %s", (order_id,))
        order = cur.fetchone()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        if order["status"] == "FILLED":
            raise HTTPException(status_code=400, detail="Order already filled")

        fill_price = market_prices.get(order["etf_ticker"], order["price_limit"])
        slippage = 0.0
        if order["arrival_price"] and order["arrival_price"] > 0:
            slippage = round((fill_price - order["arrival_price"]) / order["arrival_price"] * 10000, 2)

        cur.execute("""
            UPDATE live_orders SET
                status = 'FILLED',
                filled_qty = qty,
                avg_fill_price = %s,
                avg_slippage_bps = %s,
                fill_rate = 1.0,
                last_updated = NOW()
            WHERE order_id = %s
        """, (fill_price, slippage, order_id))
        cur.execute("""
            INSERT INTO order_actions (order_id, action_type, old_value, new_value, trader_id)
            VALUES (%s, 'EXECUTE', %s, %s, %s)
        """, (
            order_id,
            json.dumps({"status": order["status"], "filled_qty": order["filled_qty"]}),
            json.dumps({"status": "FILLED", "fill_price": fill_price, "slippage_bps": slippage}),
            body.trader_id,
        ))
        cur.close()
        conn.close()
        return {"success": True, "order_id": order_id, "fill_price": fill_price, "slippage_bps": slippage}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── PUT /api/orders/{id}/size ─────────────────────────────────────────────────

@app.put("/api/orders/{order_id}/size")
async def adjust_size(order_id: str, body: AdjustSizeRequest):
    """Adjust order quantity."""
    if body.qty <= 0:
        raise HTTPException(status_code=400, detail="qty must be positive")
    try:
        conn = get_db_conn()
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT qty, status, price_limit FROM live_orders WHERE order_id = %s", (order_id,))
        order = cur.fetchone()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        if order["status"] in ("FILLED", "CANCELLED"):
            raise HTTPException(status_code=400, detail="Cannot adjust a closed order")

        new_notional = body.qty * (order["price_limit"] or 0)
        cur.execute("""
            UPDATE live_orders SET qty = %s, notional_value = %s, last_updated = NOW()
            WHERE order_id = %s
        """, (body.qty, new_notional, order_id))
        cur.execute("""
            INSERT INTO order_actions (order_id, action_type, old_value, new_value, trader_id)
            VALUES (%s, 'ADJUST_SIZE', %s, %s, %s)
        """, (
            order_id,
            json.dumps({"qty": order["qty"]}),
            json.dumps({"qty": body.qty}),
            body.trader_id,
        ))
        cur.close()
        conn.close()
        return {"success": True, "order_id": order_id, "new_qty": body.qty}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── PUT /api/orders/{id}/price ────────────────────────────────────────────────

@app.put("/api/orders/{order_id}/price")
async def adjust_price(order_id: str, body: AdjustPriceRequest):
    """Adjust order price limit."""
    if body.price_limit <= 0:
        raise HTTPException(status_code=400, detail="price_limit must be positive")
    try:
        conn = get_db_conn()
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT price_limit, status, qty FROM live_orders WHERE order_id = %s", (order_id,))
        order = cur.fetchone()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        if order["status"] in ("FILLED", "CANCELLED"):
            raise HTTPException(status_code=400, detail="Cannot adjust a closed order")

        new_notional = (order["qty"] or 0) * body.price_limit
        cur.execute("""
            UPDATE live_orders SET price_limit = %s, notional_value = %s, last_updated = NOW()
            WHERE order_id = %s
        """, (body.price_limit, new_notional, order_id))
        cur.execute("""
            INSERT INTO order_actions (order_id, action_type, old_value, new_value, trader_id)
            VALUES (%s, 'ADJUST_PRICE', %s, %s, %s)
        """, (
            order_id,
            json.dumps({"price_limit": float(order["price_limit"]) if order["price_limit"] else None}),
            json.dumps({"price_limit": body.price_limit}),
            body.trader_id,
        ))
        cur.close()
        conn.close()
        return {"success": True, "order_id": order_id, "new_price_limit": body.price_limit}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── POST /api/orders/{id}/hedge ───────────────────────────────────────────────

@app.post("/api/orders/{order_id}/hedge")
async def submit_hedge(order_id: str, body: HedgeRequest):
    """Submit a hedge request for an order."""
    try:
        conn = get_db_conn()
        conn.autocommit = True
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("SELECT * FROM live_orders WHERE order_id = %s", (order_id,))
        order = cur.fetchone()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")

        cur.execute("""
            INSERT INTO hedge_requests (parent_order_id, instrument_id, direction, qty, hedge_type, status)
            VALUES (%s, %s, %s, %s, %s, 'PENDING')
            RETURNING hedge_id
        """, (order_id, body.instrument_id, body.direction, body.qty, body.hedge_type))
        hedge_id = cur.fetchone()["hedge_id"]

        cur.execute("""
            UPDATE live_orders SET assigned_hedge_id = %s, last_updated = NOW()
            WHERE order_id = %s
        """, (str(hedge_id), order_id))

        cur.execute("""
            INSERT INTO order_actions (order_id, action_type, old_value, new_value, trader_id)
            VALUES (%s, 'HEDGE', %s, %s, %s)
        """, (
            order_id,
            json.dumps({"assigned_hedge_id": order.get("assigned_hedge_id")}),
            json.dumps({"hedge_id": hedge_id, "instrument_id": body.instrument_id, "qty": body.qty}),
            body.trader_id,
        ))
        cur.close()
        conn.close()
        return {"success": True, "hedge_id": hedge_id, "order_id": order_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── GET /api/price-history ────────────────────────────────────────────────────

@app.get("/api/price-history")
async def get_price_history(ticker: str = "SPY", hours: int = 8):
    """Intraday price history for charting."""
    rows = run_warehouse_sql(f"""
        SELECT event_time, last_price, bid, ask, volume, vwap
        FROM {CATALOG}.{SCHEMA}.raw_market_data
        WHERE ticker = '{ticker}'
          AND event_time >= NOW() - INTERVAL '{hours} hours'
        ORDER BY event_time ASC
        LIMIT 500
    """)
    return {"ticker": ticker, "history": rows}


# ── Serve React frontend ──────────────────────────────────────────────────────

FRONTEND_DIST = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.isdir(FRONTEND_DIST):
    app.mount("/assets", StaticFiles(directory=os.path.join(FRONTEND_DIST, "assets")), name="assets")

    @app.get("/", include_in_schema=False)
    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str = ""):
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404)
        index = os.path.join(FRONTEND_DIST, "index.html")
        if os.path.exists(index):
            return FileResponse(index)
        raise HTTPException(status_code=404, detail="Frontend not built. Run: cd frontend && npm run build")
