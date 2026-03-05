# ETF Trading Execution & Order Management — Demo Guide

## Business Scenario

A buy-side trading desk is executing large ETF orders across SPY, QQQ, IVV, VTI, XLK, and XLF. Traders need real-time visibility into order status, execution quality (fill rate, slippage, participation rate), and the ability to take immediate action on live orders — cancel, force-execute, adjust size/price, or hedge with futures/options.

This demo shows how Databricks can power the full execution management stack:
- **Real-time market data ingestion** via a streaming simulator
- **Medallion pipeline** (Bronze → Silver → Gold) for analytics-ready data
- **Mutable order state** via Lakebase (PostgreSQL) for trader actions with full audit trail
- **Interactive trader UI** (Databricks App) with live charts and order management
- **Stakeholder analytics dashboard** (Lakeview) for risk managers and ops

---

## Architecture

```
[Market Data Simulator]  ──5s ticks──▶  raw_market_data (Delta Bronze)
                                                │
                                    [DLT Pipeline: Continuous]
                                    Bronze → Silver → Gold
                                                │                    │
                                    [Lakeview Dashboard]    [Lakebase PostgreSQL]
                                     (read-only analytics)   live_orders, order_actions
                                                                      │
                                              [Databricks App: React + FastAPI]
                                              - Live order blotter + KPI cards
                                              - Trader actions: Cancel / Execute / Adjust / Hedge
                                              - Intraday price charts (Recharts)
```

---

## Databricks Products Used

| Product | Role in Demo |
|---|---|
| **Delta Lake** | Append-only Bronze tables for market ticks, orders, executions |
| **Delta Live Tables (DLT)** | Continuous Bronze→Silver→Gold pipeline with deduplication, watermarks, and materialized views |
| **Lakebase (PostgreSQL)** | Mutable order state — real-time status updates, full audit trail for every trader action |
| **Databricks Apps** | Full-stack trader UI (React + FastAPI) deployed natively on Databricks |
| **Lakeview Dashboard** | Read-only analytics for stakeholders: execution quality, trader performance, market overview |
| **SQL Warehouse (Serverless)** | Powers all FastAPI backend queries and Lakeview dashboard queries |
| **Unity Catalog** | All tables and volumes in `jennifer_wang.etf_trading` with 3-layer namespace |
| **Databricks Jobs** | Always-on streaming simulator (5s loop) deployed as a continuously-running job |

---

## File Structure

```
etf-trading-demo/
├── 01_seed_data/
│   └── seed_etf_data.py          # Schema + all tables + 2-day historical backfill
│                                  # Run locally (not as cluster job — workspace IP ACL)
│
├── 02_simulator/
│   └── market_data_simulator.py  # PySpark streaming producer, 5s loop
│                                  # Deployed as always-on Databricks Job (Job ID: 32824723212989)
│
├── 03_dlt_pipeline/
│   └── etf_trading_pipeline.py   # DLT Continuous pipeline: Bronze → Silver → Gold
│                                  # Pipeline ID: 8800d221-7680-4d29-b678-cc9f6cd442c2
│
├── 04_lakebase/
│   └── lakebase_setup.py         # Provisions Lakebase instance, creates PG tables, initial sync
│
├── 05_app/
│   ├── app.py                    # FastAPI backend (10 endpoints)
│   ├── app.yaml                  # Databricks App config
│   ├── requirements.txt
│   └── frontend/
│       └── src/
│           ├── App.jsx
│           ├── MarketTicker.jsx  # Scrolling live ticker bar (auto-refresh 5s)
│           ├── KpiCards.jsx      # Fill rate, slippage, participation, active orders
│           ├── OrderBlotter.jsx  # Sortable table with per-row action buttons
│           ├── PriceChart.jsx    # Intraday price + VWAP line (Recharts)
│           ├── AnalyticsCharts.jsx
│           ├── AdjustModal.jsx   # Qty + price adjustment with notional preview
│           └── HedgeModal.jsx    # Futures/options instrument picker
│
└── 06_dashboard/
    └── deploy_dashboard.py       # Lakeview 4-page dashboard deploy script
```

---

## Data Model

### Unity Catalog Schema: `jennifer_wang.etf_trading`

#### Reference Tables (static)
| Table | Description |
|---|---|
| `etf_reference` | ETF metadata: AUM, benchmark, expense ratio, sector |
| `etf_constituents` | Top holdings per ETF with weights |
| `hedge_instruments` | Futures (/ES, /NQ) and SPY put options available for hedging |
| `traders` | Trader profiles: name, desk, risk limit |

#### Bronze (append-only, streaming landing)
| Table | Description |
|---|---|
| `raw_market_data` | Market ticks: bid/ask/last/volume/VWAP every 5s per ETF |
| `raw_orders` | Incoming orders: LIMIT/VWAP/TWAP/MKT across TWAP/VWAP/IS/POV strategies |
| `raw_executions` | Fill records: fill qty, fill price, venue, slippage |

#### Silver (DLT streaming tables — deduplicated, enriched)
| Table | Description |
|---|---|
| `market_data_silver` | Deduplicated ticks with mid_price, spread_bps, relative_volume |
| `orders_silver` | Orders joined with ETF reference, notional_value, time_in_market |
| `executions_silver` | Fills with slippage_bps computed vs arrival price |

#### Gold (DLT materialized views — aggregated)
| Table | Description |
|---|---|
| `market_snapshot_gold` | Latest tick per ticker: price, VWAP, spread, intraday H/L, % change |
| `order_analytics_gold` | Per-order: fill_rate, avg_slippage_bps, remaining_qty, vwap_vs_limit |
| `trading_performance_gold` | Per-trader per-day: total_orders, fill_rate_pct, avg_slippage_bps, notional |

#### Lakebase (PostgreSQL — mutable)
| Table | Description |
|---|---|
| `live_orders` | Current order state (mutable) — synced from Delta at startup |
| `order_actions` | Full audit trail of every trader action with before/after JSON |
| `hedge_requests` | Hedge orders linked to parent orders |

---

## ETF Universe

| Ticker | Name | Type |
|---|---|---|
| SPY | SPDR S&P 500 ETF | Equity |
| QQQ | Invesco QQQ (Nasdaq-100) | Equity |
| IVV | iShares Core S&P 500 ETF | Equity |
| VTI | Vanguard Total Stock Market | Equity |
| XLK | Technology Select Sector SPDR | Equity |
| XLF | Financial Select Sector SPDR | Equity |
| /ES | S&P 500 E-mini Futures | Hedge instrument |
| /NQ | Nasdaq-100 E-mini Futures | Hedge instrument |
| SPY Puts | SPY Put Options (various strikes) | Hedge instrument |

---

## API Endpoints (FastAPI Backend)

| Method | Path | Data Source | Description |
|---|---|---|---|
| GET | `/api/orders` | Lakebase → `order_analytics_gold` fallback | Live order blotter |
| GET | `/api/market` | `market_snapshot_gold` | Latest prices per ETF |
| GET | `/api/analytics` | `order_analytics_gold` | Execution quality metrics |
| GET | `/api/performance` | `trading_performance_gold` | Per-trader daily stats |
| GET | `/api/hedge-instruments` | `hedge_instruments` | Available futures/options |
| GET | `/api/price-history` | `raw_market_data` | Intraday ticks for charting |
| POST | `/api/orders/{id}/cancel` | **Lakebase** | Cancel order + audit log |
| POST | `/api/orders/{id}/execute` | **Lakebase** | Force fill at market price |
| PUT | `/api/orders/{id}/size` | **Lakebase** | Adjust order quantity |
| PUT | `/api/orders/{id}/price` | **Lakebase** | Adjust price limit |
| POST | `/api/orders/{id}/hedge` | **Lakebase** | Submit hedge request |

---

## Deployment

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- SQL Warehouse (Serverless recommended)
- Databricks CLI authenticated

### Step 1 — Seed Data (run locally, not as cluster job)
```bash
cd 01_seed_data
pip install databricks-sdk
python seed_etf_data.py
# Creates schema, all tables, seeds 936 ticks + 150 orders + 431 executions
```
> **Note:** Run locally because the cluster's outbound IP is blocked by the workspace IP ACL, preventing the SDK from calling the warehouse API from within the cluster.

### Step 2 — Start Market Data Simulator
Deploy `02_simulator/market_data_simulator.py` as a Databricks Job:
- Cluster: Single-Node, DBR 15.4 LTS, `data_security_mode: SINGLE_USER`
- Schedule: Continuous (no schedule — runs forever)
- Writes 6 market ticks every 5 seconds to `raw_market_data`

### Step 3 — Deploy DLT Pipeline
```
Pipeline name: etf_trading_pipeline
Source: 03_dlt_pipeline/etf_trading_pipeline.py
Target: jennifer_wang.etf_trading
Mode: Continuous
Compute: Serverless
```

### Step 4 — Setup Lakebase (optional — app falls back to Delta if unavailable)
```bash
cd 04_lakebase
pip install databricks-sdk psycopg2-binary
python lakebase_setup.py
```

### Step 5 — Deploy Databricks App
```bash
cd 05_app/frontend
npm install && npm run build

cd ..
databricks sync . /Users/<you>@databricks.com/etf-trading-demo/05_app \
  --exclude "frontend/node_modules/**"

databricks apps deploy <app-name> \
  --source-code-path /Workspace/Users/<you>@databricks.com/etf-trading-demo/05_app
```

Grant the app's service principal access to UC:
```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `<app-sp-client-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.etf_trading TO `<app-sp-client-id>`;
GRANT SELECT ON SCHEMA <catalog>.etf_trading TO `<app-sp-client-id>`;
```

### Step 6 — Deploy Lakeview Dashboard
```bash
cd 06_dashboard
pip install databricks-sdk
python deploy_dashboard.py
```

---

## Known Issues & Fixes Applied

| Issue | Root Cause | Fix |
|---|---|---|
| Seed job fails on cluster | Cluster outbound IP blocked by workspace IP ACL | Run `seed_etf_data.py` locally |
| Simulator job fails | `data_security_mode` not set → UC disabled | Set `SINGLE_USER` on cluster config |
| App shows no data | App service principal lacks UC permissions | Grant `USE CATALOG`, `USE SCHEMA`, `SELECT` to app SP |
| `/api/orders` 500 error | Fallback query referenced non-existent columns in `raw_orders` | Changed fallback to query `order_analytics_gold` |
| Intraday chart empty | Simulator was failing → no new ticks with today's timestamps | Fix simulator cluster mode and restart |
| DLT stuck `WAITING_FOR_RESOURCES` | Classic cluster slow provisioning | Set `serverless: true` on pipeline |

---

## Live Resources (jennifer.wang workspace)

| Resource | ID / URL |
|---|---|
| Workspace | https://adb-3311028655009738.18.azuredatabricks.net |
| SQL Warehouse | `65bc200a57dac15e` |
| DLT Pipeline | `8800d221-7680-4d29-b678-cc9f6cd442c2` |
| Simulator Job | `32824723212989` |
| Databricks App | https://manufacturing-assistant-3311028655009738.18.azure.databricksapps.com |
| Lakeview Dashboard | https://adb-3311028655009738.18.azuredatabricks.net/dashboardsv3/01f1184b60201434adb7568a0d639a6a/published |
