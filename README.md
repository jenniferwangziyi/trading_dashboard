# ETF Trading Execution & Order Management Demo

## Architecture

```
[market_data_simulator.py]  →  raw_market_data (Delta, ~5s ticks)
                                      ↓
                           [DLT Pipeline: Continuous]
                        Bronze → Silver → Gold Delta tables
                                      ↓                    ↓
                           [Lakeview Dashboard]    [Lakebase PostgreSQL]
                            (read-only analytics)          ↓
                                               [Databricks App: React + FastAPI]
                                               - Order blotter + KPIs
                                               - Trader actions: cancel/exec/adjust/hedge
```

## Execution Order

### 1. Seed Data
```bash
cd 01_seed_data
databricks notebooks import seed_etf_data.py /ETF-Demo/seed_etf_data
databricks jobs run-now --job-id <job-id>
# OR run directly in a Databricks notebook
```

### 2. Start Simulator (always-on job)
```bash
# Deploy as a Databricks Job on Single-Node cluster
# File: 02_simulator/market_data_simulator.py
# Cluster: Single-Node, 14.x LTS ML (or standard)
# Schedule: Continuous / no schedule (runs forever)
```

### 3. Deploy DLT Pipeline
```
- Pipeline name: etf_trading_pipeline
- Source: 03_dlt_pipeline/etf_trading_pipeline.py
- Target catalog: jennifer_wang
- Target schema: etf_trading
- Mode: Continuous
```

### 4. Setup Lakebase
```bash
pip install databricks-sdk psycopg2-binary
cd 04_lakebase
python lakebase_setup.py
```

### 5. Deploy Databricks App
```bash
cd 05_app/frontend
npm install && npm run build

cd ..
databricks apps deploy etf-oms-dashboard --source-code-path .
```

### 6. Deploy Lakeview Dashboard
```bash
cd 06_dashboard
pip install databricks-sdk
python deploy_dashboard.py
```

## Files

| Path | Description |
|---|---|
| `01_seed_data/seed_etf_data.py` | Schema + tables + 2-day historical backfill |
| `02_simulator/market_data_simulator.py` | Streaming tick generator (5s loop) |
| `03_dlt_pipeline/etf_trading_pipeline.py` | DLT Bronze→Silver→Gold pipeline |
| `04_lakebase/lakebase_setup.py` | Lakebase provisioning + order sync |
| `05_app/app.py` | FastAPI backend (10 endpoints) |
| `05_app/frontend/` | React + Recharts trader UI |
| `06_dashboard/deploy_dashboard.py` | Lakeview 4-page dashboard deploy |

## ETF Universe
SPY, QQQ, IVV, VTI, XLK, XLF + /ES futures, /NQ futures, SPY put options

## Key Features
- **Live order blotter**: sortable, filterable, per-row actions
- **Trader actions**: Cancel / Force Execute / Adjust Size / Adjust Price / Hedge
- **Hedge modal**: futures or options instrument picker with delta-based quantity suggestion
- **Real-time charts**: intraday price + VWAP line, execution analytics scatter
- **Market ticker bar**: scrolling live prices for all ETFs
- **KPI cards**: fill rate, participation rate, slippage, active order count
