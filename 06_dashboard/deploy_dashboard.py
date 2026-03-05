"""
ETF Trading Demo — Lakeview Dashboard Deployment
Creates a 4-page Lakeview (AI/BI) dashboard with Order Monitor, Market Overview,
Execution Analytics, and Trader Performance pages.

Requires: pip install databricks-sdk
Run after DLT pipeline has populated silver/gold tables.
"""

import json
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import dashboards as db_dash

w = WorkspaceClient()

CATALOG = "jennifer_wang"
SCHEMA = "etf_trading"
DASHBOARD_NAME = "ETF Trading — Execution & Order Management"

# ── Dataset definitions ────────────────────────────────────────────────────────

DATASETS = [
    {
        "name": "active_orders",
        "displayName": "Active Orders",
        "query": f"""
SELECT
    o.order_id,
    o.etf_ticker,
    o.trader_id,
    o.direction,
    o.qty,
    COALESCE(a.filled_qty, 0) AS filled_qty,
    o.price_limit,
    o.order_type,
    o.status,
    o.strategy,
    o.created_at,
    o.notional_value,
    COALESCE(a.fill_rate * 100, 0) AS fill_rate_pct,
    COALESCE(a.avg_slippage_bps, 0) AS avg_slippage_bps,
    COALESCE(a.estimated_completion_pct, 0) AS estimated_completion_pct
FROM {CATALOG}.{SCHEMA}.orders_silver o
LEFT JOIN {CATALOG}.{SCHEMA}.order_analytics_gold a USING (order_id)
ORDER BY o.created_at DESC
""",
    },
    {
        "name": "market_snapshot",
        "displayName": "Market Snapshot",
        "query": f"""
SELECT
    ticker,
    last_price,
    bid,
    ask,
    spread_bps,
    total_volume,
    vwap,
    intraday_high,
    intraday_low,
    price_change_pct,
    relative_volume,
    event_time
FROM {CATALOG}.{SCHEMA}.market_snapshot_gold
ORDER BY ticker
""",
    },
    {
        "name": "execution_analytics",
        "displayName": "Execution Analytics",
        "query": f"""
SELECT
    order_id,
    trader_id,
    etf_ticker,
    direction,
    qty,
    filled_qty,
    fill_rate * 100 AS fill_rate_pct,
    avg_slippage_bps,
    avg_fill_price,
    notional_value,
    order_type,
    strategy,
    status,
    created_at,
    vwap_vs_limit
FROM {CATALOG}.{SCHEMA}.order_analytics_gold
ORDER BY created_at DESC
""",
    },
    {
        "name": "trader_performance",
        "displayName": "Trader Performance",
        "query": f"""
SELECT
    tp.trader_id,
    t.name AS trader_name,
    t.desk,
    tp.trade_date,
    tp.total_orders,
    tp.filled_orders,
    tp.active_orders,
    tp.fill_rate_pct,
    tp.avg_slippage_bps,
    tp.total_notional_usd,
    tp.total_filled_notional_usd,
    tp.unique_etfs_traded
FROM {CATALOG}.{SCHEMA}.trading_performance_gold tp
LEFT JOIN {CATALOG}.{SCHEMA}.traders t ON tp.trader_id = t.trader_id
ORDER BY tp.trade_date DESC, tp.total_notional_usd DESC
""",
    },
    {
        "name": "price_history",
        "displayName": "Price History",
        "query": f"""
SELECT
    ticker,
    event_time,
    last_price,
    vwap,
    bid,
    ask,
    volume
FROM {CATALOG}.{SCHEMA}.market_data_silver
WHERE event_time >= NOW() - INTERVAL '1 day'
ORDER BY ticker, event_time
""",
    },
]

# ── Dashboard definition ───────────────────────────────────────────────────────

def build_dashboard_spec():
    """Build the complete dashboard JSON spec."""
    return {
        "displayName": DASHBOARD_NAME,
        "serializedDashboard": json.dumps({
            "pages": [
                build_order_monitor_page(),
                build_market_overview_page(),
                build_execution_analytics_page(),
                build_trader_performance_page(),
            ],
            "datasets": DATASETS,
        }),
    }


def widget(id, type, title, dataset, x, y, w, h, config=None):
    """Helper: build a widget dict."""
    return {
        "name": id,
        "title": title,
        "position": {"x": x, "y": y, "width": w, "height": h},
        "widget": {
            "spec": {
                "encodings": config or {},
                "frame": {"showTitle": True},
                "version": 2,
                "widgetType": type,
            },
            "queries": [{"name": "main_query", "query": {"datasetName": dataset}}],
        },
    }


def counter_widget(id, title, dataset, field, fmt, x, y):
    return {
        "name": id,
        "title": title,
        "position": {"x": x, "y": y, "width": 1, "height": 2},
        "widget": {
            "spec": {
                "encodings": {
                    "value": {"fieldName": field, "displayName": title},
                    "format": {"numberFormat": fmt},
                },
                "frame": {"showTitle": True},
                "version": 2,
                "widgetType": "counter",
            },
            "queries": [{"name": "main_query", "query": {"datasetName": dataset}}],
        },
    }


def build_order_monitor_page():
    return {
        "name": "order_monitor",
        "displayName": "Order Monitor",
        "layout": {
            "width": 6,
            "widgets": [
                # KPI counters
                counter_widget("kpi_active", "Active Orders", "active_orders", "order_id", "d", 0, 0),
                counter_widget("kpi_fill_rate", "Avg Fill Rate %", "execution_analytics", "fill_rate_pct", ".1f", 1, 0),
                counter_widget("kpi_slippage", "Avg Slippage bps", "execution_analytics", "avg_slippage_bps", ".2f", 2, 0),
                counter_widget("kpi_notional", "Total Notional $M", "active_orders", "notional_value", ",.0f", 3, 0),
                # Order table
                {
                    "name": "order_table",
                    "title": "Order Blotter",
                    "position": {"x": 0, "y": 2, "width": 6, "height": 5},
                    "widget": {
                        "spec": {
                            "encodings": {
                                "columns": [
                                    {"fieldName": "order_id", "displayName": "Order ID"},
                                    {"fieldName": "etf_ticker", "displayName": "ETF"},
                                    {"fieldName": "direction", "displayName": "Side"},
                                    {"fieldName": "qty", "displayName": "Qty", "format": {"numberFormat": ",d"}},
                                    {"fieldName": "filled_qty", "displayName": "Filled", "format": {"numberFormat": ",d"}},
                                    {"fieldName": "fill_rate_pct", "displayName": "Fill %", "format": {"numberFormat": ".1f"}},
                                    {"fieldName": "price_limit", "displayName": "Limit $", "format": {"numberFormat": ".2f"}},
                                    {"fieldName": "order_type", "displayName": "Type"},
                                    {"fieldName": "strategy", "displayName": "Strategy"},
                                    {"fieldName": "status", "displayName": "Status"},
                                    {"fieldName": "avg_slippage_bps", "displayName": "Slippage bps", "format": {"numberFormat": ".2f"}},
                                    {"fieldName": "notional_value", "displayName": "Notional", "format": {"numberFormat": ",.0f"}},
                                    {"fieldName": "created_at", "displayName": "Created"},
                                ]
                            },
                            "frame": {"showTitle": True},
                            "version": 2,
                            "widgetType": "table",
                        },
                        "queries": [{"name": "main_query", "query": {"datasetName": "active_orders"}}],
                    },
                },
                # Status distribution bar
                {
                    "name": "status_dist",
                    "title": "Order Status Distribution",
                    "position": {"x": 0, "y": 7, "width": 3, "height": 3},
                    "widget": {
                        "spec": {
                            "encodings": {
                                "x": {"fieldName": "status", "displayName": "Status", "scale": {"type": "ordinal"}},
                                "y": {"fieldName": "order_id", "displayName": "Count", "aggregation": "count"},
                                "color": {"fieldName": "status", "displayName": "Status"},
                            },
                            "frame": {"showTitle": True},
                            "version": 2,
                            "widgetType": "bar",
                        },
                        "queries": [{"name": "main_query", "query": {"datasetName": "active_orders"}}],
                    },
                },
                # Fill rate vs notional scatter
                {
                    "name": "fill_scatter",
                    "title": "Fill Rate vs Notional",
                    "position": {"x": 3, "y": 7, "width": 3, "height": 3},
                    "widget": {
                        "spec": {
                            "encodings": {
                                "x": {"fieldName": "notional_value", "displayName": "Notional ($)", "scale": {"type": "quantitative"}},
                                "y": {"fieldName": "fill_rate_pct", "displayName": "Fill Rate %", "scale": {"type": "quantitative"}},
                                "color": {"fieldName": "strategy", "displayName": "Strategy"},
                                "size": {"fieldName": "qty", "displayName": "Qty"},
                            },
                            "frame": {"showTitle": True},
                            "version": 2,
                            "widgetType": "scatter",
                        },
                        "queries": [{"name": "main_query", "query": {"datasetName": "execution_analytics"}}],
                    },
                },
            ],
        },
    }


def build_market_overview_page():
    return {
        "name": "market_overview",
        "displayName": "Market Overview",
        "layout": {
            "width": 6,
            "widgets": [
                # Market snapshot table
                {
                    "name": "market_table",
                    "title": "ETF Market Snapshot",
                    "position": {"x": 0, "y": 0, "width": 6, "height": 3},
                    "widget": {
                        "spec": {
                            "encodings": {
                                "columns": [
                                    {"fieldName": "ticker", "displayName": "ETF"},
                                    {"fieldName": "last_price", "displayName": "Last", "format": {"numberFormat": ".2f"}},
                                    {"fieldName": "bid", "displayName": "Bid", "format": {"numberFormat": ".2f"}},
                                    {"fieldName": "ask", "displayName": "Ask", "format": {"numberFormat": ".2f"}},
                                    {"fieldName": "spread_bps", "displayName": "Spread bps", "format": {"numberFormat": ".2f"}},
                                    {"fieldName": "price_change_pct", "displayName": "Chg %", "format": {"numberFormat": "+.2f"}},
                                    {"fieldName": "vwap", "displayName": "VWAP", "format": {"numberFormat": ".2f"}},
                                    {"fieldName": "intraday_high", "displayName": "High", "format": {"numberFormat": ".2f"}},
                                    {"fieldName": "intraday_low", "displayName": "Low", "format": {"numberFormat": ".2f"}},
                                    {"fieldName": "total_volume", "displayName": "Volume", "format": {"numberFormat": ",d"}},
                                    {"fieldName": "relative_volume", "displayName": "Rel Vol", "format": {"numberFormat": ".2f"}},
                                ]
                            },
                            "frame": {"showTitle": True},
                            "version": 2,
                            "widgetType": "table",
                        },
                        "queries": [{"name": "main_query", "query": {"datasetName": "market_snapshot"}}],
                    },
                },
                # Intraday price line chart
                {
                    "name": "price_chart",
                    "title": "Intraday Price",
                    "position": {"x": 0, "y": 3, "width": 4, "height": 4},
                    "widget": {
                        "spec": {
                            "encodings": {
                                "x": {"fieldName": "event_time", "displayName": "Time"},
                                "y": {"fieldName": "last_price", "displayName": "Price"},
                                "color": {"fieldName": "ticker", "displayName": "ETF"},
                            },
                            "frame": {"showTitle": True},
                            "version": 2,
                            "widgetType": "line",
                        },
                        "queries": [{"name": "main_query", "query": {"datasetName": "price_history"}}],
                    },
                },
                # Volume bar chart
                {
                    "name": "volume_bar",
                    "title": "Volume by ETF",
                    "position": {"x": 4, "y": 3, "width": 2, "height": 4},
                    "widget": {
                        "spec": {
                            "encodings": {
                                "x": {"fieldName": "ticker", "displayName": "ETF", "scale": {"type": "ordinal"}},
                                "y": {"fieldName": "total_volume", "displayName": "Volume"},
                                "color": {"fieldName": "ticker", "displayName": "ETF"},
                            },
                            "frame": {"showTitle": True},
                            "version": 2,
                            "widgetType": "bar",
                        },
                        "queries": [{"name": "main_query", "query": {"datasetName": "market_snapshot"}}],
                    },
                },
            ],
        },
    }


def build_execution_analytics_page():
    return {
        "name": "execution_analytics",
        "displayName": "Execution Analytics",
        "layout": {
            "width": 6,
            "widgets": [
                # KPIs
                counter_widget("ea_fill_rate", "Avg Fill Rate %", "execution_analytics", "fill_rate_pct", ".1f", 0, 0),
                counter_widget("ea_slippage", "Avg Slippage bps", "execution_analytics", "avg_slippage_bps", ".2f", 1, 0),
                counter_widget("ea_filled_notional", "Filled Notional $", "execution_analytics", "notional_value", ",.0f", 2, 0),
                counter_widget("ea_order_count", "Total Orders", "execution_analytics", "order_id", "d", 3, 0),
                # Slippage by ETF × strategy heatmap (bar)
                {
                    "name": "slippage_by_etf",
                    "title": "Avg Slippage by ETF",
                    "position": {"x": 0, "y": 2, "width": 3, "height": 4},
                    "widget": {
                        "spec": {
                            "encodings": {
                                "x": {"fieldName": "etf_ticker", "displayName": "ETF", "scale": {"type": "ordinal"}},
                                "y": {"fieldName": "avg_slippage_bps", "displayName": "Avg Slippage bps", "aggregation": "avg"},
                                "color": {"fieldName": "etf_ticker", "displayName": "ETF"},
                            },
                            "frame": {"showTitle": True},
                            "version": 2,
                            "widgetType": "bar",
                        },
                        "queries": [{"name": "main_query", "query": {"datasetName": "execution_analytics"}}],
                    },
                },
                # Fill rate by order type
                {
                    "name": "fill_by_type",
                    "title": "Fill Rate by Order Type",
                    "position": {"x": 3, "y": 2, "width": 3, "height": 4},
                    "widget": {
                        "spec": {
                            "encodings": {
                                "x": {"fieldName": "order_type", "displayName": "Order Type", "scale": {"type": "ordinal"}},
                                "y": {"fieldName": "fill_rate_pct", "displayName": "Fill Rate %", "aggregation": "avg"},
                                "color": {"fieldName": "order_type", "displayName": "Order Type"},
                            },
                            "frame": {"showTitle": True},
                            "version": 2,
                            "widgetType": "bar",
                        },
                        "queries": [{"name": "main_query", "query": {"datasetName": "execution_analytics"}}],
                    },
                },
                # Fills over time line
                {
                    "name": "fills_over_time",
                    "title": "Order Volume Over Time",
                    "position": {"x": 0, "y": 6, "width": 6, "height": 3},
                    "widget": {
                        "spec": {
                            "encodings": {
                                "x": {"fieldName": "created_at", "displayName": "Time"},
                                "y": {"fieldName": "order_id", "displayName": "Order Count", "aggregation": "count"},
                                "color": {"fieldName": "strategy", "displayName": "Strategy"},
                            },
                            "frame": {"showTitle": True},
                            "version": 2,
                            "widgetType": "line",
                        },
                        "queries": [{"name": "main_query", "query": {"datasetName": "execution_analytics"}}],
                    },
                },
            ],
        },
    }


def build_trader_performance_page():
    return {
        "name": "trader_performance",
        "displayName": "Trader Performance",
        "layout": {
            "width": 6,
            "widgets": [
                # Trader stats table
                {
                    "name": "trader_table",
                    "title": "Trader Performance Summary",
                    "position": {"x": 0, "y": 0, "width": 6, "height": 4},
                    "widget": {
                        "spec": {
                            "encodings": {
                                "columns": [
                                    {"fieldName": "trader_name", "displayName": "Trader"},
                                    {"fieldName": "desk", "displayName": "Desk"},
                                    {"fieldName": "trade_date", "displayName": "Date"},
                                    {"fieldName": "total_orders", "displayName": "Orders", "format": {"numberFormat": "d"}},
                                    {"fieldName": "filled_orders", "displayName": "Filled", "format": {"numberFormat": "d"}},
                                    {"fieldName": "fill_rate_pct", "displayName": "Fill Rate %", "format": {"numberFormat": ".1f"}},
                                    {"fieldName": "avg_slippage_bps", "displayName": "Slippage bps", "format": {"numberFormat": ".2f"}},
                                    {"fieldName": "total_notional_usd", "displayName": "Notional $M", "format": {"numberFormat": ",.0f"}},
                                    {"fieldName": "unique_etfs_traded", "displayName": "ETFs", "format": {"numberFormat": "d"}},
                                ]
                            },
                            "frame": {"showTitle": True},
                            "version": 2,
                            "widgetType": "table",
                        },
                        "queries": [{"name": "main_query", "query": {"datasetName": "trader_performance"}}],
                    },
                },
                # Slippage by trader bar
                {
                    "name": "slippage_by_trader",
                    "title": "Avg Slippage by Trader",
                    "position": {"x": 0, "y": 4, "width": 3, "height": 3},
                    "widget": {
                        "spec": {
                            "encodings": {
                                "x": {"fieldName": "trader_name", "displayName": "Trader", "scale": {"type": "ordinal"}},
                                "y": {"fieldName": "avg_slippage_bps", "displayName": "Avg Slippage bps"},
                                "color": {"fieldName": "desk", "displayName": "Desk"},
                            },
                            "frame": {"showTitle": True},
                            "version": 2,
                            "widgetType": "bar",
                        },
                        "queries": [{"name": "main_query", "query": {"datasetName": "trader_performance"}}],
                    },
                },
                # Notional by trader
                {
                    "name": "notional_by_trader",
                    "title": "Total Notional by Trader",
                    "position": {"x": 3, "y": 4, "width": 3, "height": 3},
                    "widget": {
                        "spec": {
                            "encodings": {
                                "x": {"fieldName": "trader_name", "displayName": "Trader", "scale": {"type": "ordinal"}},
                                "y": {"fieldName": "total_notional_usd", "displayName": "Total Notional ($)"},
                                "color": {"fieldName": "desk", "displayName": "Desk"},
                            },
                            "frame": {"showTitle": True},
                            "version": 2,
                            "widgetType": "bar",
                        },
                        "queries": [{"name": "main_query", "query": {"datasetName": "trader_performance"}}],
                    },
                },
            ],
        },
    }


# ── Deploy ────────────────────────────────────────────────────────────────────

print("=== ETF Trading Demo — Lakeview Dashboard Deployment ===\n")
print(f"Dashboard: {DASHBOARD_NAME}")

spec = build_dashboard_spec()

# Check for existing dashboard
print("\nChecking for existing dashboards...")
existing_id = None
try:
    all_dashboards = list(w.lakeview.list())
    for d in all_dashboards:
        if d.display_name == DASHBOARD_NAME:
            existing_id = d.dashboard_id
            print(f"  Found existing: {existing_id}")
            break
except Exception as e:
    print(f"  Could not list dashboards: {e}")

from databricks.sdk.service.dashboards import Dashboard

if existing_id:
    print(f"\nUpdating existing dashboard {existing_id}...")
    result = w.lakeview.update(
        dashboard_id=existing_id,
        dashboard=Dashboard(
            display_name=DASHBOARD_NAME,
            serialized_dashboard=spec["serializedDashboard"],
        ),
    )
    dashboard_id = existing_id
    print("  ✓ Dashboard updated")
else:
    print("\nCreating new dashboard...")
    result = w.lakeview.create(
        dashboard=Dashboard(
            display_name=DASHBOARD_NAME,
            serialized_dashboard=spec["serializedDashboard"],
        ),
    )
    dashboard_id = result.dashboard_id
    print(f"  ✓ Dashboard created: {dashboard_id}")

# Publish
print("\nPublishing dashboard...")
try:
    w.lakeview.publish(dashboard_id=dashboard_id)
    print("  ✓ Published")
except Exception as e:
    print(f"  Note: {e} (may need manual publish from UI)")

workspace_host = w.config.host.rstrip("/")
dashboard_url = f"{workspace_host}/dashboardsv3/{dashboard_id}"
print(f"\n=== Dashboard deployed! ===")
print(f"URL: {dashboard_url}")
print("\nPages:")
print("  1. Order Monitor — order blotter, status distribution, fill rate scatter")
print("  2. Market Overview — price snapshot, intraday line, volume bars")
print("  3. Execution Analytics — slippage by ETF, fill rate by type, volume over time")
print("  4. Trader Performance — per-trader stats, slippage, notional")
