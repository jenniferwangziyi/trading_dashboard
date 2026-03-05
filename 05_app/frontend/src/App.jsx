import { useState, useEffect, useCallback } from 'react'
import MarketTicker from './MarketTicker.jsx'
import KpiCards from './KpiCards.jsx'
import OrderBlotter from './OrderBlotter.jsx'
import PriceChart from './PriceChart.jsx'
import AnalyticsCharts from './AnalyticsCharts.jsx'
import AdjustModal from './AdjustModal.jsx'
import HedgeModal from './HedgeModal.jsx'

const API = ''  // Same-origin in Databricks App; override in dev via vite proxy

const styles = {
  app: { minHeight: '100vh', background: '#0f1117', color: '#e2e8f0', fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif' },
  header: { background: '#1a1d2e', borderBottom: '1px solid #2d3148', padding: '12px 20px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' },
  headerTitle: { fontSize: 18, fontWeight: 700, color: '#ff6b35', letterSpacing: '0.5px' },
  headerSub: { fontSize: 12, color: '#64748b', marginLeft: 8 },
  badge: { background: '#22c55e22', color: '#22c55e', border: '1px solid #22c55e55', borderRadius: 4, padding: '2px 8px', fontSize: 11, marginLeft: 12 },
  main: { padding: '16px 20px', maxWidth: 1800, margin: '0 auto' },
  topRow: { display: 'grid', gridTemplateColumns: 'auto 1fr', gap: 16, marginBottom: 16, alignItems: 'start' },
  midRow: { display: 'grid', gridTemplateColumns: '380px 1fr', gap: 16, marginBottom: 16 },
  card: { background: '#1a1d2e', border: '1px solid #2d3148', borderRadius: 8, overflow: 'hidden' },
  cardHeader: { background: '#1e2235', padding: '10px 16px', borderBottom: '1px solid #2d3148', fontSize: 12, fontWeight: 600, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.8px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' },
  refreshBtn: { background: 'none', border: '1px solid #2d3148', color: '#64748b', borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: 11 },
  lastUpdate: { fontSize: 10, color: '#475569' },
}

export default function App() {
  const [market, setMarket] = useState([])
  const [orders, setOrders] = useState([])
  const [analytics, setAnalytics] = useState([])
  const [performance, setPerformance] = useState([])
  const [priceHistory, setPriceHistory] = useState([])
  const [selectedTicker, setSelectedTicker] = useState('SPY')
  const [selectedOrder, setSelectedOrder] = useState(null)
  const [adjustModal, setAdjustModal] = useState(null)   // { order, type: 'size'|'price' }
  const [hedgeModal, setHedgeModal] = useState(null)     // order
  const [hedgeInstruments, setHedgeInstruments] = useState([])
  const [lastRefresh, setLastRefresh] = useState(null)
  const [loading, setLoading] = useState(true)

  const fetchAll = useCallback(async () => {
    try {
      const [mktRes, ordRes, anlRes, perfRes] = await Promise.all([
        fetch(`${API}/api/market`).then(r => r.json()),
        fetch(`${API}/api/orders?limit=100`).then(r => r.json()),
        fetch(`${API}/api/analytics`).then(r => r.json()).catch(() => ({ analytics: [] })),
        fetch(`${API}/api/performance`).then(r => r.json()).catch(() => ({ performance: [] })),
      ])
      setMarket(mktRes.market || [])
      setOrders(ordRes.orders || [])
      setAnalytics(anlRes.analytics || [])
      setPerformance(perfRes.performance || [])
      setLastRefresh(new Date())
      setLoading(false)
    } catch (e) {
      console.error('Fetch error:', e)
      setLoading(false)
    }
  }, [])

  const fetchPriceHistory = useCallback(async (ticker) => {
    try {
      const res = await fetch(`${API}/api/price-history?ticker=${ticker}&hours=8`)
      const data = await res.json()
      setPriceHistory(data.history || [])
    } catch (e) {
      console.error('Price history error:', e)
    }
  }, [])

  const fetchHedgeInstruments = useCallback(async () => {
    try {
      const res = await fetch(`${API}/api/hedge-instruments`)
      const data = await res.json()
      setHedgeInstruments(data.instruments || [])
    } catch (e) {}
  }, [])

  useEffect(() => {
    fetchAll()
    fetchHedgeInstruments()
    const interval = setInterval(fetchAll, 10000)
    return () => clearInterval(interval)
  }, [fetchAll, fetchHedgeInstruments])

  useEffect(() => {
    fetchPriceHistory(selectedTicker)
    const interval = setInterval(() => fetchPriceHistory(selectedTicker), 15000)
    return () => clearInterval(interval)
  }, [selectedTicker, fetchPriceHistory])

  // ── Order Actions ──────────────────────────────────────────────────────────

  const handleCancel = async (order) => {
    if (!window.confirm(`Cancel order ${order.order_id}?`)) return
    try {
      await fetch(`${API}/api/orders/${order.order_id}/cancel`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ trader_id: 'T001' }),
      })
      await fetchAll()
    } catch (e) { alert('Cancel failed: ' + e.message) }
  }

  const handleExecute = async (order) => {
    if (!window.confirm(`Force-execute order ${order.order_id} at market?`)) return
    try {
      await fetch(`${API}/api/orders/${order.order_id}/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ trader_id: 'T001' }),
      })
      await fetchAll()
    } catch (e) { alert('Execute failed: ' + e.message) }
  }

  const handleAdjustSubmit = async (values) => {
    const { order, type } = adjustModal
    try {
      const endpoint = type === 'size' ? 'size' : 'price'
      const body = type === 'size'
        ? { qty: parseInt(values.qty), trader_id: 'T001' }
        : { price_limit: parseFloat(values.price_limit), trader_id: 'T001' }
      await fetch(`${API}/api/orders/${order.order_id}/${endpoint}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      setAdjustModal(null)
      await fetchAll()
    } catch (e) { alert('Adjust failed: ' + e.message) }
  }

  const handleHedgeSubmit = async (values) => {
    try {
      await fetch(`${API}/api/orders/${hedgeModal.order_id}/hedge`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...values, trader_id: 'T001' }),
      })
      setHedgeModal(null)
      await fetchAll()
    } catch (e) { alert('Hedge failed: ' + e.message) }
  }

  // ── KPI Computation ────────────────────────────────────────────────────────

  const activeOrders = orders.filter(o => ['PENDING', 'PARTIAL'].includes(o.status))
  const filledOrders = orders.filter(o => o.status === 'FILLED')
  const kpis = {
    activeCount: activeOrders.length,
    fillRate: orders.length > 0
      ? Math.round((filledOrders.length / orders.length) * 1000) / 10
      : 0,
    avgSlippage: analytics.length > 0
      ? Math.round(analytics.reduce((s, a) => s + (parseFloat(a.avg_slippage_bps) || 0), 0) / analytics.length * 10) / 10
      : 0,
    participationRate: analytics.length > 0
      ? Math.round(analytics.reduce((s, a) => s + (parseFloat(a.fill_rate) || 0), 0) / analytics.length * 1000) / 10
      : 0,
  }

  return (
    <div style={styles.app}>
      {/* Header */}
      <div style={styles.header}>
        <div style={{ display: 'flex', alignItems: 'center' }}>
          <span style={styles.headerTitle}>ETF OMS</span>
          <span style={styles.headerSub}>Execution & Order Management</span>
          <span style={styles.badge}>● LIVE</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          {lastRefresh && (
            <span style={styles.lastUpdate}>
              Updated {lastRefresh.toLocaleTimeString()}
            </span>
          )}
          <button style={styles.refreshBtn} onClick={fetchAll}>↻ Refresh</button>
        </div>
      </div>

      {/* Market Ticker Bar */}
      <MarketTicker market={market} onSelectTicker={setSelectedTicker} selectedTicker={selectedTicker} />

      <div style={styles.main}>
        {/* KPI + Blotter Row */}
        <div style={{ display: 'grid', gridTemplateColumns: '200px 1fr', gap: 16, marginBottom: 16 }}>
          <KpiCards kpis={kpis} />
          <div style={styles.card}>
            <div style={styles.cardHeader}>
              <span>Order Blotter</span>
              <span style={styles.lastUpdate}>{orders.length} orders</span>
            </div>
            <OrderBlotter
              orders={orders}
              analytics={analytics}
              loading={loading}
              onCancel={handleCancel}
              onExecute={handleExecute}
              onAdjust={(order, type) => setAdjustModal({ order, type })}
              onHedge={(order) => setHedgeModal(order)}
            />
          </div>
        </div>

        {/* Charts Row */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
          <div style={styles.card}>
            <div style={styles.cardHeader}>
              <span>Intraday Price — {selectedTicker}</span>
              <div style={{ display: 'flex', gap: 6 }}>
                {['SPY', 'QQQ', 'IVV', 'VTI', 'XLK', 'XLF'].map(t => (
                  <button
                    key={t}
                    onClick={() => setSelectedTicker(t)}
                    style={{
                      background: selectedTicker === t ? '#ff6b35' : 'none',
                      border: `1px solid ${selectedTicker === t ? '#ff6b35' : '#2d3148'}`,
                      color: selectedTicker === t ? '#fff' : '#94a3b8',
                      borderRadius: 3, padding: '1px 6px', cursor: 'pointer', fontSize: 10
                    }}
                  >{t}</button>
                ))}
              </div>
            </div>
            <PriceChart history={priceHistory} ticker={selectedTicker} />
          </div>

          <div style={styles.card}>
            <div style={styles.cardHeader}><span>Execution Analytics</span></div>
            <AnalyticsCharts analytics={analytics} performance={performance} />
          </div>
        </div>
      </div>

      {/* Modals */}
      {adjustModal && (
        <AdjustModal
          order={adjustModal.order}
          type={adjustModal.type}
          onSubmit={handleAdjustSubmit}
          onClose={() => setAdjustModal(null)}
        />
      )}
      {hedgeModal && (
        <HedgeModal
          order={hedgeModal}
          instruments={hedgeInstruments}
          onSubmit={handleHedgeSubmit}
          onClose={() => setHedgeModal(null)}
        />
      )}
    </div>
  )
}
