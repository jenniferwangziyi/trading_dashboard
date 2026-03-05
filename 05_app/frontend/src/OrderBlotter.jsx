import { useState, useMemo } from 'react'

const STATUS_COLORS = {
  FILLED:    { bg: '#14532d', text: '#22c55e', border: '#166534' },
  PARTIAL:   { bg: '#7c2d12', text: '#fb923c', border: '#9a3412' },
  PENDING:   { bg: '#1e3a5f', text: '#60a5fa', border: '#1d4ed8' },
  CANCELLED: { bg: '#1c1c2e', text: '#475569', border: '#334155' },
}

const DIR_COLOR = { BUY: '#22c55e', SELL: '#ef4444' }

const styles = {
  container: { overflowX: 'auto', overflowY: 'auto', maxHeight: '42vh' },
  table: { width: '100%', borderCollapse: 'collapse', fontSize: 12 },
  th: {
    background: '#12141f',
    padding: '7px 10px',
    textAlign: 'left',
    color: '#64748b',
    fontWeight: 600,
    fontSize: 10,
    textTransform: 'uppercase',
    letterSpacing: '0.6px',
    borderBottom: '1px solid #1e2235',
    position: 'sticky',
    top: 0,
    cursor: 'pointer',
    userSelect: 'none',
    whiteSpace: 'nowrap',
  },
  td: {
    padding: '6px 10px',
    borderBottom: '1px solid #1a1d2e',
    color: '#e2e8f0',
    whiteSpace: 'nowrap',
  },
  row: (selected) => ({
    background: selected ? '#1e2235' : 'transparent',
    cursor: 'pointer',
    transition: 'background 0.1s',
  }),
  badge: (status) => ({
    display: 'inline-block',
    padding: '1px 6px',
    borderRadius: 3,
    fontSize: 10,
    fontWeight: 600,
    background: STATUS_COLORS[status]?.bg || '#1c1c2e',
    color: STATUS_COLORS[status]?.text || '#94a3b8',
    border: `1px solid ${STATUS_COLORS[status]?.border || '#334155'}`,
  }),
  actionBtn: (color, disabled) => ({
    background: 'none',
    border: `1px solid ${disabled ? '#2d3148' : color + '44'}`,
    color: disabled ? '#334155' : color,
    borderRadius: 3,
    padding: '2px 6px',
    fontSize: 10,
    cursor: disabled ? 'not-allowed' : 'pointer',
    marginRight: 3,
    fontWeight: 500,
    transition: 'background 0.1s',
  }),
  fillBar: {
    height: 4,
    borderRadius: 2,
    background: '#1e2235',
    overflow: 'hidden',
    marginTop: 2,
    width: 60,
  },
  fillBarInner: (pct, color) => ({
    height: '100%',
    width: `${Math.min(100, pct)}%`,
    background: color,
    borderRadius: 2,
  }),
  sortArrow: { fontSize: 9, marginLeft: 3 },
  empty: { textAlign: 'center', padding: 40, color: '#475569', fontSize: 13 },
  filterRow: { display: 'flex', gap: 8, padding: '8px 12px', borderBottom: '1px solid #1e2235', alignItems: 'center' },
  filterBtn: (active) => ({
    background: active ? '#ff6b3522' : 'none',
    border: `1px solid ${active ? '#ff6b35' : '#2d3148'}`,
    color: active ? '#ff6b35' : '#64748b',
    borderRadius: 4,
    padding: '3px 10px',
    cursor: 'pointer',
    fontSize: 11,
    fontWeight: active ? 600 : 400,
  }),
  search: {
    background: '#12141f',
    border: '1px solid #2d3148',
    color: '#e2e8f0',
    borderRadius: 4,
    padding: '3px 10px',
    fontSize: 11,
    outline: 'none',
    marginLeft: 'auto',
    width: 180,
  },
}

const fmt = (n, dec = 2) => n == null ? '--' : Number(n).toFixed(dec)
const fmtN = (n) => n == null ? '--' : Number(n).toLocaleString('en-US', { maximumFractionDigits: 0 })

export default function OrderBlotter({ orders, analytics, loading, onCancel, onExecute, onAdjust, onHedge }) {
  const [sortCol, setSortCol] = useState('created_at')
  const [sortDir, setSortDir] = useState('desc')
  const [statusFilter, setStatusFilter] = useState('ALL')
  const [search, setSearch] = useState('')
  const [selectedId, setSelectedId] = useState(null)

  // Build analytics map for fill rate lookup
  const analyticsMap = useMemo(() => {
    const m = {}
    for (const a of (analytics || [])) m[a.order_id] = a
    return m
  }, [analytics])

  const handleSort = (col) => {
    if (sortCol === col) setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    else { setSortCol(col); setSortDir('asc') }
  }

  const filtered = useMemo(() => {
    let rows = orders || []
    if (statusFilter !== 'ALL') rows = rows.filter(o => o.status === statusFilter)
    if (search) {
      const s = search.toLowerCase()
      rows = rows.filter(o =>
        o.order_id?.toLowerCase().includes(s) ||
        o.etf_ticker?.toLowerCase().includes(s) ||
        o.trader_id?.toLowerCase().includes(s) ||
        o.strategy?.toLowerCase().includes(s)
      )
    }
    rows = [...rows].sort((a, b) => {
      let va = a[sortCol], vb = b[sortCol]
      if (va == null) return 1
      if (vb == null) return -1
      if (typeof va === 'string') va = va.toLowerCase()
      if (typeof vb === 'string') vb = vb.toLowerCase()
      return sortDir === 'asc' ? (va > vb ? 1 : -1) : (va < vb ? 1 : -1)
    })
    return rows
  }, [orders, statusFilter, search, sortCol, sortDir])

  const statusCounts = useMemo(() => {
    const c = { ALL: orders.length, PENDING: 0, PARTIAL: 0, FILLED: 0, CANCELLED: 0 }
    for (const o of orders) if (c[o.status] !== undefined) c[o.status]++
    return c
  }, [orders])

  const SortTh = ({ col, label }) => (
    <th style={styles.th} onClick={() => handleSort(col)}>
      {label}
      {sortCol === col && <span style={styles.sortArrow}>{sortDir === 'asc' ? '↑' : '↓'}</span>}
    </th>
  )

  const canAction = (o) => !['FILLED', 'CANCELLED'].includes(o.status)

  if (loading) return <div style={styles.empty}>Loading orders...</div>

  return (
    <div>
      <div style={styles.filterRow}>
        {['ALL', 'PENDING', 'PARTIAL', 'FILLED', 'CANCELLED'].map(s => (
          <button key={s} style={styles.filterBtn(statusFilter === s)} onClick={() => setStatusFilter(s)}>
            {s} ({statusCounts[s] || 0})
          </button>
        ))}
        <input
          style={styles.search}
          placeholder="Search orders..."
          value={search}
          onChange={e => setSearch(e.target.value)}
        />
      </div>
      <div style={styles.container}>
        <table style={styles.table}>
          <thead>
            <tr>
              <SortTh col="order_id" label="Order ID" />
              <SortTh col="etf_ticker" label="ETF" />
              <SortTh col="direction" label="Side" />
              <SortTh col="qty" label="Qty" />
              <th style={styles.th}>Filled</th>
              <SortTh col="price_limit" label="Limit" />
              <SortTh col="strategy" label="Strategy" />
              <SortTh col="order_type" label="Type" />
              <SortTh col="status" label="Status" />
              <th style={styles.th}>Slippage</th>
              <SortTh col="created_at" label="Created" />
              <th style={styles.th}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 && (
              <tr><td colSpan={12} style={styles.empty}>No orders found</td></tr>
            )}
            {filtered.map(o => {
              const anl = analyticsMap[o.order_id]
              const fillPct = anl
                ? parseFloat(anl.fill_rate) * 100
                : (o.filled_qty && o.qty ? (o.filled_qty / o.qty * 100) : 0)
              const slippage = anl ? parseFloat(anl.avg_slippage_bps) : null
              const statusColor = STATUS_COLORS[o.status]?.text || '#94a3b8'
              const isSelected = selectedId === o.order_id
              const actionable = canAction(o)

              return (
                <tr
                  key={o.order_id}
                  style={styles.row(isSelected)}
                  onClick={() => setSelectedId(isSelected ? null : o.order_id)}
                  onMouseEnter={e => e.currentTarget.style.background = '#1e2235'}
                  onMouseLeave={e => e.currentTarget.style.background = isSelected ? '#1e2235' : 'transparent'}
                >
                  <td style={{ ...styles.td, fontFamily: 'monospace', fontSize: 11, color: '#94a3b8' }}>
                    {o.order_id}
                  </td>
                  <td style={{ ...styles.td, fontWeight: 700, color: '#e2e8f0', fontSize: 13 }}>
                    {o.etf_ticker}
                  </td>
                  <td style={{ ...styles.td, fontWeight: 600, color: DIR_COLOR[o.direction] }}>
                    {o.direction}
                  </td>
                  <td style={{ ...styles.td, textAlign: 'right', fontFamily: 'monospace' }}>
                    {fmtN(o.qty)}
                  </td>
                  <td style={styles.td}>
                    <div style={{ fontFamily: 'monospace', fontSize: 11 }}>
                      {fmtN(o.filled_qty || 0)}
                    </div>
                    <div style={styles.fillBar}>
                      <div style={styles.fillBarInner(fillPct, statusColor)} />
                    </div>
                  </td>
                  <td style={{ ...styles.td, fontFamily: 'monospace' }}>
                    ${fmt(o.price_limit)}
                  </td>
                  <td style={{ ...styles.td, color: '#818cf8', fontSize: 11 }}>
                    {o.strategy}
                  </td>
                  <td style={{ ...styles.td, color: '#64748b', fontSize: 11 }}>
                    {o.order_type}
                  </td>
                  <td style={styles.td}>
                    <span style={styles.badge(o.status)}>{o.status}</span>
                  </td>
                  <td style={{
                    ...styles.td,
                    fontFamily: 'monospace',
                    color: slippage == null ? '#475569' : slippage > 3 ? '#ef4444' : slippage < -1 ? '#22c55e' : '#f59e0b'
                  }}>
                    {slippage == null ? '--' : `${slippage > 0 ? '+' : ''}${fmt(slippage, 1)}`}
                  </td>
                  <td style={{ ...styles.td, fontSize: 11, color: '#475569' }}>
                    {o.created_at ? new Date(o.created_at).toLocaleTimeString() : '--'}
                  </td>
                  <td style={styles.td} onClick={e => e.stopPropagation()}>
                    <button
                      style={styles.actionBtn('#ef4444', !actionable)}
                      disabled={!actionable}
                      onClick={() => actionable && onCancel(o)}
                      title="Cancel order"
                    >✕ Cancel</button>
                    <button
                      style={styles.actionBtn('#22c55e', !actionable)}
                      disabled={!actionable}
                      onClick={() => actionable && onExecute(o)}
                      title="Force execute at market"
                    >▶ Exec</button>
                    <button
                      style={styles.actionBtn('#60a5fa', !actionable)}
                      disabled={!actionable}
                      onClick={() => actionable && onAdjust(o, 'size')}
                      title="Adjust size"
                    >⊞ Size</button>
                    <button
                      style={styles.actionBtn('#f59e0b', !actionable)}
                      disabled={!actionable}
                      onClick={() => actionable && onAdjust(o, 'price')}
                      title="Adjust price"
                    >$ Price</button>
                    <button
                      style={styles.actionBtn('#818cf8', !actionable)}
                      disabled={!actionable}
                      onClick={() => actionable && onHedge(o)}
                      title="Add hedge"
                    >⟳ Hedge</button>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
