import { useState } from 'react'

const overlay = {
  position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
  background: 'rgba(0,0,0,0.65)', display: 'flex', alignItems: 'center', justifyContent: 'center',
  zIndex: 1000,
}
const modal = {
  background: '#1a1d2e', border: '1px solid #2d3148', borderRadius: 10,
  padding: '24px 28px', width: 360, boxShadow: '0 20px 60px rgba(0,0,0,0.5)',
}
const title = { fontSize: 15, fontWeight: 700, color: '#e2e8f0', marginBottom: 16 }
const label = { fontSize: 11, color: '#64748b', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.7px', marginBottom: 4, display: 'block' }
const input = {
  width: '100%', background: '#12141f', border: '1px solid #2d3148',
  color: '#e2e8f0', borderRadius: 6, padding: '8px 12px', fontSize: 14,
  outline: 'none', marginBottom: 16,
}
const row = { display: 'flex', gap: 10, marginTop: 8 }
const btnPrimary = {
  flex: 1, background: '#ff6b35', border: 'none', color: '#fff',
  borderRadius: 6, padding: '9px 0', cursor: 'pointer', fontWeight: 600, fontSize: 13,
}
const btnSecondary = {
  flex: 1, background: 'none', border: '1px solid #2d3148', color: '#64748b',
  borderRadius: 6, padding: '9px 0', cursor: 'pointer', fontWeight: 500, fontSize: 13,
}
const info = {
  background: '#12141f', border: '1px solid #1e2235', borderRadius: 6,
  padding: '10px 12px', marginBottom: 16, fontSize: 12,
}
const infoRow = { display: 'flex', justifyContent: 'space-between', marginBottom: 4 }
const infoLabel = { color: '#64748b' }
const infoVal = { color: '#e2e8f0', fontFamily: 'monospace' }

export default function AdjustModal({ order, type, onSubmit, onClose }) {
  const isSize = type === 'size'
  const [value, setValue] = useState(
    isSize ? String(order.qty || '') : String(order.price_limit || '')
  )
  const [error, setError] = useState('')

  const handleSubmit = (e) => {
    e.preventDefault()
    const num = parseFloat(value)
    if (isNaN(num) || num <= 0) {
      setError(`${isSize ? 'Quantity' : 'Price'} must be a positive number`)
      return
    }
    if (isSize && !Number.isInteger(num)) {
      setError('Quantity must be a whole number')
      return
    }
    setError('')
    onSubmit(isSize ? { qty: Math.round(num) } : { price_limit: num })
  }

  const newNotional = parseFloat(value) > 0
    ? (isSize
        ? parseFloat(value) * (order.price_limit || 0)
        : (order.qty || 0) * parseFloat(value))
    : null

  return (
    <div style={overlay} onClick={onClose}>
      <div style={modal} onClick={e => e.stopPropagation()}>
        <div style={title}>
          {isSize ? 'Adjust Order Size' : 'Adjust Price Limit'}
          <span style={{ fontSize: 12, color: '#64748b', marginLeft: 8 }}>{order.order_id}</span>
        </div>

        <div style={info}>
          <div style={infoRow}>
            <span style={infoLabel}>ETF</span>
            <span style={{ ...infoVal, fontWeight: 700 }}>{order.etf_ticker}</span>
          </div>
          <div style={infoRow}>
            <span style={infoLabel}>Direction</span>
            <span style={{ ...infoVal, color: order.direction === 'BUY' ? '#22c55e' : '#ef4444' }}>{order.direction}</span>
          </div>
          <div style={infoRow}>
            <span style={infoLabel}>Current {isSize ? 'Qty' : 'Limit'}</span>
            <span style={infoVal}>{isSize ? order.qty?.toLocaleString() : `$${order.price_limit?.toFixed(2)}`}</span>
          </div>
          <div style={infoRow}>
            <span style={infoLabel}>Strategy</span>
            <span style={{ ...infoVal, color: '#818cf8' }}>{order.strategy}</span>
          </div>
        </div>

        <form onSubmit={handleSubmit}>
          <label style={label}>
            New {isSize ? 'Quantity (shares)' : 'Price Limit ($)'}
          </label>
          <input
            style={{ ...input, borderColor: error ? '#ef4444' : '#2d3148' }}
            type="number"
            min="1"
            step={isSize ? '100' : '0.01'}
            value={value}
            onChange={e => { setValue(e.target.value); setError('') }}
            autoFocus
          />
          {error && <div style={{ color: '#ef4444', fontSize: 11, marginBottom: 10, marginTop: -12 }}>{error}</div>}

          {newNotional && (
            <div style={{ ...info, marginBottom: 16, fontSize: 12 }}>
              <div style={infoRow}>
                <span style={infoLabel}>New Notional Value</span>
                <span style={{ ...infoVal, color: '#22c55e' }}>
                  ${newNotional.toLocaleString('en-US', { maximumFractionDigits: 0 })}
                </span>
              </div>
              {isSize && (
                <div style={infoRow}>
                  <span style={infoLabel}>Change</span>
                  <span style={{ ...infoVal, color: parseFloat(value) >= order.qty ? '#22c55e' : '#ef4444' }}>
                    {parseFloat(value) >= order.qty ? '+' : ''}
                    {((parseFloat(value) - (order.qty || 0)) / (order.qty || 1) * 100).toFixed(1)}%
                  </span>
                </div>
              )}
            </div>
          )}

          <div style={row}>
            <button type="button" style={btnSecondary} onClick={onClose}>Cancel</button>
            <button type="submit" style={btnPrimary}>
              Confirm {isSize ? 'Size' : 'Price'} Adjustment
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
