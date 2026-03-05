import { useState, useMemo } from 'react'

const overlay = {
  position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
  background: 'rgba(0,0,0,0.65)', display: 'flex', alignItems: 'center', justifyContent: 'center',
  zIndex: 1000,
}
const modal = {
  background: '#1a1d2e', border: '1px solid #2d3148', borderRadius: 10,
  padding: '24px 28px', width: 420, boxShadow: '0 20px 60px rgba(0,0,0,0.5)',
  maxHeight: '90vh', overflowY: 'auto',
}
const title = { fontSize: 15, fontWeight: 700, color: '#e2e8f0', marginBottom: 16 }
const lbl = { fontSize: 11, color: '#64748b', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.7px', marginBottom: 4, display: 'block' }
const inp = {
  width: '100%', background: '#12141f', border: '1px solid #2d3148',
  color: '#e2e8f0', borderRadius: 6, padding: '8px 12px', fontSize: 13,
  outline: 'none', marginBottom: 14, cursor: 'pointer',
}
const inpText = { ...inp, cursor: 'text' }
const row = { display: 'flex', gap: 10, marginTop: 8 }
const btnPrimary = {
  flex: 1, background: '#818cf8', border: 'none', color: '#fff',
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
const typeBtn = (active) => ({
  flex: 1, background: active ? '#818cf822' : 'none',
  border: `1px solid ${active ? '#818cf8' : '#2d3148'}`,
  color: active ? '#818cf8' : '#64748b',
  borderRadius: 5, padding: '7px 0', cursor: 'pointer', fontWeight: active ? 600 : 400, fontSize: 12,
})
const instrCard = (selected) => ({
  background: selected ? '#818cf811' : '#12141f',
  border: `1px solid ${selected ? '#818cf8' : '#1e2235'}`,
  borderRadius: 6, padding: '8px 12px', cursor: 'pointer', marginBottom: 6,
  transition: 'all 0.15s',
})

const HEDGE_TYPES = ['FUTURES', 'OPTIONS']

export default function HedgeModal({ order, instruments, onSubmit, onClose }) {
  const [hedgeType, setHedgeType] = useState('FUTURES')
  const [selectedInstrument, setSelectedInstrument] = useState(null)
  const [direction, setDirection] = useState(order.direction === 'BUY' ? 'SELL' : 'BUY')
  const [qty, setQty] = useState('')
  const [error, setError] = useState('')

  const filtered = useMemo(() =>
    (instruments || []).filter(i => i.type === hedgeType && i.underlying === order.etf_ticker),
    [instruments, hedgeType, order.etf_ticker]
  )

  // Suggest hedge quantity based on delta
  const suggestQty = (instr) => {
    if (!instr) return
    const orderNotional = (order.qty || 0) * (order.price_limit || 0)
    let hedgeQty
    if (instr.type === 'FUTURES') {
      hedgeQty = Math.ceil(orderNotional / ((order.price_limit || 500) * (instr.contract_size || 50)))
    } else {
      // Options: delta-adjusted
      const deltaAdj = Math.abs(parseFloat(instr.delta) || 0.3)
      hedgeQty = Math.ceil(order.qty / (instr.contract_size || 100) / deltaAdj)
    }
    setQty(String(Math.max(1, hedgeQty)))
    setSelectedInstrument(instr)
  }

  const handleSubmit = (e) => {
    e.preventDefault()
    if (!selectedInstrument) { setError('Select a hedge instrument'); return }
    const q = parseInt(qty)
    if (isNaN(q) || q <= 0) { setError('Quantity must be positive'); return }
    setError('')
    onSubmit({
      instrument_id: selectedInstrument.instrument_id,
      direction,
      qty: q,
      hedge_type: hedgeType,
    })
  }

  return (
    <div style={overlay} onClick={onClose}>
      <div style={modal} onClick={e => e.stopPropagation()}>
        <div style={title}>
          Add Hedge
          <span style={{ fontSize: 12, color: '#64748b', marginLeft: 8 }}>{order.order_id}</span>
        </div>

        <div style={info}>
          <div style={infoRow}>
            <span style={infoLabel}>Order</span>
            <span style={{ ...infoVal, fontWeight: 700 }}>
              {order.direction} {order.qty?.toLocaleString()} {order.etf_ticker}
            </span>
          </div>
          <div style={infoRow}>
            <span style={infoLabel}>Notional</span>
            <span style={infoVal}>
              ${((order.qty || 0) * (order.price_limit || 0)).toLocaleString('en-US', { maximumFractionDigits: 0 })}
            </span>
          </div>
          <div style={infoRow}>
            <span style={infoLabel}>Strategy</span>
            <span style={{ ...infoVal, color: '#818cf8' }}>{order.strategy}</span>
          </div>
        </div>

        {/* Hedge Type Toggle */}
        <label style={lbl}>Hedge Instrument Type</label>
        <div style={{ display: 'flex', gap: 8, marginBottom: 14 }}>
          {HEDGE_TYPES.map(t => (
            <button key={t} style={typeBtn(hedgeType === t)} onClick={() => { setHedgeType(t); setSelectedInstrument(null); setQty('') }}>
              {t === 'FUTURES' ? '⟳ Futures' : '◎ Options'}
            </button>
          ))}
        </div>

        {/* Instrument Picker */}
        <label style={lbl}>Select Instrument</label>
        {filtered.length === 0 ? (
          <div style={{ color: '#475569', fontSize: 12, marginBottom: 14 }}>
            No {hedgeType.toLowerCase()} available for {order.etf_ticker}
          </div>
        ) : (
          <div style={{ marginBottom: 14 }}>
            {filtered.map(instr => (
              <div
                key={instr.instrument_id}
                style={instrCard(selectedInstrument?.instrument_id === instr.instrument_id)}
                onClick={() => suggestQty(instr)}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <span style={{ fontWeight: 600, color: '#e2e8f0', fontSize: 13 }}>{instr.instrument_id}</span>
                  <span style={{ fontSize: 11, color: '#64748b' }}>exp {instr.expiry}</span>
                </div>
                <div style={{ display: 'flex', gap: 16, marginTop: 4, fontSize: 11, color: '#64748b' }}>
                  {instr.strike && <span>Strike: ${instr.strike}</span>}
                  <span>Size: {instr.contract_size}</span>
                  <span>Δ: {instr.delta}</span>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Direction */}
        <label style={lbl}>Hedge Direction</label>
        <div style={{ display: 'flex', gap: 8, marginBottom: 14 }}>
          {['BUY', 'SELL'].map(d => (
            <button
              key={d}
              style={{
                ...typeBtn(direction === d),
                borderColor: direction === d ? (d === 'BUY' ? '#22c55e' : '#ef4444') : '#2d3148',
                color: direction === d ? (d === 'BUY' ? '#22c55e' : '#ef4444') : '#64748b',
                background: direction === d ? (d === 'BUY' ? '#22c55e11' : '#ef444411') : 'none',
              }}
              onClick={() => setDirection(d)}
            >{d === 'BUY' ? '▲ BUY' : '▼ SELL'}</button>
          ))}
        </div>

        {/* Quantity */}
        <form onSubmit={handleSubmit}>
          <label style={lbl}>Hedge Quantity {hedgeType === 'FUTURES' ? '(contracts)' : '(option contracts)'}</label>
          <input
            style={{ ...inpText, borderColor: error ? '#ef4444' : '#2d3148' }}
            type="number"
            min="1"
            value={qty}
            onChange={e => { setQty(e.target.value); setError('') }}
            placeholder="Enter quantity or select instrument for suggestion"
          />
          {error && <div style={{ color: '#ef4444', fontSize: 11, marginBottom: 8, marginTop: -10 }}>{error}</div>}

          {selectedInstrument && qty && (
            <div style={{ ...info, fontSize: 12, marginBottom: 14 }}>
              <div style={infoRow}>
                <span style={infoLabel}>Hedge Ratio</span>
                <span style={infoVal}>
                  {Math.round(parseInt(qty) * (selectedInstrument.contract_size || 1) / (order.qty || 1) * 100)}%
                </span>
              </div>
              <div style={infoRow}>
                <span style={infoLabel}>Delta Exposure Offset</span>
                <span style={{ ...infoVal, color: '#818cf8' }}>
                  {Math.abs(parseFloat(selectedInstrument.delta) * parseInt(qty) * (selectedInstrument.contract_size || 1)).toLocaleString('en-US', { maximumFractionDigits: 0 })} shares equiv.
                </span>
              </div>
            </div>
          )}

          <div style={row}>
            <button type="button" style={btnSecondary} onClick={onClose}>Cancel</button>
            <button type="submit" style={btnPrimary}>Submit Hedge</button>
          </div>
        </form>
      </div>
    </div>
  )
}
