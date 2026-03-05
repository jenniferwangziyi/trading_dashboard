import { useEffect, useRef } from 'react'

const styles = {
  bar: {
    background: '#12141f',
    borderBottom: '1px solid #2d3148',
    padding: '6px 20px',
    overflow: 'hidden',
    whiteSpace: 'nowrap',
  },
  scroll: {
    display: 'inline-flex',
    gap: 32,
    animation: 'ticker-scroll 40s linear infinite',
  },
  item: {
    display: 'inline-flex',
    alignItems: 'center',
    gap: 6,
    cursor: 'pointer',
    padding: '2px 4px',
    borderRadius: 4,
    transition: 'background 0.15s',
  },
  ticker: {
    fontSize: 12,
    fontWeight: 700,
    color: '#94a3b8',
    letterSpacing: '0.5px',
  },
  price: {
    fontSize: 13,
    fontWeight: 600,
    color: '#e2e8f0',
  },
  change: (pct) => ({
    fontSize: 11,
    fontWeight: 500,
    color: pct >= 0 ? '#22c55e' : '#ef4444',
  }),
  arrow: (pct) => ({
    fontSize: 10,
    color: pct >= 0 ? '#22c55e' : '#ef4444',
  }),
  selected: {
    background: '#ff6b3522',
    outline: '1px solid #ff6b3555',
  },
}

const fmt = (n, dec = 2) => n == null ? '--' : Number(n).toFixed(dec)

export default function MarketTicker({ market, onSelectTicker, selectedTicker }) {
  const scrollRef = useRef(null)

  useEffect(() => {
    const style = document.createElement('style')
    style.textContent = `
      @keyframes ticker-scroll {
        0% { transform: translateX(0); }
        100% { transform: translateX(-50%); }
      }
      .ticker-item:hover { background: #1e2235 !important; }
    `
    document.head.appendChild(style)
    return () => document.head.removeChild(style)
  }, [])

  if (!market.length) {
    return (
      <div style={styles.bar}>
        <span style={{ color: '#475569', fontSize: 12 }}>Loading market data...</span>
      </div>
    )
  }

  // Duplicate for seamless loop
  const items = [...market, ...market]

  return (
    <div style={styles.bar}>
      <div ref={scrollRef} style={styles.scroll}>
        {items.map((m, i) => {
          const pct = parseFloat(m.price_change_pct) || 0
          const isSelected = m.ticker === selectedTicker
          return (
            <div
              key={`${m.ticker}-${i}`}
              className="ticker-item"
              style={{ ...styles.item, ...(isSelected ? styles.selected : {}) }}
              onClick={() => onSelectTicker && onSelectTicker(m.ticker)}
            >
              <span style={styles.ticker}>{m.ticker}</span>
              <span style={styles.price}>${fmt(m.last_price)}</span>
              <span style={styles.arrow(pct)}>{pct >= 0 ? '▲' : '▼'}</span>
              <span style={styles.change(pct)}>{pct >= 0 ? '+' : ''}{fmt(pct, 2)}%</span>
              <span style={{ fontSize: 10, color: '#334155', margin: '0 2px' }}>|</span>
              <span style={{ fontSize: 10, color: '#475569' }}>
                B {fmt(m.bid)} A {fmt(m.ask)}
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}
