const styles = {
  grid: {
    display: 'flex',
    flexDirection: 'column',
    gap: 10,
  },
  card: {
    background: '#1a1d2e',
    border: '1px solid #2d3148',
    borderRadius: 8,
    padding: '12px 14px',
    position: 'relative',
    overflow: 'hidden',
  },
  label: {
    fontSize: 10,
    fontWeight: 600,
    color: '#64748b',
    textTransform: 'uppercase',
    letterSpacing: '0.8px',
    marginBottom: 4,
  },
  value: {
    fontSize: 24,
    fontWeight: 700,
    lineHeight: 1,
    marginBottom: 2,
  },
  sub: {
    fontSize: 11,
    color: '#475569',
  },
  accent: {
    position: 'absolute',
    top: 0,
    left: 0,
    bottom: 0,
    width: 3,
    borderRadius: '8px 0 0 8px',
  },
}

function KpiCard({ label, value, sub, color, prefix = '', suffix = '' }) {
  return (
    <div style={styles.card}>
      <div style={{ ...styles.accent, background: color }} />
      <div style={{ paddingLeft: 8 }}>
        <div style={styles.label}>{label}</div>
        <div style={{ ...styles.value, color }}>
          {prefix}{value}{suffix}
        </div>
        {sub && <div style={styles.sub}>{sub}</div>}
      </div>
    </div>
  )
}

export default function KpiCards({ kpis }) {
  const { activeCount = 0, fillRate = 0, avgSlippage = 0, participationRate = 0 } = kpis || {}

  const slippageColor = avgSlippage <= 2 ? '#22c55e' : avgSlippage <= 5 ? '#f59e0b' : '#ef4444'

  return (
    <div style={styles.grid}>
      <KpiCard
        label="Active Orders"
        value={activeCount}
        sub="PENDING + PARTIAL"
        color="#ff6b35"
      />
      <KpiCard
        label="Fill Rate"
        value={fillRate.toFixed(1)}
        suffix="%"
        sub="Orders fully filled"
        color="#22c55e"
      />
      <KpiCard
        label="Avg Slippage"
        value={Math.abs(avgSlippage).toFixed(1)}
        suffix=" bps"
        sub={avgSlippage >= 0 ? 'vs arrival' : 'saved vs arrival'}
        color={slippageColor}
      />
      <KpiCard
        label="Participation"
        value={participationRate.toFixed(1)}
        suffix="%"
        sub="Avg fill rate"
        color="#818cf8"
      />
    </div>
  )
}
