import {
  ResponsiveContainer, BarChart, Bar, ScatterChart, Scatter,
  XAxis, YAxis, CartesianGrid, Tooltip, Cell, Legend,
} from 'recharts'
import { useMemo } from 'react'

const STRATEGY_COLORS = {
  TWAP: '#818cf8',
  VWAP: '#22c55e',
  IS: '#ff6b35',
  POV: '#f59e0b',
  MKT_ON_CLOSE: '#60a5fa',
}

const styles = {
  container: { padding: '8px 12px', display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, height: 280 },
  section: { display: 'flex', flexDirection: 'column' },
  sectionTitle: { fontSize: 10, fontWeight: 600, color: '#64748b', textTransform: 'uppercase', letterSpacing: '0.7px', marginBottom: 6 },
  tooltipBox: {
    background: '#1a1d2e',
    border: '1px solid #2d3148',
    borderRadius: 6,
    padding: '6px 10px',
    fontSize: 11,
  },
  empty: { display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: '#334155', fontSize: 12 },
}

function TooltipBox({ active, payload }) {
  if (!active || !payload?.length) return null
  return (
    <div style={styles.tooltipBox}>
      {payload.map(p => (
        <div key={p.name} style={{ color: p.color || '#e2e8f0', marginBottom: 1 }}>
          {p.name}: {typeof p.value === 'number' ? p.value.toFixed(2) : p.value}
        </div>
      ))}
    </div>
  )
}

export default function AnalyticsCharts({ analytics, performance }) {
  // Fill rate by strategy (bar chart)
  const byStrategy = useMemo(() => {
    if (!analytics?.length) return []
    const agg = {}
    for (const a of analytics) {
      const s = a.strategy || 'UNKNOWN'
      if (!agg[s]) agg[s] = { strategy: s, total: 0, fillRateSum: 0, slippageSum: 0 }
      agg[s].total++
      agg[s].fillRateSum += parseFloat(a.fill_rate) || 0
      agg[s].slippageSum += parseFloat(a.avg_slippage_bps) || 0
    }
    return Object.values(agg).map(d => ({
      strategy: d.strategy,
      fill_rate: Math.round(d.fillRateSum / d.total * 1000) / 10,
      avg_slippage: Math.round(d.slippageSum / d.total * 10) / 10,
    })).sort((a, b) => b.fill_rate - a.fill_rate)
  }, [analytics])

  // Scatter: participation_rate vs slippage per order
  const scatterData = useMemo(() => {
    if (!analytics?.length) return []
    return analytics
      .filter(a => a.fill_rate != null)
      .slice(0, 80)
      .map(a => ({
        x: Math.round(parseFloat(a.fill_rate) * 1000) / 10,
        y: Math.round(parseFloat(a.avg_slippage_bps) * 10) / 10,
        strategy: a.strategy || 'UNKNOWN',
      }))
  }, [analytics])

  return (
    <div style={styles.container}>
      {/* Fill Rate by Strategy */}
      <div style={styles.section}>
        <div style={styles.sectionTitle}>Fill Rate by Strategy</div>
        {byStrategy.length === 0
          ? <div style={styles.empty}>Awaiting DLT data</div>
          : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={byStrategy} layout="vertical" margin={{ top: 0, right: 8, left: 8, bottom: 0 }}>
                <CartesianGrid strokeDasharray="2 2" stroke="#1e2235" horizontal={false} />
                <XAxis type="number" domain={[0, 100]} tick={{ fill: '#475569', fontSize: 10 }} tickLine={false} axisLine={false} tickFormatter={v => `${v}%`} />
                <YAxis type="category" dataKey="strategy" tick={{ fill: '#94a3b8', fontSize: 10 }} tickLine={false} axisLine={false} width={80} />
                <Tooltip content={<TooltipBox />} />
                <Bar dataKey="fill_rate" name="Fill Rate %" radius={[0, 3, 3, 0]}>
                  {byStrategy.map(d => (
                    <Cell key={d.strategy} fill={STRATEGY_COLORS[d.strategy] || '#64748b'} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )
        }
      </div>

      {/* Participation Rate vs Slippage Scatter */}
      <div style={styles.section}>
        <div style={styles.sectionTitle}>Participation Rate vs Slippage</div>
        {scatterData.length === 0
          ? <div style={styles.empty}>Awaiting DLT data</div>
          : (
            <ResponsiveContainer width="100%" height={220}>
              <ScatterChart margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="2 2" stroke="#1e2235" />
                <XAxis
                  type="number"
                  dataKey="x"
                  name="Fill Rate"
                  domain={[0, 100]}
                  tick={{ fill: '#475569', fontSize: 10 }}
                  tickLine={false}
                  axisLine={false}
                  label={{ value: 'Fill Rate %', position: 'insideBottom', fill: '#334155', fontSize: 9 }}
                />
                <YAxis
                  type="number"
                  dataKey="y"
                  name="Slippage bps"
                  tick={{ fill: '#475569', fontSize: 10 }}
                  tickLine={false}
                  axisLine={false}
                  label={{ value: 'Slippage bps', angle: -90, position: 'insideLeft', fill: '#334155', fontSize: 9 }}
                />
                <Tooltip
                  content={({ active, payload }) => {
                    if (!active || !payload?.length) return null
                    const d = payload[0]?.payload
                    return (
                      <div style={styles.tooltipBox}>
                        <div style={{ color: '#94a3b8', marginBottom: 2 }}>{d?.strategy}</div>
                        <div style={{ color: '#22c55e' }}>Fill: {d?.x?.toFixed(1)}%</div>
                        <div style={{ color: '#f59e0b' }}>Slippage: {d?.y?.toFixed(2)} bps</div>
                      </div>
                    )
                  }}
                />
                <Scatter data={scatterData} fill="#818cf8" opacity={0.7}>
                  {scatterData.map((d, i) => (
                    <Cell key={i} fill={STRATEGY_COLORS[d.strategy] || '#64748b'} />
                  ))}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          )
        }
      </div>
    </div>
  )
}
