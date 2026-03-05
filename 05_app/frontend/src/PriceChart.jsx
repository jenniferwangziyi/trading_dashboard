import {
  ResponsiveContainer, ComposedChart, Line, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts'
import { useMemo } from 'react'

const styles = {
  container: { padding: '12px 16px', height: 280 },
  empty: { display: 'flex', alignItems: 'center', justifyContent: 'center', height: 240, color: '#475569', fontSize: 13 },
  tooltipBox: {
    background: '#1a1d2e',
    border: '1px solid #2d3148',
    borderRadius: 6,
    padding: '8px 12px',
    fontSize: 12,
  },
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div style={styles.tooltipBox}>
      <div style={{ color: '#64748b', marginBottom: 4, fontSize: 11 }}>{label}</div>
      {payload.map(p => (
        <div key={p.dataKey} style={{ color: p.color, marginBottom: 2 }}>
          {p.name}: {p.dataKey === 'volume' ? Number(p.value).toLocaleString() : `$${Number(p.value).toFixed(2)}`}
        </div>
      ))}
    </div>
  )
}

export default function PriceChart({ history, ticker }) {
  const data = useMemo(() => {
    if (!history?.length) return []
    return history.map(row => ({
      time: row.event_time
        ? new Date(row.event_time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
        : '',
      price: parseFloat(row.last_price),
      vwap: parseFloat(row.vwap),
      volume: parseInt(row.volume) || 0,
    }))
  }, [history])

  if (!data.length) {
    return <div style={styles.empty}>No price data for {ticker}</div>
  }

  const prices = data.map(d => d.price).filter(Boolean)
  const minPrice = Math.min(...prices) * 0.9995
  const maxPrice = Math.max(...prices) * 1.0005

  return (
    <div style={styles.container}>
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={data} margin={{ top: 4, right: 12, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e2235" vertical={false} />
          <XAxis
            dataKey="time"
            tick={{ fill: '#475569', fontSize: 10 }}
            tickLine={false}
            axisLine={{ stroke: '#1e2235' }}
            interval="preserveStartEnd"
          />
          <YAxis
            yAxisId="price"
            domain={[minPrice, maxPrice]}
            tick={{ fill: '#475569', fontSize: 10 }}
            tickLine={false}
            axisLine={false}
            tickFormatter={v => `$${v.toFixed(0)}`}
            width={52}
          />
          <YAxis
            yAxisId="vol"
            orientation="right"
            tick={{ fill: '#334155', fontSize: 9 }}
            tickLine={false}
            axisLine={false}
            tickFormatter={v => `${(v / 1000).toFixed(0)}k`}
            width={36}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend
            wrapperStyle={{ fontSize: 11, color: '#64748b' }}
            iconType="line"
          />
          <Bar
            yAxisId="vol"
            dataKey="volume"
            name="Volume"
            fill="#1e3a5f"
            opacity={0.6}
            barSize={3}
          />
          <Line
            yAxisId="price"
            type="monotone"
            dataKey="vwap"
            name="VWAP"
            stroke="#f59e0b"
            strokeWidth={1}
            dot={false}
            strokeDasharray="4 2"
          />
          <Line
            yAxisId="price"
            type="monotone"
            dataKey="price"
            name="Price"
            stroke="#ff6b35"
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 4, fill: '#ff6b35' }}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}
