import type { TableData } from '../../types/workspace'

interface DataEditorProps {
  table: TableData
}

export function DataEditor({ table }: DataEditorProps) {
  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Toolbar */}
      <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--border-color)', display: 'flex', alignItems: 'center', gap: 16 }}>
        <span style={{ fontWeight: 600 }}>{table.name}</span>
        <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>
          {table.rows.length} rows × {table.columns.length} columns
        </span>
      </div>

      {/* Table */}
      <div style={{ flex: 1, overflow: 'auto', padding: 12 }}>
        <table className="data-table">
          <thead>
            <tr>
              <th style={{ width: 50 }}>#</th>
              {table.columns.map((col) => (
                <th key={col.name}>
                  <div>{col.name}</div>
                  <div style={{ fontSize: 10, fontWeight: 'normal', color: 'var(--text-muted)' }}>
                    {col.type}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {table.rows.slice(0, 100).map((row, i) => (
              <tr key={i}>
                <td style={{ color: 'var(--text-muted)' }}>{i + 1}</td>
                {table.columns.map((col) => (
                  <td key={col.name}>
                    {formatCellValue(row[col.name])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {table.rows.length > 100 && (
          <div style={{ padding: 12, textAlign: 'center', color: 'var(--text-muted)', fontSize: 12 }}>
            Showing first 100 of {table.rows.length} rows
          </div>
        )}
      </div>
    </div>
  )
}

function formatCellValue(value: unknown): React.ReactNode {
  if (value === null || value === undefined) {
    return <span style={{ color: 'var(--text-muted)' }}>null</span>
  }
  if (typeof value === 'boolean') {
    return <span style={{ color: 'var(--info)' }}>{value ? 'true' : 'false'}</span>
  }
  if (typeof value === 'number') {
    return <span style={{ color: 'var(--success)' }}>{value.toLocaleString()}</span>
  }
  if (typeof value === 'object') {
    return <span style={{ color: 'var(--text-muted)' }}>{JSON.stringify(value)}</span>
  }
  return String(value)
}
