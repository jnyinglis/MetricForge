import { useState } from 'react'
import { useWorkspaceStore } from '../hooks/useWorkspaceStore'
import type { EditorTab } from '../types/workspace'

export function Sidebar() {
  const tables = useWorkspaceStore((state) => state.tables)
  const schema = useWorkspaceStore((state) => state.schema)
  const metrics = useWorkspaceStore((state) => state.metrics)
  const queries = useWorkspaceStore((state) => state.queries)
  const activeTab = useWorkspaceStore((state) => state.activeTab)
  const sidebarExpanded = useWorkspaceStore((state) => state.sidebarExpanded)
  const setActiveTab = useWorkspaceStore((state) => state.setActiveTab)
  const toggleSidebarSection = useWorkspaceStore((state) => state.toggleSidebarSection)
  const addMetric = useWorkspaceStore((state) => state.addMetric)
  const addQuery = useWorkspaceStore((state) => state.addQuery)
  const removeMetric = useWorkspaceStore((state) => state.removeMetric)
  const removeQuery = useWorkspaceStore((state) => state.removeQuery)
  const removeTable = useWorkspaceStore((state) => state.removeTable)

  const [showNewMetricInput, setShowNewMetricInput] = useState(false)
  const [showNewQueryInput, setShowNewQueryInput] = useState(false)
  const [newMetricName, setNewMetricName] = useState('')
  const [newQueryName, setNewQueryName] = useState('')

  const isTabActive = (tab: EditorTab) => {
    if (!activeTab) return false
    if (tab.type !== activeTab.type) return false
    if (tab.type === 'data' && activeTab.type === 'data') {
      return tab.tableName === activeTab.tableName
    }
    if (tab.type === 'metric' && activeTab.type === 'metric') {
      return tab.metricName === activeTab.metricName
    }
    if (tab.type === 'query' && activeTab.type === 'query') {
      return tab.queryName === activeTab.queryName
    }
    return tab.type === activeTab.type
  }

  const handleAddMetric = () => {
    if (newMetricName.trim()) {
      addMetric(newMetricName.trim())
      setActiveTab({ type: 'metric', metricName: newMetricName.trim() })
      setNewMetricName('')
      setShowNewMetricInput(false)
    }
  }

  const handleAddQuery = () => {
    if (newQueryName.trim()) {
      addQuery(newQueryName.trim())
      setActiveTab({ type: 'query', queryName: newQueryName.trim() })
      setNewQueryName('')
      setShowNewQueryInput(false)
    }
  }

  return (
    <aside className="sidebar">
      {/* Data Section */}
      <div className="sidebar-section">
        <div
          className="sidebar-section-header"
          onClick={() => toggleSidebarSection('data')}
        >
          <span>{sidebarExpanded.data ? '▼' : '▶'}</span>
          <span>DATA ({tables.length})</span>
        </div>
        {sidebarExpanded.data && (
          <div className="sidebar-section-content">
            {tables.map((table) => (
              <div
                key={table.name}
                className={`sidebar-item ${isTabActive({ type: 'data', tableName: table.name }) ? 'active' : ''}`}
                onClick={() => setActiveTab({ type: 'data', tableName: table.name })}
              >
                <span className="tab-icon">📊</span>
                <span style={{ flex: 1 }}>{table.name}</span>
                <span
                  className="sidebar-item-delete"
                  onClick={(e) => {
                    e.stopPropagation()
                    removeTable(table.name)
                  }}
                  style={{ opacity: 0.5, cursor: 'pointer' }}
                >
                  ×
                </span>
              </div>
            ))}
            {tables.length === 0 && (
              <div className="sidebar-item" style={{ color: 'var(--text-muted)' }}>
                No tables loaded
              </div>
            )}
          </div>
        )}
      </div>

      {/* Schema Section */}
      <div className="sidebar-section">
        <div
          className="sidebar-section-header"
          onClick={() => toggleSidebarSection('schema')}
        >
          <span>{sidebarExpanded.schema ? '▼' : '▶'}</span>
          <span>SCHEMA</span>
        </div>
        {sidebarExpanded.schema && (
          <div className="sidebar-section-content">
            <div
              className={`sidebar-item ${isTabActive({ type: 'schema' }) ? 'active' : ''}`}
              onClick={() => setActiveTab({ type: 'schema' })}
            >
              <span className="tab-icon">🔗</span>
              <span>Schema Editor</span>
            </div>
            <div className="sidebar-item" style={{ paddingLeft: 36, fontSize: 11, color: 'var(--text-muted)' }}>
              Facts: {schema.facts.length} | Dims: {schema.dimensions.length}
            </div>
            <div className="sidebar-item" style={{ paddingLeft: 36, fontSize: 11, color: 'var(--text-muted)' }}>
              Attrs: {schema.attributes.length} | Joins: {schema.joins.length}
            </div>
          </div>
        )}
      </div>

      {/* Metrics Section */}
      <div className="sidebar-section">
        <div
          className="sidebar-section-header"
          onClick={() => toggleSidebarSection('metrics')}
        >
          <span>{sidebarExpanded.metrics ? '▼' : '▶'}</span>
          <span>METRICS ({metrics.length})</span>
          <button
            className="btn btn-sm"
            style={{ marginLeft: 'auto', padding: '2px 6px', fontSize: 11 }}
            onClick={(e) => {
              e.stopPropagation()
              setShowNewMetricInput(true)
            }}
          >
            +
          </button>
        </div>
        {sidebarExpanded.metrics && (
          <div className="sidebar-section-content">
            {showNewMetricInput && (
              <div className="sidebar-item" style={{ gap: 4 }}>
                <input
                  type="text"
                  className="form-input"
                  style={{ flex: 1, padding: '2px 6px', fontSize: 12 }}
                  placeholder="Metric name"
                  value={newMetricName}
                  onChange={(e) => setNewMetricName(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') handleAddMetric()
                    if (e.key === 'Escape') {
                      setShowNewMetricInput(false)
                      setNewMetricName('')
                    }
                  }}
                  autoFocus
                />
                <button className="btn btn-sm btn-primary" onClick={handleAddMetric}>
                  Add
                </button>
              </div>
            )}
            {metrics.map((metric) => (
              <div
                key={metric.name}
                className={`sidebar-item ${isTabActive({ type: 'metric', metricName: metric.name }) ? 'active' : ''}`}
                onClick={() => setActiveTab({ type: 'metric', metricName: metric.name })}
              >
                <span className="tab-icon">{metric.valid ? '📐' : '⚠️'}</span>
                <span style={{ flex: 1 }}>{metric.name}</span>
                <span
                  onClick={(e) => {
                    e.stopPropagation()
                    removeMetric(metric.name)
                  }}
                  style={{ opacity: 0.5, cursor: 'pointer' }}
                >
                  ×
                </span>
              </div>
            ))}
            {metrics.length === 0 && !showNewMetricInput && (
              <div className="sidebar-item" style={{ color: 'var(--text-muted)' }}>
                No metrics defined
              </div>
            )}
          </div>
        )}
      </div>

      {/* Queries Section */}
      <div className="sidebar-section">
        <div
          className="sidebar-section-header"
          onClick={() => toggleSidebarSection('queries')}
        >
          <span>{sidebarExpanded.queries ? '▼' : '▶'}</span>
          <span>QUERIES ({queries.length})</span>
          <button
            className="btn btn-sm"
            style={{ marginLeft: 'auto', padding: '2px 6px', fontSize: 11 }}
            onClick={(e) => {
              e.stopPropagation()
              setShowNewQueryInput(true)
            }}
          >
            +
          </button>
        </div>
        {sidebarExpanded.queries && (
          <div className="sidebar-section-content">
            {showNewQueryInput && (
              <div className="sidebar-item" style={{ gap: 4 }}>
                <input
                  type="text"
                  className="form-input"
                  style={{ flex: 1, padding: '2px 6px', fontSize: 12 }}
                  placeholder="Query name"
                  value={newQueryName}
                  onChange={(e) => setNewQueryName(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') handleAddQuery()
                    if (e.key === 'Escape') {
                      setShowNewQueryInput(false)
                      setNewQueryName('')
                    }
                  }}
                  autoFocus
                />
                <button className="btn btn-sm btn-primary" onClick={handleAddQuery}>
                  Add
                </button>
              </div>
            )}
            {queries.map((query) => (
              <div
                key={query.name}
                className={`sidebar-item ${isTabActive({ type: 'query', queryName: query.name }) ? 'active' : ''}`}
                onClick={() => setActiveTab({ type: 'query', queryName: query.name })}
              >
                <span className="tab-icon">{query.valid ? '🔍' : '⚠️'}</span>
                <span style={{ flex: 1 }}>{query.name}</span>
                <span
                  onClick={(e) => {
                    e.stopPropagation()
                    removeQuery(query.name)
                  }}
                  style={{ opacity: 0.5, cursor: 'pointer' }}
                >
                  ×
                </span>
              </div>
            ))}
            {queries.length === 0 && !showNewQueryInput && (
              <div className="sidebar-item" style={{ color: 'var(--text-muted)' }}>
                No queries defined
              </div>
            )}
          </div>
        )}
      </div>
    </aside>
  )
}
