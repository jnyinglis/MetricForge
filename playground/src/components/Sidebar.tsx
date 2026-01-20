import { useState } from 'react'
import { useWorkspaceStore } from '../hooks/useWorkspaceStore'
import type { EditorTab } from '../types/workspace'

export function Sidebar() {
  const tables = useWorkspaceStore((state) => state.tables)
  const schema = useWorkspaceStore((state) => state.schema)
  const metrics = useWorkspaceStore((state) => state.metrics)
  const queries = useWorkspaceStore((state) => state.queries)
  const jsonFiles = useWorkspaceStore((state) => state.jsonFiles)
  const activeTab = useWorkspaceStore((state) => state.activeTab)
  const sidebarExpanded = useWorkspaceStore((state) => state.sidebarExpanded)
  const setActiveTab = useWorkspaceStore((state) => state.setActiveTab)
  const toggleSidebarSection = useWorkspaceStore((state) => state.toggleSidebarSection)
  const addMetric = useWorkspaceStore((state) => state.addMetric)
  const addQuery = useWorkspaceStore((state) => state.addQuery)
  const addJsonFile = useWorkspaceStore((state) => state.addJsonFile)
  const removeMetric = useWorkspaceStore((state) => state.removeMetric)
  const removeQuery = useWorkspaceStore((state) => state.removeQuery)
  const removeJsonFile = useWorkspaceStore((state) => state.removeJsonFile)
  const removeTable = useWorkspaceStore((state) => state.removeTable)
  const addTable = useWorkspaceStore((state) => state.addTable)

  const [showNewMetricInput, setShowNewMetricInput] = useState(false)
  const [showNewQueryInput, setShowNewQueryInput] = useState(false)
  const [showNewJsonInput, setShowNewJsonInput] = useState(false)
  const [showNewTableInput, setShowNewTableInput] = useState(false)
  const [newMetricName, setNewMetricName] = useState('')
  const [newQueryName, setNewQueryName] = useState('')
  const [newJsonName, setNewJsonName] = useState('')
  const [newTableName, setNewTableName] = useState('')
  const [newTableJson, setNewTableJson] = useState('')

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
    if (tab.type === 'json' && activeTab.type === 'json') {
      return tab.jsonFileName === activeTab.jsonFileName
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

  const handleAddJsonFile = () => {
    if (newJsonName.trim()) {
      addJsonFile(newJsonName.trim())
      setActiveTab({ type: 'json', jsonFileName: newJsonName.trim() })
      setNewJsonName('')
      setShowNewJsonInput(false)
    }
  }

  const handleAddTable = () => {
    if (!newTableJson.trim()) {
      alert('Please provide JSON data')
      return
    }

    try {
      const data = JSON.parse(newTableJson)

      if (Array.isArray(data)) {
        // Single table: require table name
        if (!newTableName.trim()) {
          alert('Please provide a table name for the array data')
          return
        }
        addTable(newTableName.trim(), data)
        setActiveTab({ type: 'data', tableName: newTableName.trim() })
      } else if (typeof data === 'object' && data !== null) {
        // Multiple tables: extract from object keys
        const entries = Object.entries(data)
        const arrayEntries = entries.filter(([, v]) => Array.isArray(v))

        if (arrayEntries.length === 0) {
          alert('JSON must be an array of objects or an object with array properties')
          return
        }

        arrayEntries.forEach(([key, value]) => {
          addTable(key, value as Record<string, unknown>[])
        })
        setActiveTab({ type: 'data', tableName: arrayEntries[0][0] })
      } else {
        alert('JSON must be an array of objects or an object with array properties')
        return
      }

      setNewTableName('')
      setNewTableJson('')
      setShowNewTableInput(false)
    } catch (err) {
      alert('Invalid JSON: ' + (err instanceof Error ? err.message : String(err)))
    }
  }

  return (
    <aside style={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
      {/* Data Section */}
      <div className="sidebar-section">
        <div
          className="sidebar-section-header"
          onClick={() => toggleSidebarSection('data')}
        >
          <span>{sidebarExpanded.data ? '▼' : '▶'}</span>
          <span>DATA ({tables.length})</span>
          <button
            className="btn btn-sm"
            style={{ marginLeft: 'auto', padding: '2px 6px', fontSize: 11 }}
            onClick={(e) => {
              e.stopPropagation()
              setShowNewTableInput(true)
            }}
          >
            +
          </button>
        </div>
        {sidebarExpanded.data && (
          <div className="sidebar-section-content">
            {showNewTableInput && (
              <div style={{ padding: '8px', backgroundColor: 'var(--bg-secondary)', borderRadius: 4, marginBottom: 8 }}>
                <div style={{ marginBottom: 8 }}>
                  <input
                    type="text"
                    className="form-input"
                    style={{ width: '100%', padding: '4px 8px', fontSize: 12 }}
                    placeholder="Table name (optional for object with keys)"
                    value={newTableName}
                    onChange={(e) => setNewTableName(e.target.value)}
                  />
                </div>
                <div style={{ marginBottom: 8 }}>
                  <textarea
                    className="form-textarea"
                    style={{ width: '100%', padding: '4px 8px', fontSize: 11, fontFamily: 'monospace' }}
                    placeholder='[{"id": 1, "name": "test"}] or {"table1": [...], "table2": [...]}'
                    value={newTableJson}
                    onChange={(e) => setNewTableJson(e.target.value)}
                    rows={4}
                  />
                </div>
                <div style={{ display: 'flex', gap: 4 }}>
                  <button className="btn btn-sm btn-primary" onClick={handleAddTable}>
                    Add
                  </button>
                  <button
                    className="btn btn-sm"
                    onClick={() => {
                      setShowNewTableInput(false)
                      setNewTableName('')
                      setNewTableJson('')
                    }}
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}
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
            {tables.length === 0 && !showNewTableInput && (
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

      {/* JSON Files Section */}
      <div className="sidebar-section">
        <div
          className="sidebar-section-header"
          onClick={() => toggleSidebarSection('json')}
        >
          <span>{sidebarExpanded.json ? '▼' : '▶'}</span>
          <span>JSON ({jsonFiles.length})</span>
          <button
            className="btn btn-sm"
            style={{ marginLeft: 'auto', padding: '2px 6px', fontSize: 11 }}
            onClick={(e) => {
              e.stopPropagation()
              setShowNewJsonInput(true)
            }}
          >
            +
          </button>
        </div>
        {sidebarExpanded.json && (
          <div className="sidebar-section-content">
            {showNewJsonInput && (
              <div className="sidebar-item" style={{ gap: 4 }}>
                <input
                  type="text"
                  className="form-input"
                  style={{ flex: 1, padding: '2px 6px', fontSize: 12 }}
                  placeholder="JSON file name"
                  value={newJsonName}
                  onChange={(e) => setNewJsonName(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') handleAddJsonFile()
                    if (e.key === 'Escape') {
                      setShowNewJsonInput(false)
                      setNewJsonName('')
                    }
                  }}
                  autoFocus
                />
                <button className="btn btn-sm btn-primary" onClick={handleAddJsonFile}>
                  Add
                </button>
              </div>
            )}
            {jsonFiles.map((jsonFile) => (
              <div
                key={jsonFile.name}
                className={`sidebar-item ${isTabActive({ type: 'json', jsonFileName: jsonFile.name }) ? 'active' : ''}`}
                onClick={() => setActiveTab({ type: 'json', jsonFileName: jsonFile.name })}
              >
                <span className="tab-icon">{jsonFile.valid ? '{ }' : '⚠️'}</span>
                <span style={{ flex: 1 }}>{jsonFile.name}</span>
                <span
                  onClick={(e) => {
                    e.stopPropagation()
                    removeJsonFile(jsonFile.name)
                  }}
                  style={{ opacity: 0.5, cursor: 'pointer' }}
                >
                  ×
                </span>
              </div>
            ))}
            {jsonFiles.length === 0 && !showNewJsonInput && (
              <div className="sidebar-item" style={{ color: 'var(--text-muted)' }}>
                No JSON files
              </div>
            )}
          </div>
        )}
      </div>
    </aside>
  )
}
