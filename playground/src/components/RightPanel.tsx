import { useWorkspaceStore } from '../hooks/useWorkspaceStore'
import type { RightPanelTab } from '../types/workspace'
import { parseDsl } from '../utils/parserAdapter'
import { buildLogicalPlan } from '../utils/logicalPlanBuilder'
import { PlanVisualizer, PlanTextView } from './PlanVisualizer'

export function RightPanel() {
  const activeTab = useWorkspaceStore((state) => state.activeTab)
  const rightPanelTab = useWorkspaceStore((state) => state.rightPanelTab)
  const setRightPanelTab = useWorkspaceStore((state) => state.setRightPanelTab)
  const tables = useWorkspaceStore((state) => state.tables)
  const metrics = useWorkspaceStore((state) => state.metrics)
  const queries = useWorkspaceStore((state) => state.queries)
  const schema = useWorkspaceStore((state) => state.schema)

  const activePanelTab = rightPanelTab === 'results' ? 'preview' : rightPanelTab

  const tabs: Array<{ id: RightPanelTab; label: string }> = [
    { id: 'preview', label: 'Preview' },
    { id: 'ast', label: 'AST' },
    { id: 'plan', label: 'Plan' },
    { id: 'errors', label: 'Errors' },
  ]

  const renderPreview = () => {
    if (!activeTab) return <div style={{ color: 'var(--text-muted)' }}>No selection</div>

    switch (activeTab.type) {
      case 'data': {
        const table = tables.find((t) => t.name === activeTab.tableName)
        if (!table) return <div>Table not found</div>
        return (
          <div>
            <h4 style={{ marginBottom: 8 }}>Table Info</h4>
            <div style={{ fontSize: 12, marginBottom: 8 }}>
              <strong>Rows:</strong> {table.rows.length}
            </div>
            <div style={{ fontSize: 12, marginBottom: 8 }}>
              <strong>Columns:</strong> {table.columns.length}
            </div>
            <h4 style={{ marginTop: 16, marginBottom: 8 }}>Column Types</h4>
            {table.columns.map((col) => (
              <div key={col.name} style={{ fontSize: 12, marginBottom: 4 }}>
                <span style={{ color: 'var(--info)' }}>{col.name}</span>
                <span style={{ color: 'var(--text-muted)' }}> : {col.type}</span>
              </div>
            ))}
          </div>
        )
      }

      case 'schema':
        return (
          <div className="json-preview" style={{ maxHeight: '100%', overflow: 'auto' }}>
            <pre>{JSON.stringify(schema, null, 2)}</pre>
          </div>
        )

      case 'metric': {
        const metric = metrics.find((m) => m.name === activeTab.metricName)
        if (!metric) return <div>Metric not found</div>
        return (
          <div>
            <h4 style={{ marginBottom: 8 }}>Metric: {metric.name}</h4>
            <div style={{ fontSize: 12, marginBottom: 8 }}>
              <strong>Status:</strong>{' '}
              <span className={`badge ${metric.valid ? 'badge-success' : 'badge-error'}`}>
                {metric.valid ? 'Valid' : 'Invalid'}
              </span>
            </div>
            {metric.errors.length > 0 && (
              <div style={{ marginTop: 8 }}>
                <strong>Errors:</strong>
                {metric.errors.map((err, i) => (
                  <div key={i} style={{ color: 'var(--error)', fontSize: 12 }}>
                    {err.message}
                  </div>
                ))}
              </div>
            )}
          </div>
        )
      }

      case 'query': {
        const query = queries.find((q) => q.name === activeTab.queryName)
        if (!query) return <div>Query not found</div>
        return (
          <div>
            <h4 style={{ marginBottom: 8 }}>Query: {query.name}</h4>
            <div style={{ fontSize: 12, marginBottom: 8 }}>
              <strong>Status:</strong>{' '}
              <span className={`badge ${query.valid ? 'badge-success' : 'badge-error'}`}>
                {query.valid ? 'Valid' : 'Invalid'}
              </span>
            </div>
            {query.errors.length > 0 && (
              <div style={{ marginTop: 8 }}>
                <strong>Errors:</strong>
                {query.errors.map((err, i) => (
                  <div key={i} style={{ color: 'var(--error)', fontSize: 12 }}>
                    {err.message}
                  </div>
                ))}
              </div>
            )}
          </div>
        )
      }

      default:
        return <div>No preview available</div>
    }
  }

  const renderAst = () => {
    if (!activeTab) return <div style={{ color: 'var(--text-muted)' }}>No selection</div>

    if (activeTab.type === 'metric') {
      const metric = metrics.find((m) => m.name === activeTab.metricName)
      if (!metric || !metric.dsl) return <div>No AST available</div>

      const { ast, errors } = parseDsl(
        metric.dsl.startsWith('metric')
          ? metric.dsl
          : `metric ${metric.name} on default = ${metric.dsl}`
      )
      if (errors.length > 0 || !ast) {
        return <div style={{ color: 'var(--error)' }}>Parse error</div>
      }

      return (
        <div className="json-preview" style={{ maxHeight: '100%', overflow: 'auto' }}>
          <pre>{JSON.stringify(ast.metrics[0] || {}, null, 2)}</pre>
        </div>
      )
    }

    if (activeTab.type === 'query') {
      const query = queries.find((q) => q.name === activeTab.queryName)
      if (!query || !query.dsl) return <div>No AST available</div>

      const { ast, errors } = parseDsl(query.dsl)
      if (errors.length > 0 || !ast) {
        return <div style={{ color: 'var(--error)' }}>Parse error</div>
      }

      return (
        <div className="json-preview" style={{ maxHeight: '100%', overflow: 'auto' }}>
          <pre>{JSON.stringify(ast.queries[0] || {}, null, 2)}</pre>
        </div>
      )
    }

    return <div style={{ color: 'var(--text-muted)' }}>AST not available for this item</div>
  }

  const renderPlan = () => {
    if (!activeTab) return <div style={{ color: 'var(--text-muted)' }}>No selection</div>

    if (activeTab.type === 'query') {
      const query = queries.find((q) => q.name === activeTab.queryName)
      if (!query || !query.dsl) return <div>No query selected</div>

      const { ast, errors } = parseDsl(query.dsl)
      if (errors.length > 0 || !ast || ast.queries.length === 0) {
        return <div style={{ color: 'var(--error)' }}>Parse error - cannot build plan</div>
      }

      const querySpec = ast.queries[0].spec

      try {
        const plan = buildLogicalPlan(querySpec, schema, metrics)
        return <PlanVisualizer plan={plan} />
      } catch (e) {
        return (
          <div style={{ color: 'var(--error)' }}>
            Error building plan: {e instanceof Error ? e.message : 'Unknown error'}
          </div>
        )
      }
    }

    if (activeTab.type === 'metric') {
      return (
        <div style={{ color: 'var(--text-muted)' }}>
          <p>Logical plan visualization is available for queries.</p>
          <p style={{ marginTop: 8 }}>Select a query to see its execution plan.</p>
        </div>
      )
    }

    return <div style={{ color: 'var(--text-muted)' }}>Plan not available for this item</div>
  }

  const renderErrors = () => {
    const allErrors: Array<{ source: string; message: string; severity: string }> = []

    metrics.forEach((m) => {
      m.errors.forEach((e) => {
        allErrors.push({ source: `Metric: ${m.name}`, message: e.message, severity: e.severity })
      })
    })

    queries.forEach((q) => {
      q.errors.forEach((e) => {
        allErrors.push({ source: `Query: ${q.name}`, message: e.message, severity: e.severity })
      })
    })

    if (allErrors.length === 0) {
      return <div style={{ color: 'var(--success)' }}>No errors</div>
    }

    return (
      <div>
        {allErrors.map((err, i) => (
          <div key={i} className="problem-item">
            <span
              className={`problem-icon ${err.severity === 'error' ? 'error' : 'warning'}`}
            >
              {err.severity === 'error' ? '●' : '▲'}
            </span>
            <div>
              <div className="problem-message">{err.message}</div>
              <div className="problem-location">{err.source}</div>
            </div>
          </div>
        ))}
      </div>
    )
  }

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
      <div className="panel-tabs">
        {tabs.map((tab) => (
          <div
            key={tab.id}
            className={`panel-tab ${activePanelTab === tab.id ? 'active' : ''}`}
            onClick={() => setRightPanelTab(tab.id)}
          >
            {tab.label}
            {tab.id === 'errors' && (
              <span style={{ marginLeft: 4 }}>
                ({metrics.reduce((acc, m) => acc + m.errors.length, 0) +
                  queries.reduce((acc, q) => acc + q.errors.length, 0)})
              </span>
            )}
          </div>
        ))}
      </div>
      <div className="panel-content">
        {activePanelTab === 'preview' && renderPreview()}
        {activePanelTab === 'ast' && renderAst()}
        {activePanelTab === 'plan' && renderPlan()}
        {activePanelTab === 'errors' && renderErrors()}
      </div>
    </div>
  )
}
