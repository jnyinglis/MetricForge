import { useWorkspaceStore } from '../hooks/useWorkspaceStore'
import { DataEditor } from './editors/DataEditor'
import { SchemaEditor } from './editors/SchemaEditor'
import { MetricEditor } from './editors/MetricEditor'
import { QueryEditor } from './editors/QueryEditor'
import { WelcomeScreen } from './WelcomeScreen'

export function Workspace() {
  const activeTab = useWorkspaceStore((state) => state.activeTab)
  const tables = useWorkspaceStore((state) => state.tables)
  const metrics = useWorkspaceStore((state) => state.metrics)
  const queries = useWorkspaceStore((state) => state.queries)

  const renderContent = () => {
    if (!activeTab) {
      return <WelcomeScreen />
    }

    switch (activeTab.type) {
      case 'data': {
        const table = tables.find((t) => t.name === activeTab.tableName)
        if (!table) return <div className="empty-state">Table not found</div>
        return <DataEditor table={table} />
      }

      case 'schema':
        return <SchemaEditor />

      case 'metric': {
        const metric = metrics.find((m) => m.name === activeTab.metricName)
        if (!metric) return <div className="empty-state">Metric not found</div>
        return <MetricEditor metric={metric} />
      }

      case 'query': {
        const query = queries.find((q) => q.name === activeTab.queryName)
        if (!query) return <div className="empty-state">Query not found</div>
        return <QueryEditor query={query} />
      }

      default:
        return <WelcomeScreen />
    }
  }

  const getTabTitle = () => {
    if (!activeTab) return ''

    switch (activeTab.type) {
      case 'data':
        return `Data: ${activeTab.tableName}`
      case 'schema':
        return 'Schema Editor'
      case 'metric':
        return `Metric: ${activeTab.metricName}`
      case 'query':
        return `Query: ${activeTab.queryName}`
      default:
        return ''
    }
  }

  return (
    <div className="workspace">
      {activeTab && (
        <div className="workspace-tabs">
          <div className="workspace-tab active">
            <span>{getTabTitle()}</span>
          </div>
        </div>
      )}
      <div className="workspace-content">{renderContent()}</div>
    </div>
  )
}
